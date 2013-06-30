%% @doc This module allows multiple worker processes to perform blocking calls or queries to a
%%      shared resource. The queries are collected by an autobatch manager and when all workers are
%%      waiting, the calls are delegated to a batch handler which is supposed to compute a
%%      response for each query in the batch. The responses are then delivered to each of the
%%      worker processes.
%%
%%      This can be used to batch multiple small queries to a database or an rpc server into larger
%%      batches.
-module(autobatch).

-export([start_link/2, stop/1, call/2, spawn_worker/3, wait_for_worker/2, map/3]).
-export_type([batch_fun/0]).

-define(DEFAULT_TIMEOUT, infinity).

%% @doc A state of a batch manager.
-record(state, {batchfun, batchstate = nostate, numactive = 1, queries = dict:new()}).

%% @doc A callback function that should take a list of pids and queries along with a state and
%%      compute the response for some or all of the queries. The batch response should be returned
%%      in the same form, i.e. as a list of pairs with the pid and the response of its query, along
%%      with a new state.
-type batch_fun() :: fun((Queries :: [{pid(), term()}], State :: term()) ->
	{Responses :: [{pid(), term()}], NextState :: term()}).

%% @doc A worker function that can take any arguments along with the autobatch process id which
%%      it belongs to. It may return any term.
-type worker_fun() :: fun((Args :: term(), Batch :: pid()) -> Result :: term()).

%% @doc Starts an autobatch manager process and links to it. Takes a batch handler function and
%%      an initial state that will be passed to the handler function along with the queries.
%%      The BatchFun should return a pair of the resonses and a new batch state.
-spec start_link(BatchFun :: batch_fun(), State :: term()) -> BatchPid :: pid().
start_link(BatchFun, State) ->
	{arity, 2} = erlang:fun_info(BatchFun, arity),
	Loop = fun() -> batch_loop(#state{batchfun = BatchFun, batchstate = State}) end,
	spawn_link(Loop).

%% @doc Stops the batch handler. Raises an error if there are unhandled queries or workers waiting.
-spec stop(BatchPid :: pid()) -> {ok, State :: term()}.
stop(BatchPid) ->
	BatchPid ! {stop, self()},
	receive {ok, BatchState} -> {ok, BatchState} end.

%% @doc Perform a blocking query.
-spec call(Query :: term(), BatchPid :: pid()) -> Response :: term().
call(Query, BatchPid) ->
	BatchPid ! {call, self(), Query},
	receive {response, Response} -> Response end.

%% @doc Makes a non-blocking call to WorkerFun(BatchPid, Input) in a new worker process and returns
%%      its pid. To wait for and collect the result, use wait_for_worker/2.
-spec spawn_worker(worker_fun(), Input :: [term()], BatchPid :: pid()) -> WorkerPid :: pid().
spawn_worker(WorkerFun, Input, BatchPid) ->
	BatchPid ! inc_active,
	ParentPid = self(),
	spawn_link(fun () -> ParentPid ! {done, self(), WorkerFun(Input, BatchPid)} end).

%% @doc Blocks until the result form a worker process is available.
-spec wait_for_worker(WorkerPid :: pid(), BatchPid :: pid()) -> Result :: term().
wait_for_worker(WorkerPid, BatchPid) ->
	%% start waiting = decrement the number of running workers
	BatchPid ! dec_active,
	receive
		{done, WorkerPid, Result} ->
			%% the child stops running and we start running again,
			%% i.e. no change in the number of running workers
			Result
	end.

%% @doc Applies each element in Inputs and BatchPid to WorkerFun, in a separate processes, and
%% waits for them to return. Their return values are returned as a list in the same order as their
%% Inputs.
-spec map(worker_fun(), Inputs :: [term()], BatchPid :: pid()) -> Results :: [term()].
map(WorkerFun, Inputs, BatchPid) ->
	WorkerPids = lists:map(
		fun (Input) -> spawn_worker(WorkerFun, Input, BatchPid) end,
		Inputs),
	Results = lists:map(
		fun (WorkerPid) -> wait_for_worker(WorkerPid, BatchPid) end,
		WorkerPids),
	Results.

%% --- Internal stuff ---

%% @doc The batch handler receive loop
batch_loop(#state{queries = Q, numactive = NumActive} = State) ->
	receive
		inc_active ->
			State1 = State#state{numactive = NumActive + 1},
			batch_loop(State1);
		dec_active ->
			State1 = State#state{numactive = NumActive - 1},
			batch_handle_and_loop(State1);
		{call, FromPid, Query} ->
			Q1 = dict:store(FromPid, Query, Q),
			State1 = State#state{queries   = Q1,
			                     numactive = NumActive - 1},
			batch_handle_and_loop(State1);
		{stop, Pid} ->
			case NumActive == 1 andalso dict:size(Q) == 0 of
				true  -> Pid ! {ok, State#state.batchstate};
				false -> error(batch_not_empty)
			end
	after ?DEFAULT_TIMEOUT ->
		error(timeout)
	end.

%% @doc Executes a batch if possible and loops again.
batch_handle_and_loop(#state{queries = Q, numactive = NumActive} = State) ->
	case NumActive == 0 andalso dict:size(Q) /= 0 of
		true  -> batch_handle_and_loop(batch_execute(State));
		false -> batch_loop(State)
	end.

%% @doc Dispatches batch query to and delegate the responses back to at each of
%%      the waiting pids. Returns a new state.
-spec batch_execute(#state{}) -> #state{}.
batch_execute(#state{queries    = Q,
                     batchfun   = BatchFun,
                     batchstate = BatchState,
                     numactive  = NumActive} = State) ->
	Queries = dict:to_list(Q),
	{Responses, BatchState1} = case BatchState of
		nostate -> {nostate, BatchFun(Queries)};
		_       -> BatchFun(Queries, BatchState)
	end,
	%% Check that each response has a query and send responses.
	Q1 = send_responses(Responses, Q),
	%% Check that at least some query got a respone, otherwise we may get
	%% an infinite loop.
	length(Responses) > 0 orelse error(batch_did_nothing),
	%% Increase the number of active workers for those who got responses.
	NumActive1 = NumActive + length(Responses),
	%% Return the new state to the batch loop
	State#state{queries = Q1, numactive = NumActive1, batchstate = BatchState1}.

%% @doc For each response in the list, sends the response to the pid and
%%      removes the query. Returns the remaining queries that didn't get any
%%      responses. Raises an error if a response doesn't have a matching query.
-spec send_responses([{pid(), term()}], dict()) -> dict().
send_responses([], Q) -> Q;
send_responses([{Pid, Response}|Responses], Q) ->
	dict:is_key(Pid, Q) orelse error(invalid_batch_response),
	Pid ! {response, Response},
	send_responses(Responses, dict:erase(Pid, Q)).

%% --- Unit tests ---

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

autobatch_test() ->
	%% The expected results. We build it here without the batch stuff.
	ExpectedResults = lists:map(fun (Car) -> {Car, my_query({get_origin, Car})} end,
	                            my_query(get_cars)),

	%% The batches we expect (in reverse order) and their queries.
	ExpectedBatches = [
		[{get_origin, bmw}, {get_origin, skoda}, {get_origin, fiat}],
		[get_cars]
	],

	%% Do it in a batch.
	Batch = autobatch:start_link(fun my_batch_fun/2, []),
	Cars = autobatch:call(get_cars, Batch),
	ExpectedResults = autobatch:map(
		fun (Car, Batch0) -> {Car, autobatch:call({get_origin, Car}, Batch0)} end,
		Cars,
		Batch
	),
	{ok, ExpectedBatches} = autobatch:stop(Batch).

%% Test helpers
my_batch_fun(Queries, OldQueryList) ->
	%% Dump the queries
	QueriesOnly = lists:map(fun ({_, Query}) -> Query end, Queries),

	%% Simulate batch call
	Responses = lists:map(fun ({Pid, Query}) -> {Pid, my_query(Query)} end, Queries),
	{Responses, [QueriesOnly | OldQueryList]}.

my_query(get_cars)            -> [bmw, skoda, fiat];
my_query({get_origin, bmw})   -> germany;
my_query({get_origin, skoda}) -> czech_rep;
my_query({get_origin, fiat})  -> italy.

-endif.
