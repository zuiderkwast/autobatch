%% @doc This module allows multiple worker processes to perform blocking calls or queries to a
%%      shared resource. The queries are collected by an autobatch manager and when all workers are
%%      waiting, the calls are delegated to a batch handler which is supposed to compute a
%%      response for each query in the batch. The responses are then delivered to each of the
%%      worker processes.
%%
%%      This can be used to batch multiple small queries to a database or an rpc server into larger
%%      batches.
-module(autobatch).

-export([start_link/2, start_link/3, stop/1, call/2, call/3, spawn_worker/3, wait_for_worker/2,
         map/3]).
-export_type([batch_fun/0]).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(default_maxsize, infinity).

%% @doc A state of a batch manager.
-record(state, {batchfun, batchstate = nostate, numactive = 1, queries = dict:new(),
                maxsize = ?default_maxsize}).

%% @doc A callback function that should take a list of pids and queries along with a state and
%%      compute the response for some or all of the queries. The batch response should be returned
%%      in the same form, i.e. as a list of pairs with the pid and the response of its query, along
%%      with a new state.
-type batch_fun() :: fun((Queries :: [{pid(), term()}], State :: term()) ->
	{Responses :: [{pid(), term()}], NextState :: term()}).

%% @doc A worker function that can take any arguments along with the autobatch process id which
%%      it belongs to. It may return any term.
-type worker_fun() :: fun((Args :: term(), Batch :: pid()) -> Result :: term()).

%% @doc See start_link/3.
-spec start_link(BatchFun :: batch_fun(), BatchState :: term()) -> BatchPid :: pid().
start_link(BatchFun, BatchState) ->
	start_link(BatchFun, BatchState, []).

%% @doc Starts an autobatch manager process and links to it. Takes a batch handler function and
%%      an initial state that will be passed to the handler function along with the queries.
%%      The BatchFun should return a pair of the resonses and a new batch state.
-spec start_link(BatchFun :: batch_fun(), BatchState :: term(), Options :: list() | integer()) ->
	BatchPid :: pid().
start_link(BatchFun, BatchState, MaxBatchSize) when is_integer(MaxBatchSize) ->
    %% Deprecated variant of start_link/3. See next function clause.
    start_link(BatchFun, BatchState, [{maxsize, MaxBatchSize}]);
start_link(BatchFun, BatchState, Options) when is_list(Options) ->
	{arity, 2} = erlang:fun_info(BatchFun, arity),
	State = #state{
	    batchfun = BatchFun,
	    batchstate = BatchState,
	    maxsize = proplists:get_value(maxsize, Options, ?default_maxsize)
    },
	{ok, Pid} = gen_server:start_link(?MODULE, State, []),
	Pid.

%% @doc Stops the batch handler. Raises an error if there are unhandled queries or workers waiting.
-spec stop(BatchPid :: pid()) -> {ok, BatchState :: term()}.
stop(BatchPid) ->
	case gen_server:call(BatchPid, stop) of
		{ok, BatchState} -> {ok, BatchState};
		{error, batch_not_empty} -> error(batch_not_empty)
	end.

%% @doc Perform a blocking query.
-spec call(Query :: term(), BatchPid :: pid()) -> Response :: term().
call(Query, BatchPid) ->
	gen_server:call(BatchPid, {call, Query}).

%% @doc Perform a blocking query with a custom timeout.
-spec call(Query :: term(), BatchPid :: pid(), Timeout :: integer() | infinity) ->
    Response :: term().
call(Query, BatchPid, Timeout) ->
	gen_server:call(BatchPid, {call, Query}, Timeout).

%% @doc Makes a non-blocking call to WorkerFun(BatchPid, Input) in a new worker process and returns
%%      its pid. To wait for and collect the result, use wait_for_worker/2.
-spec spawn_worker(worker_fun(), Input :: [term()], BatchPid :: pid()) -> WorkerPid :: pid().
spawn_worker(WorkerFun, Input, BatchPid) ->
	gen_server:cast(BatchPid, inc_active),
	ParentPid = self(),
	spawn_link(fun () -> ParentPid ! {done, self(), WorkerFun(Input, BatchPid)} end).

%% @doc Blocks until the result form a worker process is available. The worker must be started by
%% the same process as the one calling this function.
-spec wait_for_worker(WorkerPid :: pid(), BatchPid :: pid()) -> Result :: term().
wait_for_worker(WorkerPid, BatchPid) ->
	%% start waiting = decrement the number of running workers
	gen_server:cast(BatchPid, dec_active),
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

%% --- Gen_server stuff ---

init(InitState) -> {ok, InitState}.

handle_call(stop, _From, State = #state{queries = Q, numactive = NumActive,
                                        batchstate = BatchState}) ->
	case NumActive == 1 andalso dict:size(Q) == 0 of
		true  -> {stop, normal, {ok, BatchState}, State};
		false -> {reply, {error, batch_not_empty}, State}
	end;
handle_call({call, Query}, FromPid, State = #state{queries = Q, numactive = NumActive}) ->
	Q1 = dict:store(FromPid, Query, Q),
	State1 = State#state{queries = Q1,
	                   numactive = NumActive - 1},
	State2 = do_work(State1),
	{noreply, State2}.

handle_cast(inc_active, State = #state{numactive = NumActive}) ->
	State1 = State#state{numactive = NumActive + 1},
	{noreply, State1};
handle_cast(dec_active, State = #state{numactive = NumActive}) ->
	State1 = State#state{numactive = NumActive - 1},
	State2 = do_work(State1),
	{noreply, State2}.

handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% --- Internal stuff ---

%% @doc Executes a batch if possible.
-spec do_work(#state{}) -> #state{}.
do_work(#state{queries = Queries, numactive = NumActive, maxsize = MaxSize} = State) ->
	InQueue = dict:size(Queries),
	if
		NumActive == 0, InQueue /= 0;
		MaxSize /= infinity, InQueue >= MaxSize ->
			batch_execute(State);
		true -> State
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
	gen_server:reply(Pid, Response),
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
		[{get_origin, bmw}, {get_origin, fiat}, {get_origin, skoda}],
		[get_cars]
	],
	%% Do it in a batch.
	BatchMgr = autobatch:start_link(fun my_batch_fun/2, []),
	Cars = autobatch:call(get_cars, BatchMgr),
	ExpectedResults = autobatch:map(
		fun (Car, BatchMgr0) -> {Car, autobatch:call({get_origin, Car}, BatchMgr0)} end,
		Cars,
		BatchMgr
	),
	?assertEqual({ok, ExpectedBatches}, autobatch:stop(BatchMgr)).

maxsize_test() ->
	%% The same as above but with batches of maxsize = 2.

	%% The expected results. We build it here without the batch stuff.
	ExpectedResults = lists:map(fun (Car) -> {Car, my_query({get_origin, Car})} end,
	                            my_query(get_cars)),
	%% Batch queries in reverse order
	ExpectedBatches = [
		[{get_origin, fiat}],
		[{get_origin, bmw}, {get_origin, skoda}],
		[get_cars]
	],
	BatchMgr = autobatch:start_link(fun my_batch_fun/2, [], 2),
	Cars = autobatch:call(get_cars, BatchMgr),
	ExpectedResults = autobatch:map(
		fun (Car, BatchMgr0) -> {Car, autobatch:call({get_origin, Car}, BatchMgr0)} end,
		Cars,
		BatchMgr
	),
	?assertEqual({ok, ExpectedBatches}, autobatch:stop(BatchMgr)).

timeout_test() ->
	BatchMgr = autobatch:start_link(fun slow_batch_fun/2, []),
	?assertExit({timeout, _}, autobatch:call(get_cars, BatchMgr, 10)),
	?assertMatch({ok, _}, autobatch:stop(BatchMgr)).

%% Test helpers
my_batch_fun(Queries, OldQueryList) ->
	%% Dump the queries
	QueriesOnly = lists:sort([Query || {_, Query} <- Queries]),

	%% Simulate batch call
	Responses = [{Pid, my_query(Query)} || {Pid, Query} <- Queries],
	{Responses, [QueriesOnly | OldQueryList]}.

my_query(get_cars)            -> [bmw, skoda, fiat];
my_query({get_origin, bmw})   -> germany;
my_query({get_origin, skoda}) -> czech_rep;
my_query({get_origin, fiat})  -> italy.

slow_batch_fun(Queries, State) ->
	receive after 1000 -> ok end,
	my_batch_fun(Queries, State).

-endif.
