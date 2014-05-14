-module(autobatch_tests).
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
	ExpectedResults = get_origin_of_each_vechicle(Cars, BatchMgr),
	?assertEqual({ok, ExpectedBatches}, autobatch:stop(BatchMgr)).

maxsize_testx() ->
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
	ExpectedResults = get_origin_of_each_vechicle(Cars, BatchMgr),
	?assertEqual({ok, ExpectedBatches}, autobatch:stop(BatchMgr)).

timeout_testx() ->
	BatchMgr = autobatch:start_link(fun slow_batch_fun/2, []),
	?assertExit({timeout, _}, autobatch:call(get_cars, BatchMgr, 10)),
	?assertMatch({ok, _}, autobatch:stop(BatchMgr)).

multilevel_test() ->
	BatchMgr = autobatch:start_link(fun my_batch_fun/2, [], 2),
	VehicleGetters = [get_cars, get_ufos],
	Result = autobatch:map(
		fun (VehicleGetter, BatchMgr0) ->
			Vehicles = autobatch:call(VehicleGetter, BatchMgr0),
			{VehicleGetter, get_origin_of_each_vechicle(Vehicles, BatchMgr0)}
		end,
		VehicleGetters,
		BatchMgr
	),
	ExpectedResult = [
		{get_cars, [{bmw, germany}, {skoda, czech_rep}, {fiat, italy}]},
		{get_bicycles, [{bianchi, italy}, {trek, usa}, {miyata, japan}]},
		{get_ufos, []}
	],
	?assertEqual(ExpectedResult, Result),
	?assertMatch({ok, _Batches}, autobatch:stop(BatchMgr)).

%% Helpers
%% -------

%% @doc Fetches the cars (get_cars) and for each car, fetches its origin using autobatch:map/3.
get_origin_of_each_vechicle(Vehicles, BatchMgr) ->
	autobatch:map(
		fun (Vehicle, BatchMgr0) -> {Vehicle, autobatch:call({get_origin, Vehicle}, BatchMgr0)} end,
		Vehicles,
		BatchMgr
	).

my_batch_fun(Queries, OldQueryList) ->
	%% Dump the queries
	QueriesOnly = lists:sort([Query || {_, Query} <- Queries]),

	%% Simulate batch call
	Responses = [{Pid, my_query(Query)} || {Pid, Query} <- Queries],
	{Responses, [QueriesOnly | OldQueryList]}.

my_query(get_cars)              -> [bmw, skoda, fiat];
my_query({get_origin, bmw})     -> germany;
my_query({get_origin, skoda})   -> czech_rep;
my_query({get_origin, fiat})    -> italy;
my_query(get_bicycles)          -> [bianchi, trek, miyata];
my_query({get_origin, bianchi}) -> italy;
my_query({get_origin, trek})    -> usa;
my_query({get_origin, miyata})  -> japan;
my_query(get_ufos)              -> [].

slow_batch_fun(Queries, State) ->
	receive after 1000 -> ok end,
	my_batch_fun(Queries, State).
