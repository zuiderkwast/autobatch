Autobatch
=========

An Erlang library to automatically group similar jobs, queries, rpc calls etc. together into batches. It can be used to avoid the "*Select N+1 Problem*" and similar things.

Usage outline:

# Start an *autobatch manager* process.
# Spawn one or more worker processes that perform blocking calls using `autobatch:call/2`. The worker
  processes themself may spawn their own child workers if they whish, in any number of levels.
# The autobatch manager keeps track of how many workers are running and which of them are waiting for
  a reply. When all workers are waiting, the autobatch manager sends the queries to a user-defined
  callback function that is supposed to return the replies for some or all of the queries. These are
  returned to the workers as the return value of `autobatch:call/2` and they continue where they were.

Data types
----------

```Erlang
batch_fun() = fun((Queries :: [{pid(), term()}], State :: term()) ->
                  {Responses :: [{pid(), term()}], NextState :: term()}).
```

A callback function that should take a list of pids and queries, along with a state, and
compute the response for some or all of the queries. The batch response should be returned
in the same form, i.e. as a list of pairs with the pid and the response of its query along
with a new state.

```Erlang
worker_fun() = fun((Args :: term(), Batch :: pid()) -> Result :: term()).
```

A worker function that can take any arguments along with the autobatch process id which
it belongs to. It may return any term.

Functions
---------

```Erlang
start_link(BatchFun :: batch_fun(), State :: term()) -> BatchPid :: pid()
```

Starts an autobatch manager process and links to it. Takes a batch handler function and
an initial state that will be passed to the handler function along with the queries.
The BatchFun should return a pair of the resonses and a new batch state.

```Erlang
stop(BatchPid :: pid()) -> {ok, State :: term()}.
```

Stops the batch handler and returns the state returned by the last batch call. Raises an
error if there are any workers still running or waiting.

```Erlang
call(Query :: term(), BatchPid :: pid()) -> Response :: term().
```
Perform a blocking query.

```Erlang
spawn_worker(worker_fun(), Input :: [term()], BatchPid :: pid()) -> WorkerPid :: pid().
```

Makes a non-blocking call to `WorkerFun(BatchPid, Input)` in a new worker process and returns
its pid. To wait for it and collect the result, use `wait_for_worker/2`.

```Erlang
wait_for_worker(WorkerPid :: pid(), BatchPid :: pid()) -> Result :: term().
```

Blocks until the response form a worker process is available and its response.

```Erlang
map(worker_fun(), Inputs :: [term()], BatchPid :: pid()) -> Results :: [term()].
```

Spawns a worker process for each element in `Inputs`, applies the input and `BatchPid` to `WorkerFun` in that processes and waits for them to return. Their return values are returned as a list in the same order as their Inputs.
