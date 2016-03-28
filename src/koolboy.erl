%%%-------------------------------------------------------------------
%%% @author Liming Zhang <<zhanglm at me.com>>
%%% @copyright (C) 2016, zskevin.com
%%% @doc
%%%
%%% @end
%%% Created : 26. Mar 2016 23:57
%%%-------------------------------------------------------------------
-module(koolboy).
-author("Liming Zhang <<zhanglm at me.com>>").

-behavior(gen_server).

%% API

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([checkout/1,
         checkout/2,
         checkin/2,
         transaction/2,
         child_spec/2,
         start/1,
         start_link/1]).


-define(TIMEOUT, 5000). %%缺省超时时间5000毫秒

-ifdef(pre17).
-type pid_queue() :: queue().
-else.
-type pid_queue() :: queue:queue().
-endif.

-type pool() ::
Name :: (atom() | pid()) |
{Name :: atom(), node()} |
{local, Name :: atom()} |
{global, GlobalName :: any()} |
{via, Module :: atom(), ViaName :: any()}.

-type start_ret() :: {'ok', pid()} | 'ignore' | {'error', term()}.

-record(state, {
          supervisor :: pid(),
          workers :: [pid()],
          waiting :: pid_queue(),
          monitors :: ets:tid(),
          size = 5 :: non_neg_integer(),
          overflow = 0 :: non_neg_integer(),
          max_overflow = 10 :: non_neg_integer(),
          strategy = lifo :: lifo | fifo
         }).

%%%%==================================================
%%% Callback functions
%%%==================================================

init({PoolArgs, WorkerArgs}) ->
  process_flag(trap_exit, true),
  Waiting = queue:new(),
  Monitors = ets:new(monitors, [private]),
  init(PoolArgs, WorkerArgs, #state{waiting = Waiting, monitors = Monitors}).

init([{worker_module, Mod} | Rest], WorkerArgs, State) when is_atom(Mod) ->
  {ok, Sup} = koolboy_sup:start_link(Mod, WorkerArgs),
  init(Rest, WorkerArgs, State#state{supervisor = Sup});
init([{size, Size} | Rest], WorkerArgs, State) when is_integer(Size) ->
  init(Rest, WorkerArgs, State#state{size = Size});
init([{max_overflow, MaxOverFlow} | Rest], WorkerArgs, State) when is_integer(MaxOverFlow) ->
  init(Rest, WorkerArgs, State#state{max_overflow = MaxOverFlow});
init([{strategy, fifo} | Rest], WorkerArgs, State) ->
  init(Rest, WorkerArgs, State#state{strategy = fifo});
init([_ | Rest], WorkerArgs, State) ->
  init(Rest, WorkerArgs, State);
init([], _WorkerArgs, #state{size=Size, supervisor = Sup} = State) ->
  Workers = prepopulate(Size, Sup),
  {ok, State#state{workers = Workers}}.


handle_call(Request, From, State) ->
  erlang:error(not_implemented).

handle_cast({checkin, Pid}, State=#state{monitors = Monitors}) ->
  case ets:lookup(Monitors, Pid) of
    [{Pid, _, MRef}] ->
      true = erlang:demonitor(MRef),
      true = ets:delete(Monitors, Pid),
      NewState = handle_checkin(Pid, State),
      {noreply, NewState};
    [] ->
      {noreply, State}
  end;



handle_cast(Request, State) ->
  erlang:error(not_implemented).

handle_info(Info, State) ->
  erlang:error(not_implemented).

terminate(Reason, State) ->
  erlang:error(not_implemented).

code_change(OldVsn, State, Extra) ->
  erlang:error(not_implemented).

%%%==================================================
%%% API functions
%%%==================================================

%% ==================================================
%% @doc checkout
%% @end
%% ==================================================
-spec checkout(Pool :: pool()) -> pid() | full.
checkout(Pool) ->
  checkout(Pool, true).

-spec checkout(Pool :: pool(),
               Block :: boolean()) -> pid() | full.
checkout(Pool, Block) ->
  checkout(Pool, Block, ?TIMEOUT).

-spec checkout(Pool :: pool(),
               Block :: boolean(),
               Timeout :: timeout()) -> pid() | full.
checkout(Pool, Block, Timeout) ->
  CRef = make_ref(),
  try
    gen_server:call(Pool, {checkout, CRef, Block}, Timeout)
  catch
    Class:Reason ->
      gen_server:cast(Pool, {cancel_waiting, CRef}),
      erlang:raise(Class, Reason, eralng:get_stracktrace())
  end.

%% ==================================================
%% @doc checkin
%% @end
%% ==================================================
-spec checkin(Pool :: pool(),
              Worker :: pid()) -> ok.
checkin(Pool, Worker) when is_pid(Worker) ->
  gen_server:cast(Pool, {checkin, Worker}).

%% ==================================================
%% @doc transaction
%% @end
%% ==================================================
-spec transaction(Pool :: pool(),
                  Fun :: fun((Worker :: pid()) -> any())) -> any().
transaction(Pool, Fun) ->
  transaction(Pool, Fun, ?TIMEOUT).

-spec transaction(Pool :: pool(),
                  Fun :: fun((Worker :: pid()) -> any()),
                           Timeout :: timeout()) -> any().
transaction(Pool, Fun, Timeout) ->
  Worker = koolboy:checkout(Pool, true, Timeout),
  try
    Fun(Worker)
  after
    ok = koolboy:checkin(Pool, Worker)
  end.

%% ==================================================
%% @doc child_spec
%% @end
%% ==================================================
-spec child_spec(PoolId :: term(), 
                 PoolArgs :: proplists:proplist()) -> supervirsor:child_spec().
child_spec(PoolId, PoolArgs) ->
  child_spec(PoolId, PoolArgs, []).

-spec child_spec(PoolId :: term(),
                 PoolArgs :: proplists:proplist(),
                 WorkerArgs :: proplists:proplist()) -> supervisor:child_spec().
child_spec(PoolId, PoolArgs, WorkerArgs) ->
  {PoolId, {koolboy, start_link, [PoolArgs, WorkerArgs]},
   permanent, 5000, worker, [koolboy]}.

-spec start(PoolArgs :: proplists:proplist()) -> start_ret().
start(PoolArgs)->
  start(PoolArgs, PoolArgs).

-spec start(PoolArgs :: proplists:proplist(),
            WorkerArgs :: proplists:proplist()) -> start_ret().
start(PoolArgs, WorkerArgs) ->
  start_pool(start, PoolArgs, WorkerArgs).

-spec start_link(PoolArgs :: proplists:proplist()) -> start_ret().
start_link(PoolArgs) ->
  start_link(PoolArgs, PoolArgs).

-spec start_link(PoolArgs :: proplists:proplist(),
                 WorkerArgs :: proplists:proplist()) -> start_ret().
start_link(PoolArgs, WorkerArgs) ->
  start_pool(start_link, PoolArgs, WorkerArgs).

-spec status(Pool :: pool()) -> {atom(), integer(), integer(), integer()}.
status(Pool) ->
  gen_server:call(Pool, status).







%%%==================================================
%%% Private functions
%%%==================================================
start_pool(StartFun, PoolArgs, WorkerArgs) ->
  case proplists:get_value(name, PoolArgs) of
    undefined ->
      gen_server:StartFun(?MODULE, {PoolArgs, WorkerArgs}, []);
    Name ->
      gen_sever:StartFun(Name, ?MODULE, {PoolArgs, WorkerArgs}, [])
  end.

prepopulate(N, _Sup) when N < 1 ->
  [];
prepopulate(N, Sup) ->
  prepopulate(N, Sup, []).


prepopulate(0, _Sup, Workers) ->
  Workers;
prepopulate(N, Sup, Workers) ->
  prepopulate(N-1, Sup, [new_worker(Sup) | Workers]).


new_worker(Sup) ->
  {ok, Pid} = supervisor:start_child(Sup, []),
  true = link(Pid),
  Pid.
new_worker(Sup, FromPid) ->
  Pid = new_worker(Sup),
  Ref = erlang:monitor(process, FromPid),
  {Pid, Ref}.

dismiss_worker(Sup, Pid) ->
  true = unlink(Pid),
  supervisor:terminate_child(Sup, Pid).

handle_checkin(Pid, State) ->
  #state{supervisor = Sup,
    waiting = Waiting,
    monitors = Monitors,
    overflow = Overflow,
    strategy = Strategy} = State,
  case queue:out(Waiting) of
    {{value, {From, CRef, MRef}}, Left} ->
      true = ets:insert(Monitors, {Pid, CRef, MRef}),
      gen_server:reply(From, Pid),

  end
