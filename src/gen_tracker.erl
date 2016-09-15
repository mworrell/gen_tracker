%% @doc gen_tracker module
%% @author Max Lapshin
%% @copyright (c) 2012-2014 Max Lapshin
%%
%% MIT license.
%%

-module(gen_tracker).
-author('Max Lapshin <max@maxidoors.ru>').

-behaviour(gen_server).
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("eunit/include/eunit.hrl").


-export([start_link/1, start_link/2, find/2, find_or_open/2, info/2, list/1, 
         setattr/3, setattr/4, getattr/3, getattr/4, increment/4, delattr/3]).
-export([wait/1]).
-export([list/2, info/3]).

-export([which_children/1]).
-export([add_existing_child/2]).
-export([child_monitoring/5]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-export([init_setter/1]).

-type options() :: list(option()).
-type option() :: {callback_module, module()}.

-type zone() :: atom().
-type name() :: term().
-type key() :: term().
-type attr() :: { key(), term() }.
-type restarttype() :: transient.
-type childtype() :: supervisor | worker. 
-type shutdown() :: brutal_kill | infinity | timeout().
-type childspec() ::
          {name(), mfa()}
        | {name(), mfa(), restarttype()}
        | {name(), mfa(), restarttype(), shutdown(), childtype(), list(module())}.

-export_type([
    zone/0,
    name/0,
    key/0,
    attr/0,
    restarttype/0,
    childtype/0,
    shutdown/0,
    childspec/0
  ]).

-record(tracker, {
  zone :: atom(),
  callback_module :: module(),
  launchers = [] :: list()
}).

-record(launch, {
  name :: term(),
  mfa :: mfa(),
  pid :: pid(),
  ref :: reference(),
  waiters = [] :: list()
}).

-record(entry, {
  name :: term(),
  ref :: reference(),
  mfa :: mfa(),
  restart_type :: restarttype(),
  child_type :: worker,
  mods :: list(),
  shutdown :: pos_integer(),
  sub_pid :: pid(),   % watchdog process
  pid :: pid()        % child process
}).


-spec info(zone(), name()) -> list(attr()).
info(Zone, Name) ->
  ets:select(attr_table(Zone), ets:fun2ms(fun({{N, K}, V}) when N =:= Name -> {K,V} end)).

% Name == <<"aa">>
% ets:fun2ms(fun({{N, K}, V}) when N == Name andalso (K == <<"key">> orelse K == key2) -> {K,V} end).
% [{  
%   {{'$1','$2'},'$3'},
%
%   [{'andalso',{'==','$1',{const,<<"aa">>}},
%               {'orelse',{'==','$2',<<"key">>},{'==','$2',key2}}}],
%
%   [{{'$2','$3'}}]
% }]

-spec info(zone(), name(), list(key())) -> list(attr()).
info(_Zone, _Name, []) ->
  [];
info(Zone, Name, Keys) ->
  Head = {{'$1','$2'},'$3'},
  Output = [{{'$2','$3'}}],

  KeyCond = case Keys of
    {ms, Keys0} -> Keys0;
    _ -> build_key_condition(Keys)
  end,
  Cond = [{'andalso',{'==', '$1', Name}, KeyCond }],
  MS = [{Head, Cond, Output}],
  ets:select(attr_table(Zone), MS).


build_key_condition([Key]) -> {'==', '$2', Key};
build_key_condition([Key|Keys]) -> {'orelse', {'==', '$2', Key}, build_key_condition(Keys)}.



-spec setattr(zone(), name(), list(attr())) -> true.
setattr(Zone, Name, Attributes) ->
  % ets:insert(attr_table(Zone), [{{Name, K}, V} || {K,V} <- Attributes]).
  gen_server:call(attr_process(Zone), {set, Name, Attributes}).

-spec setattr(zone(), name(), key(), term()) -> true.
setattr(Zone, Name, Key, Value) ->
  % ets:insert(attr_table(Zone), {{Name, Key}, Value}).
  gen_server:call(attr_process(Zone), {set, Name, Key, Value}).

-spec delattr(zone(), name(), term()) -> ok.
delattr(Zone,Name,Key) ->
  % ets:delete(attr_table(Zone),{Name,Key}).
  gen_server:call(attr_process(Zone), {delete, Name, Key}).


-spec getattr(zone(), name(), key()) -> {ok, term()} | undefined.
getattr(Zone, Name, Key) ->
  case ets:lookup(attr_table(Zone), {Name, Key}) of
    [{_, V}] -> {ok, V};
    [] -> undefined
  end.

-spec getattr(zone(), name(), key(), timeout()) -> {ok, term()} | undefined.
getattr(_Zone, _Name, _Key, Timeout) when Timeout < -1000 ->
  undefined;
getattr(Zone, Name, Key, Timeout) ->
  case getattr(Zone, Name, Key) of
    undefined ->
      timer:sleep(1000),
      getattr(Zone, Name, Key, Timeout - 1000);
    Else ->
      Else
  end.  

-spec increment(zone(), name(), key(), integer()) -> integer().
increment(Zone, Name, Key, Incr) ->
  ets:update_counter(attr_table(Zone), {Name, Key}, Incr).

-spec list(zone()) -> list({name(), list(attr())}).
list(Zone) ->
  [{Name,[{pid,Pid}|info(Zone, Name)]} || #entry{name = Name, pid = Pid} <- ets:tab2list(Zone)].

-spec list(zone(), list(key())) -> list({name(), list(attr())}).
list(Zone, []) ->
  [{Name,[{pid,Pid}]} || #entry{name = Name, pid = Pid} <- ets:tab2list(Zone)];
list(Zone, Keys) ->
  KeysMS = {ms, build_key_condition(Keys)},
  [{Name,[{pid,Pid}|info(Zone, Name, KeysMS)]} || #entry{name = Name, pid = Pid} <- ets:tab2list(Zone)].



-spec find(zone(), name()) -> {ok, pid()} | undefined.
find(Zone, Name) ->
  case ets:lookup(Zone, Name) of
    [] -> undefined;
    [#entry{pid = Pid}] -> {ok, Pid}
  end.


-spec find_or_open(zone(), childspec()) -> {ok, pid()} | {error, term()}.
find_or_open(Zone, {Name, {_,_,_} = MFA}) ->
  find_or_open(Zone, {Name, MFA, transient, 200, worker, []});
find_or_open(Zone, {Name, {_,_,_} = MFA, RestartType}) ->
  find_or_open(Zone, {Name, MFA, RestartType, 200, worker, []});
find_or_open(Zone, {Name, _MFA, _RestartType, _Shutdown, _ChildType, _Mods} = ChildSpec) ->
  try ets:lookup(Zone, Name) of
    [] ->
      case supervisor:check_childspecs([ChildSpec]) of
        ok -> gen_server:call(Zone, {find_or_open, ChildSpec}, 10000);
        Error -> Error
      end;
    [#entry{pid = Pid}] -> {ok, Pid}
  catch
    error:badarg -> {error, gen_tracker_not_started}
  end.

% Sync call to ensure that all messages have been processed
-spec wait(zone()) -> ok.
wait(Zone) ->
  gen_server:call(Zone, wait).

-spec which_children(zone()) -> list({name(), pid(), childtype(), list(module())}).
which_children(Zone) ->
  ets:select(Zone, ets:fun2ms(fun(#entry{name = Name, pid = Pid, child_type = CT, mods = Mods}) ->
    {Name, Pid, CT, Mods}
  end)).

-spec add_existing_child(zone(), {name(), pid()} | {name(), pid(), worker, list(module())}) ->
    {ok, pid()} | {error, {already_started, pid()}}.
add_existing_child(Zone, {Name, Pid}) ->
  add_existing_child(Zone, {Name, Pid, worker, []});
add_existing_child(Zone, {_Name, Pid, worker, _} = ChildSpec) when is_pid(Pid) ->
  gen_server:call(Zone, {add_existing_child, ChildSpec}).

-spec start_link(zone()) -> {ok, pid()} | {error, term()}.
start_link(Zone) ->
  start_link(Zone, []).

-spec start_link(zone(), options()) -> {ok, pid()} | {error, term()}.
start_link(Zone, Options) ->
  gen_server:start_link({local, Zone}, ?MODULE, [Zone|Options], [{spawn_opt,[{fullsweep_after,1}]}]).


%%====================================================================
%% gen_server init callback
%%====================================================================

init([Zone|Options]) ->
  process_flag(trap_exit, true),
  ets:new(Zone, [public,named_table,{keypos,#entry.name}, {read_concurrency, true}]),
  ets:new(attr_table(Zone), [public,named_table, {read_concurrency, true}]),
  proc_lib:start_link(?MODULE, init_setter, [Zone]),
  {ok, #tracker{ zone = Zone, callback_module = proplists:get_value(callback_module, Options) }}.


%%====================================================================
%% child startup and watchdog process handling
%%====================================================================

init_setter(Zone) ->
  erlang:register(attr_process(Zone), self()),
  proc_lib:init_ack({ok, self()}),
  put(name, {setter, Zone}),
  loop_setter(attr_table(Zone)).

loop_setter(Table) ->
  Msg = receive M -> M end,
  case Msg of
    {'$gen_call', From, {set, Name, Key, Value}} ->
      gen:reply(From, ok),
      ets:insert(Table, {{Name, Key}, Value}),
      loop_setter(Table);
    {'$gen_call', From, {set, Name, Attributes}} ->
      gen:reply(From, ok),
      ets:insert(Table, [{{Name, K}, V} || {K,V} <- Attributes]),
      loop_setter(Table);
    {'$gen_call', From, {delete, Name, Key}} ->
      gen:reply(From, ok),
      ets:delete(Table, {Name, Key}),
      loop_setter(Table);
    Else ->
      error_logger:info_msg("Unknown msg to ~p: ~p", [Table, Else]),
      loop_setter(Table)
  end.

launch_child(Zone, {Name, {M,F,A} = MFA, RestartType, Shutdown, ChildType, Mods}, CallbackModule) ->
  Parent = self(),
  proc_lib:spawn_link(fun() ->
    put(name, {gen_tracker,Zone,proxy,Name}),
    process_flag(trap_exit,true),
    try erlang:apply(M,F,A) of
      {ok, Pid} ->
        ets:insert(Zone, #entry{name = Name, mfa = {M,F,A}, pid = Pid, restart_type = RestartType, 
          shutdown = Shutdown, child_type = ChildType, sub_pid = self(), mods = Mods}),
        Parent ! {launch_ready, self(), Name, {ok, Pid}},
        erlang:monitor(process,Pid),
        opt_callback(CallbackModule, after_add_child, [Zone, Name, MFA, {ok, Pid}]),
        proc_lib:hibernate(?MODULE, child_monitoring, [Zone, Name, Pid, Parent, CallbackModule]);
      {error, Error} ->
        Parent ! {launch_ready, self(), Name, {error, Error}},
        opt_callback(CallbackModule, after_add_child, [Zone, Name, MFA, {error, Error}]);
      Error ->
        error_logger:error_msg("Spawn function in gen_tracker ~p~n for name ~240p~n returned error: ~p~n", [Zone, Name, Error]),
        Parent ! {launch_ready, self(), Name, Error},
        opt_callback(CallbackModule, after_add_child, [Zone, Name, MFA, Error])
    catch
      _Class:Error ->
        error_logger:error_msg("Spawn function in gen_tracker ~p~n for name ~240p~n failed with error: ~p~nStacktrace: ~n~p~n", 
          [Zone, Name,Error, erlang:get_stacktrace()]),
        Parent ! {launch_ready, self(), Name, {error, Error}},
        opt_callback(CallbackModule, after_add_child, [Zone, Name, MFA, {error, Error}])
    end 
  end).


child_monitoring(Zone, Name, Pid, Parent, CallbackModule) ->
  receive
    {'EXIT', Parent, Reason} ->
      % Parent process died and didn't bother to clean up the children.
      % This means that the parent was killed unexpectedly, so now we
      % have to kill our kid as well.
      % As the tracker is down, it is safe to assume that the ets tables
      % are gone as well and we won't be able to do any cleanup.
      MRef = monitor(process, Pid),
      exit(Pid, Reason),
      receive
        {'DOWN', MRef, process, _, _} -> ok
      after
        5000 -> exit(Pid, kill)
      end,
      exit(Reason);
    {'DOWN', _, process, Pid, Reason} ->
      delete_entry(Zone, Name, Reason, CallbackModule),
      exit(normal);
    _ -> ok
  after
    0 -> ok
  end,
  proc_lib:hibernate(?MODULE, child_monitoring, [Zone, Name, Pid, Parent, CallbackModule]).



% Use a monitor, as the process might be down already or shutting down just before we kill it.
% (Spurious 'EXIT' signals are dropped in handle_info/2 below.)
shutdown_child(#entry{sub_pid = SubPid, pid = Pid, shutdown = Shutdown, name = Name}, Zone, CallbackModule) when is_number(Shutdown) ->
  MRef = erlang:monitor(process, Pid),
  exit(Pid, shutdown),
  receive
    {'DOWN', MRef, process, Pid, _Reason} -> ok
  after
    Shutdown ->
      erlang:exit(SubPid, kill),
      erlang:exit(Pid, kill),
      receive
        {'DOWN', MRef, process, Pid, _Reason} -> ok
      end,
      delete_entry(Zone, Name, kill, CallbackModule)
  end,
  ok;
shutdown_child(#entry{sub_pid = SubPid, pid = Pid, shutdown = brutal_kill, name = Name}, Zone, CallbackModule) ->
  MRef = erlang:monitor(process, Pid),
  receive
    {'DOWN', MRef, process, Pid, _Reason} -> ok
  after 0 ->
    erlang:exit(SubPid, kill),
    erlang:exit(Pid, kill),
    receive
      {'DOWN', MRef, process, Pid, _Reason} -> ok
    end,
    delete_entry(Zone, Name, kill, CallbackModule)
  end,
  ok;
shutdown_child(#entry{sub_pid = _SubPid, pid = Pid, shutdown = infinity}, _Zone, _CallbackModule) ->
  MRef = erlang:monitor(process, Pid),
  erlang:exit(Pid, shutdown),
  receive
    {'DOWN', MRef, process, Pid, _Reason} -> ok
  end.


%%====================================================================
%% gen_server callbacks
%%====================================================================

handle_call(wait, _From, Tracker) ->
  {reply, ok, Tracker};

handle_call(shutdown, _From, #tracker{zone = Zone, callback_module=CallbackModule} = Tracker) ->
  [shutdown_child(E, Zone, CallbackModule) || E <- ets:tab2list(Zone)],
  {stop, shutdown, ok, Tracker};

handle_call(which_children, _From, #tracker{zone = Zone} = Tracker) ->
  {reply, which_children(Zone), Tracker};

handle_call({find_or_open, {Name, {M,F,A}, _RT, _S, _CT, _M} = Spec}, From, 
            #tracker{zone = Zone, launchers = Launchers, callback_module=CallbackModule} = Tracker) ->
  case ets:lookup(Zone, Name) of
    [] ->
      case lists:keytake(Name, #launch.name, Launchers) of
        {value, #launch{waiters = Waiters} = L, Launchers1} ->
          L1 = L#launch{waiters = [From|Waiters]},
          {noreply, Tracker#tracker{launchers = [L1|Launchers1]}};
        false ->
          Pid = launch_child(Zone, Spec, CallbackModule),
          Ref = erlang:monitor(process, Pid),
          L = #launch{name = Name, pid = Pid, ref = Ref, mfa = {M,F,A}, waiters = [From]},
          {noreply, Tracker#tracker{launchers = [L|Launchers]}}
      end;  
    [#entry{pid = Pid}] ->
      {reply, {ok, Pid}, Tracker}
  end;

handle_call({terminate_child, Name}, From, #tracker{} = Tracker) ->
  handle_call({delete_child, Name}, From, Tracker);

handle_call({delete_child, Name}, _From, #tracker{zone = Zone, callback_module = CallbackModule} = Tracker) ->
  case ets:lookup(Zone, Name) of
    [Entry] ->
      shutdown_child(Entry, Zone, CallbackModule),
      {reply, ok, Tracker};
    [] ->
      {reply, {error, no_child}, Tracker}
  end;

handle_call({add_existing_child, {Name, Pid, worker, Mods}}, _From, 
            #tracker{zone = Zone, callback_module = CallbackModule} = Tracker) ->
  case ets:lookup(Zone, Name) of
    [#entry{pid = Pid2}] ->
      {reply, {error, {already_started, Pid2}}, Tracker};
    [] ->
      Ref = erlang:monitor(process,Pid),
      ets:insert(Zone, #entry{name = Name, mfa = undefined, sub_pid = Pid, pid = Pid, restart_type = temporary,
        shutdown = 200, child_type = worker, mods = Mods, ref = Ref}),

      Parent = self(),
      proc_lib:spawn_link(fun() ->
        put(name, {gen_tracker,Zone,proxy,Name}),
        process_flag(trap_exit,true),
        ets:update_element(Zone, Name, {#entry.sub_pid, self()}),
        erlang:monitor(process,Pid),
        opt_callback(CallbackModule, after_add_child, [Zone, Name, undefined, {ok, Pid}]),
        proc_lib:hibernate(?MODULE, child_monitoring, [Zone, Name, Pid, Parent, CallbackModule])
      end),
      {reply, {ok, Pid}, Tracker}
  end;

handle_call(stop, _From, #tracker{} = Tracker) ->
  {stop, normal, ok, Tracker};

handle_call(Call, _From, State) ->
  {stop, {unknown_call, Call}, State}.

handle_cast(Cast, State) ->
  {stop, {unknown_cast, Cast}, State}.



handle_info({launch_ready, Launcher, Name, Reply}, #tracker{launchers = Launchers} = Tracker) ->
  {value, #launch{pid = Launcher, ref = Ref, name = Name, waiters = Waiters}, Launchers1} =
    lists:keytake(Name, #launch.name, Launchers),
  erlang:demonitor(Ref, [flush]),
  [gen_server:reply(From, Reply) || From <- Waiters],
  {noreply, Tracker#tracker{launchers = Launchers1}};

handle_info({'DOWN', MRef, process, _Pid, Reason},
            #tracker{zone = Zone, launchers = Launchers, callback_module = CallbackModule} = Tracker) ->
  case lists:keytake(MRef, #launch.ref, Launchers) of
    {value, #launch{waiters = Waiters, name=Name, mfa = MFA}, Launchers1} ->
      [ gen_server:reply(From, {error, {startup_error, Reason}}) || From <- Waiters ],
      opt_callback(CallbackModule, after_add_child, [Zone, Name, MFA, {error, {'DOWN', Reason}}]),
      {noreply, Tracker#tracker{launchers = Launchers1}};
    false ->
      {noreply, Tracker}
  end;

handle_info({'EXIT', _Pid, _Reason}, #tracker{} = Tracker) ->
  {noreply, Tracker};

handle_info(_Msg, State) ->
  {noreply, State}.

terminate(_,#tracker{zone = Zone}) ->
  [erlang:exit(Pid, shutdown) || #entry{pid = Pid} <- ets:tab2list(Zone)],

  [begin
    erlang:monitor(process, Pid),
    erlang:monitor(process, SubPid),
    if Shutdown == brutal_kill ->
      erlang:exit(SubPid, kill),
      erlang:exit(Pid, kill),
      receive
        {'DOWN', _, _, Pid, _} -> ok
      end,
      receive
        {'DOWN', _, _, SubPid, _} -> ok
      end,
      ok;
    true ->
      Delay = if Shutdown == infinity -> 20000; is_number(Shutdown) -> Shutdown end,
      receive
        {'DOWN', _, _, Pid, _} -> ok
      after
        Delay -> 
          erlang:exit(SubPid,kill),
          erlang:exit(Pid,kill),
          receive
            {'DOWN', _, _, Pid, _} -> ok
          end,
          receive
            {'DOWN', _, _, SubPid, _} -> ok
          end
      end
    end
  end || #entry{sub_pid = SubPid, pid = Pid, shutdown = Shutdown} <- ets:tab2list(Zone)],
  ok.

code_change(_, State, _) ->
  {ok, State}.


%%====================================================================
%% Support functions
%%====================================================================

attr_table(live_streams) -> live_streams_attrs;
attr_table(vod_files) -> vod_files_attrs;
attr_table(Zone) ->
  list_to_atom(atom_to_list(Zone)++"_attrs").

attr_process(live_streams) -> live_streams_setter;
attr_process(vod_files) -> vod_files_setter;
attr_process(Zone) ->
  list_to_atom(atom_to_list(Zone)++"_setter").


opt_callback(undefined, _F, _A) ->
  ok;
opt_callback(M, F, A) ->
  try
    erlang:apply(M, F, A)
  catch
    Class:Error ->
      error_logger:info_msg("Error calling ~p:~p ~p: ~p:~p\n~p\n", 
                            [M, F, A, Class, Error, erlang:get_stacktrace()])
  end.


delete_entry(Zone, #entry{name = Name, mfa = MFA}, Reason, CallbackModule) ->
  ets:delete(Zone, Name),
  {Mod,MFun2,MFun4} = case MFA of
    {M,_,_} ->
      {M,
       erlang:function_exported(M, after_terminate, 2), 
       erlang:function_exported(M, after_terminate, 4)};
    undefined ->
      {undefined, false, false}
  end,
  case MFun2 or MFun4 or (CallbackModule =/= undefined) of
    true ->
      Attrs = info(Zone, Name),
      delete_attrs(Zone, Name),
      put(name, {gen_tracker_after_terminate,Zone,Name}),
      try
          case {MFun2,MFun4} of
            {_, true} -> Mod:after_terminate(Name, Attrs, MFA, Reason);
            {true, _} -> Mod:after_terminate(Name, Attrs);
            {false, false} -> ok
          end
      catch
        MClass:MError ->
          error_logger:info_msg("Error calling ~p:after_terminate(~p,attrs,~p,~p): ~p:~p\n~p\n", 
                                [Mod, Name, MFA, Reason, MClass, MError, erlang:get_stacktrace()])
      end,
      opt_callback(CallbackModule, after_terminate, [Zone, Name, Attrs, MFA, Reason]);
    false ->
      delete_attrs(Zone, Name)
  end,
  ok;
delete_entry(Zone, Name, Reason, CallbackModule) ->
  case ets:lookup(Zone, Name) of
    [#entry{} = E] -> delete_entry(Zone, E, Reason, CallbackModule);
    [] -> ok
  end.


delete_attrs(Zone, Name) ->
  ets:select_delete(attr_table(Zone), ets:fun2ms(fun({{N, _}, _}) when N =:= Name -> true end)).

