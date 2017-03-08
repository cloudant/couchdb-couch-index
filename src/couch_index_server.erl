% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_index_server).
-behaviour(gen_server).
-behaviour(config_listener).

-vsn(2).

-export([start_link/0, validate/2, get_index/4, get_index/3, get_index/2]).

-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

% Exported for callbacks
-export([
    handle_config_change/5,
    handle_config_terminate/3,
    handle_db_event/3
]).

-include("couch_index.hrl").

-include_lib("couch/include/couch_db.hrl").

-define(RELISTEN_DELAY, 5000).
-define(MAX_INDEXES_OPEN, 500).
-define(CLOSE_INTERVAL, 5000).

-record(st, {
    root_dir,
    soft_max_indexes,
    open
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


validate(DbName, DDoc) ->
    LoadModFun = fun
        ({ModNameList, "true"}) ->
            try
                [list_to_existing_atom(ModNameList)]
            catch error:badarg ->
                []
            end;
        ({_ModNameList, _Enabled}) ->
            []
    end,
    ValidateFun = fun
        (ModName) ->
            ModName:validate(DbName, DDoc)
    end,
    EnabledIndexers = lists:flatmap(LoadModFun, config:get("indexers")),
    lists:foreach(ValidateFun, EnabledIndexers).


get_index(Module, #db{name = <<"shards/", _/binary>> = DbName}, DDoc) ->
    case is_record(DDoc, doc) of
        true -> get_index(Module, DbName, DDoc, nil);
        false -> get_index(Module, DbName, DDoc)
    end;
get_index(Module, <<"shards/", _/binary>> = DbName, DDoc) ->
    {Pid, Ref} = spawn_monitor(fun() ->
        exit(fabric:open_doc(mem3:dbname(DbName), DDoc, [ejson_body, ?ADMIN_CTX]))
    end),
    receive {'DOWN', Ref, process, Pid, {ok, Doc}} ->
        get_index(Module, DbName, Doc, nil);
    {'DOWN', Ref, process, Pid, Error} ->
        Error
    after 61000 ->
        erlang:demonitor(Ref, [flush]),
        {error, timeout}
    end;

get_index(Module, DbName, DDoc) ->
    get_index(Module, DbName, DDoc, nil).


get_index(Module, DbName, DDoc, Fun) when is_binary(DbName) ->
    couch_util:with_db(DbName, fun(Db) ->
        get_index(Module, Db, DDoc, Fun)
    end);
get_index(Module, Db, DDoc, Fun) when is_binary(DDoc) ->
    case couch_db:open_doc(Db, DDoc, [ejson_body, ?ADMIN_CTX]) of
        {ok, Doc} -> get_index(Module, Db, Doc, Fun);
        Error -> Error
    end;
get_index(Module, Db, DDoc, Fun) when is_function(Fun, 1) ->
    {ok, InitState} = Module:init(Db, DDoc),
    {ok, FunResp} = Fun(InitState),
    {ok, Pid} = get_index(Module, InitState),
    {ok, Pid, FunResp};
get_index(Module, Db, DDoc, _Fun) ->
    {ok, InitState} = Module:init(Db, DDoc),
    get_index(Module, InitState).


get_index(Module, IdxState) ->
    DbName = Module:get(db_name, IdxState),
    Sig = Module:get(signature, IdxState),
    Args = {Module, IdxState, DbName, Sig},
    case incref({DbName, Sig}) of
        ok ->
            [{_, {Pid, Monitor}}] = ets:lookup(?BY_SIG, {DbName, Sig}),
            ok = couch_index_monitor:notify(Monitor),
            {ok, Pid};
        _ ->
            gen_server:call(?MODULE, {get_index, Args}, infinity)
    end.


init([]) ->
    process_flag(trap_exit, true),
    ok = config:listen_for_changes(?MODULE, couch_index_util:root_dir()),
    ets:new(?BY_DB, [public, bag, named_table]),
    ets:new(?BY_SIG, [public, set, named_table]),
    ets:new(?BY_PID, [public, set, named_table]),
    ets:new(?BY_COUNTERS, [public, set, named_table]),
    ets:new(?BY_IDLE, [public, set, named_table]),
    couch_event:link_listener(?MODULE, handle_db_event, nil, [all_dbs]),
    RootDir = couch_index_util:root_dir(),
    couch_file:init_delete_dir(RootDir),
    erlang:send_after(?CLOSE_INTERVAL, self(), close_idle),
    MaxIndexes = list_to_integer(
        config:get("couchdb", "soft_max_indexes_open", integer_to_list(?MAX_INDEXES_OPEN))),
    {ok, #st{root_dir=RootDir, soft_max_indexes=MaxIndexes, open=0}}.


terminate(_Reason, _State) ->
    Pids = [Pid || {Pid, _} <- ets:tab2list(?BY_PID)],
    lists:map(fun couch_util:shutdown_sync/1, Pids),
    ok.


-spec maybe_close_idle(#st{}) -> {ok, #st{}}.
maybe_close_idle(#st{open=Open, soft_max_indexes=Max}=State) when Open < Max ->
    {ok, State};

maybe_close_idle(State) ->
    ets:safe_fixtable(?BY_IDLE, true),
    try
        {ok, close_idle(State, ets:first(?BY_IDLE))}
    after
        ets:safe_fixtable(?BY_IDLE, false)
    end.


-spec close_idle(#st{}, term()) -> #st{}.
close_idle(State, '$end_of_table') ->
    State;

close_idle(State, {DbName, Sig}) ->
    true = ets:delete(?BY_IDLE, {DbName, Sig}),
    case ets:lookup(?BY_SIG, {DbName, Sig}) of
        [{_, {Pid, _Monitor}}] ->
            couch_index:stop(Pid),
            [{DbName, {DDocId, Sig}}] =
                ets:match_object(?BY_DB, {DbName, {'$1', Sig}}),
            rem_from_ets(DbName, Sig, DDocId, Pid),
            case closed(State) of
                NewState when NewState#st.open > State#st.soft_max_indexes ->
                    close_idle(NewState, ets:next(?BY_IDLE, {DbName, Sig}));
                NewState ->
                    NewState
            end;
        [] ->
            close_idle(State, ets:next(?BY_IDLE, {DbName, Sig}))
    end.


handle_call({get_index, {_Mod, _IdxState, DbName, Sig}=Args}, From, State) ->
    case ets:lookup(?BY_SIG, {DbName, Sig}) of
        [] ->
            {ok, NewState} = maybe_close_idle(State),
            spawn_link(fun() -> new_index(Args) end),
            {ok, Monitor} = couch_index_monitor:start_link({DbName, Sig}),
            ets:insert(?BY_SIG, {{DbName, Sig}, {[From], Monitor}}),
            {noreply, NewState};
        [{_, {Waiters, Monitor}}] when is_list(Waiters) ->
            ets:insert(?BY_SIG, {{DbName, Sig}, {[From | Waiters], Monitor}}),
            {noreply, State};
        [{_, {Pid, Monitor}}] when is_pid(Pid) ->
            ok = incref({DbName, Sig}),
            ok = couch_index_monitor:notify(Monitor, From),
            {reply, {ok, Pid}, State}
    end;
handle_call({async_open, {DbName, DDocId, Sig}, {ok, Pid}}, _From, State) ->
    [{_, {Waiters, Monitor}}] = ets:lookup(?BY_SIG, {DbName, Sig}),
    link(Pid),
    ets:insert(?BY_SIG, {{DbName, Sig}, {Pid, Monitor}}),
    ets:insert(?BY_PID, {Pid, {DbName, Sig}}),
    ets:insert(?BY_COUNTERS, {{DbName, Sig}, 0}),
    ets:insert(?BY_DB, {DbName, {DDocId, Sig}}),
    lists:foreach(fun(From) ->
        {Client, _} = From,
        ok = incref({DbName, Sig}),
        ok = couch_index_monitor:notify(Monitor, Client),
        gen_server:reply(From, {ok, Pid})
    end, Waiters),
    {reply, ok, opened(State)};
handle_call({async_error, {DbName, _DDocId, Sig}, Error}, {FromPid, _}, State) ->
    [{_, {Waiters, Monitor, _SO}}] = ets:lookup(?BY_SIG, {DbName, Sig}),
    [gen_server:reply(From, Error) || From <- Waiters],
    true = ets:delete(?BY_COUNTERS, {DbName, Sig}),
    true = ets:delete(?BY_SIG, {DbName, Sig}),
    true = ets:delete(?BY_PID, FromPid),
    true = ets:delete(?BY_IDLE, {DbName, Sig}),
    ok = couch_index_monitor:close(Monitor),
    {reply, ok, State};
handle_call({set_soft_max_indexes_open, Max}, _From, State) ->
    {reply, ok, State#st{soft_max_indexes=Max}};
handle_call({reset_indexes, DbName}, _From, State) ->
    {reply, ok, reset_indexes(DbName, State)};
handle_call(open_index_count, _From, State) ->
    {reply, State#st.open, State};
handle_call(get_server, _From, State) ->
    {reply, State, State}.


handle_cast({reset_indexes, DbName}, State) ->
    {noreply, reset_indexes(DbName, State)}.

handle_info({'EXIT', Pid, Reason}, Server) ->
    case ets:lookup(?BY_PID, Pid) of
        [{_, {DbName, Sig}}] ->
            [{DbName, {DDocId, Sig}}] =
                ets:match_object(?BY_DB, {DbName, {'$1', Sig}}),
            rem_from_ets(DbName, Sig, DDocId, Pid),
            {noreply, closed(Server)};
        [] when Reason /= normal ->
            exit(Reason);
        _Else ->
            {noreply, Server}
    end;
handle_info({timeout, _Timer, close_idle}, State) ->
    {ok, NewState} = maybe_close_idle(State),
    erlang:send_after(?CLOSE_INTERVAL, self(), close_idle),
    {noreply, NewState};
handle_info(restart_config_listener, State) ->
    ok = config:listen_for_changes(?MODULE, couch_index_util:root_dir()),
    {noreply, State};
handle_info(Msg, State) ->
    couch_log:warning("~p did not expect ~p", [?MODULE, Msg]),
    {noreply, State}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


handle_config_change("couchdb", "index_dir", RootDir, _, RootDir) ->
    {ok, RootDir};
handle_config_change("couchdb", "view_index_dir", RootDir, _, RootDir) ->
    {ok, RootDir};
handle_config_change("couchdb", "index_dir", _, _, _) ->
    exit(whereis(couch_index_server), config_change),
    remove_handler;
handle_config_change("couchdb", "view_index_dir", _, _, _) ->
    exit(whereis(couch_index_server), config_change),
    remove_handler;
handle_config_change("couchdb", "soft_max_indexes_open", Max, _, _) when is_list(Max) ->
    {ok, gen_server:call(?MODULE, {set_soft_max_indexes_open, list_to_integer(Max)})};
handle_config_change(_, _, _, _, _) ->
    {ok, nil}.

handle_config_terminate(_, stop, _) ->
    ok;
handle_config_terminate(_Server, _Reason, _State) ->
    erlang:send_after(?RELISTEN_DELAY, whereis(?MODULE), restart_config_listener),
    {ok, couch_index_util:root_dir()}.


new_index({Mod, IdxState, DbName, Sig}) ->
    DDocId = Mod:get(idx_name, IdxState),
    case couch_index:start_link({Mod, IdxState}) of
        {ok, Pid} ->
            ok = gen_server:call(
                ?MODULE, {async_open, {DbName, DDocId, Sig}, {ok, Pid}}),
            unlink(Pid);
        Error ->
            ok = gen_server:call(
                ?MODULE, {async_error, {DbName, DDocId, Sig}, Error})
    end.


reset_indexes(DbName, State) ->
    #st{root_dir=Root} = State,
    % shutdown all the updaters and clear the files, the db got changed
    Fun = fun({_, {DDocId, Sig}}, StateAcc) ->
        [{_, {Pid, Monitor}}] = ets:lookup(?BY_SIG, {DbName, Sig}),
        couch_index_monitor:close(Monitor),
        MRef = erlang:monitor(process, Pid),
        gen_server:cast(Pid, delete),
        receive {'DOWN', MRef, _, _, _} -> ok end,
        rem_from_ets(DbName, Sig, DDocId, Pid),
        closed(StateAcc)
    end,
    NewState = lists:foldl(Fun, State, ets:lookup(?BY_DB, DbName)),
    Path = couch_index_util:index_dir("", DbName),
    couch_file:nuke_dir(Root, Path),
    NewState.


rem_from_ets(DbName, Sig, DDocId, Pid) ->
    true = ets:delete(?BY_COUNTERS, {DbName, Sig}),
    true = ets:delete(?BY_SIG, {DbName, Sig}),
    true = ets:delete(?BY_PID, Pid),
    true = ets:delete(?BY_IDLE, {DbName, Sig}),
    ets:delete_object(?BY_DB, {DbName, {DDocId, Sig}}).


handle_db_event(DbName, created, St) ->
    gen_server:cast(?MODULE, {reset_indexes, DbName}),
    {ok, St};
handle_db_event(DbName, deleted, St) ->
    gen_server:cast(?MODULE, {reset_indexes, DbName}),
    {ok, St};
handle_db_event(DbName, {ddoc_updated, DDocId}, St) ->
    lists:foreach(fun({_DbName, {_DDocId, Sig}}) ->
        case ets:lookup(?BY_SIG, {DbName, Sig}) of
            [{_, {IndexPid, _Monitor}}] ->
                (catch gen_server:cast(IndexPid, ddoc_updated));
            [] ->
                ok
        end
    end, ets:match_object(?BY_DB, {DbName, {DDocId, '$1'}})),
    {ok, St};
handle_db_event(_DbName, _Event, St) ->
    {ok, St}.


-spec opened(#st{}) -> #st{}.
opened(State) ->
    State#st{open=State#st.open + 1}.


-spec closed(#st{}) -> #st{}.
closed(State) ->
    State#st{open=State#st.open - 1}.


incref(Name) ->
    case (catch ets:update_counter(?BY_COUNTERS, Name, 1)) of
        1 ->
            ets:delete(?BY_IDLE, Name),
            ok;
        N when is_integer(N), N > 0 ->
            ok;
        N when is_integer(N) ->
            {invalid_refcount, N};
        {'EXIT', {badarg, _}} ->
            missing_counter
	end.
