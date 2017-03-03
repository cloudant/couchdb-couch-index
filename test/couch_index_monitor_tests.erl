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

-module(couch_index_monitor_tests).

-include("couch_index.hrl").
-include_lib("couch/include/couch_eunit.hrl").
-include_lib("couch/include/couch_db.hrl").

ddoc_gen(I) ->
    IBin = integer_to_binary(I),
    #doc{
        id = <<"_design/mydb_", IBin/binary>>,
        body = {[
            {<<"views">>, {[
                {<<"test_view_", IBin/binary>>, {[
                    {<<"map">>, <<"function(doc) {emit(null, 1);}">>}]}
                }
            ]}}
        ]}
    }.


setup() ->
    ok.


teardown(_) ->
    ok.


monitoring_test_() ->
    {
        "Test index monitoring",
        {
            setup,
            fun() -> test_util:start_couch([]) end, fun test_util:stop_couch/1,
            {
                foreach,
                fun setup/0, fun teardown/1,
                [
                    fun test_basic/1,
                    fun test_soft_max/1
                ]
            }
        }
    }.

test_basic(_) ->
    ?_test(begin
        DbName = ?tempdb(),
        {ok, Db} = couch_db:create(DbName, [?ADMIN_CTX]),
        DDoc = ddoc_gen(1),
        {ok, _} = couch_db:update_doc(Db, DDoc, [?ADMIN_CTX]),
        {ok, Pid} = couch_index_server:get_index(couch_mrview_index, DbName, DDoc#doc.id),
        {ok, IdxState} = couch_mrview_index:init(Db, DDoc),
        Sig = couch_mrview_index:get(signature, IdxState),
        [{_, {Pid, Monitor}}] = ets:lookup(?BY_SIG, {DbName, Sig}),
        ?assert(is_pid(Pid)),
        ?assert(is_pid(Monitor)),
        ?assertEqual(1, get_count({DbName, Sig})),
        Acq1 = spawn_monitor_acquirer(DbName, DDoc#doc.id),
        ?assertEqual(2, get_count({DbName, Sig})),
        Acq2 = spawn_monitor_acquirer(DbName, DDoc#doc.id),
        ?assertEqual(3, get_count({DbName, Sig})),
        wait_down(Acq2),
        ?assertEqual(2, get_count({DbName, Sig})),
        wait_down(Acq1),
        ?assertEqual(1, get_count({DbName, Sig})),
        couch_index_monitor:cancel({DbName, Sig}, {self(), Monitor}),
        ?assertEqual(0, get_count({DbName, Sig})),
        ?assertEqual(1, length(ets:lookup(?BY_IDLE, {DbName, Sig}))),
        couch_db:close(Db),
        couch_server:delete(DbName, [?ADMIN_CTX]),
        ok
    end).


test_soft_max(_) ->
    ?_test(begin
        DbName = ?tempdb(),
        {ok, Db} = couch_db:create(DbName, [?ADMIN_CTX]),
        lists:foreach(fun(I) ->
            {ok, _} = couch_db:update_doc(Db, ddoc_gen(I), [?ADMIN_CTX])
        end, lists:seq(1, 10)),
        config:set("couchdb", "soft_max_indexes_open", "5"),
        ?assertEqual(0, length(ets:tab2list(?BY_SIG))),
        ?assertEqual(0, gen_server:call(couch_index_server, open_index_count)),
        Acqs = lists:map(fun(I) ->
            DDoc = ddoc_gen(I),
            Acq = spawn_monitor_acquirer(DbName, DDoc#doc.id),
            ?assertEqual(I, length(ets:tab2list(?BY_SIG))),
            ?assertEqual(I, gen_server:call(couch_index_server, open_index_count)),
            Acq
        end, lists:seq(1, 10)),
        {First5, Last5} = lists:split(5, Acqs),
        lists:foldl(fun(Acq, Acc) ->
            wait_down(Acq),
            couch_index_server ! close_idle,
            sys:get_status(couch_index_server), % wait until close_idle processed
            ?assertEqual(10-Acc, gen_server:call(couch_index_server, open_index_count)),
            Acc+1
        end, 1, Last5),
        lists:foreach(fun(Acq) ->
            wait_down(Acq),
            ?assertEqual(5, gen_server:call(couch_index_server, open_index_count))
        end, First5),
        ok
    end).


wait_down({Pid, Ref}) ->
    Pid ! close,
    receive {'DOWN', Ref, process, Pid, normal} ->
        % this is a hack to wait for couch_index_server to process DOWN message
        sys:get_status(couch_index_server),
        ok
    end.


spawn_monitor_acquirer(DbName, DDocID) ->
    {Pid, Ref} = spawn_monitor(fun() ->
        receive {acquire, From} ->
            {ok, _} = couch_index_server:get_index(couch_mrview_index, DbName, DDocID),
            From ! acquired
        end,
        receive close -> ok end
    end),
    Pid ! {acquire, self()},
    receive acquired -> ok end,
    {Pid, Ref}.


get_count(NameSig) ->
    case ets:lookup(?BY_COUNTERS, NameSig) of
        [] -> 0;
        [{_, Count}] -> Count
    end.
