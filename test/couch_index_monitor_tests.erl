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

-define(DDOC, {[{
    <<"views">>, {[
        {<<"test_view">>, {[{<<"map">>, <<"function(doc) {emit(null, 1);}">>}]}
    }]}
}]}).


setup() ->
    DbName = ?tempdb(),
    {ok, Db} = couch_db:create(DbName, [?ADMIN_CTX]),
    DDocName = <<"_design/", DbName/binary>>,
    DDoc = #doc{id = DDocName, body = ?DDOC},
    {ok, _} = couch_db:update_doc(Db, DDoc, [?ADMIN_CTX]),
    {Db, DDoc}.


teardown({Db, _DDoc}) ->
    couch_db:close(Db),
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
                    fun test_basic/1
                ]
            }
        }
    }.


test_basic({Db, DDoc}) ->
    ?_test(begin
        {ok, Pid} = couch_index_server:get_index(couch_mrview_index, Db#db.name, DDoc#doc.id),
        {ok, IdxState} = couch_mrview_index:init(Db, DDoc),
        DbName = couch_mrview_index:get(db_name, IdxState),
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
        ok
    end).


wait_down({Pid, Ref}) ->
    Pid ! close,
    receive {'DOWN', Ref, process, Pid, normal} ->
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
