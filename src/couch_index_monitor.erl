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

-module(couch_index_monitor).

-behaviour(gen_server).


-export([
    start_link/1,
    close/1,
    set_pid/2,

    notify/1,
    notify/2,
    cancel/2
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3,
    terminate/2
]).


-include("couch_index.hrl").


-record(st, {
    name,
    type,
    ref,
    client_refs,
    closing
}).


start_link(Name) ->
    gen_server:start_link(?MODULE, [Name], []).


close(Monitor) ->
    gen_server:cast(Monitor, exit).


set_pid(Monitor, Pid) ->
    gen_server:cast(Monitor, {set_pid, Pid}).


notify(Monitor) ->
    notify(Monitor, self()).


notify(Monitor, Client) when is_pid(Client) ->
    gen_server:cast(Monitor, {notify, Client});

notify(Monitor, {Client, _}) when is_pid(Client) ->
    notify(Monitor, Client).


cancel(Name, {Client, Monitor})
        when Client == self(), is_pid(Monitor) ->
    gen_server:cast(Monitor, {cancel, self()}),
    case (catch ets:update_counter(?BY_COUNTERS, Name, -1)) of
        0 ->
            true = ets:insert(?BY_IDLE, {Name}),
            ok;
        _ ->
            ok
    end.


init([Name]) ->
    {ok, CRefs} = khash:new(),
    {ok, #st{
        name = Name,
        ref = undefined,
        client_refs = CRefs,
        closing = false
    }}.


handle_call(Msg, From, St) ->
    {stop, {unknown_call, Msg, From}, St}.


handle_cast(exit, St) ->
    {stop, shutdown, St};

handle_cast({set_pid, Pid}, #st{ref = undefined} = St) ->
    Ref = erlang:monitor(process, Pid),
    {noreply, St#st{ref = Ref}};

handle_cast({set_pid, Pid}, #st{ref = Ref} = St) when is_reference(Ref) ->
    erlang:demonitor(Ref, [flush]),
    handle_cast({set_pid, Pid}, St#st{ref = undefined});

handle_cast({notify, Client}, St) when is_pid(Client) ->
    case khash:get(St#st.client_refs, Client) of
        {Ref, Count} when is_reference(Ref), is_integer(Count), Count > 0 ->
            khash:put(St#st.client_refs, Client, {Ref, Count + 1});
        undefined ->
            Ref = erlang:monitor(process, Client),
            case khash:size(St#st.client_refs) of
                0 ->
                    % Our first monitor after being idle
                    khash:put(St#st.client_refs, Client, {Ref, 1}),
                    true = ets:delete(?BY_IDLE, {St#st.name});
                N when is_integer(N), N > 0 ->
                    % Still not idle
                    khash:put(St#st.client_refs, Client, {Ref, 1}),
                    ok
            end
    end,
    {noreply, St};

handle_cast({cancel, Client}, St) when is_pid(Client) ->
    case khash:get(St#st.client_refs, Client) of
        {Ref, 1} when is_reference(Ref) ->
            erlang:demonitor(Ref, [flush]),
            khash:del(St#st.client_refs, Client),
            maybe_set_idle(St);
        {Ref, Count} when is_reference(Ref), is_integer(Count), Count > 1 ->
            khash:put(St#st.client_refs, Client, {Ref, Count - 1})
    end,
    {noreply, St};

handle_cast(Msg, St) ->
    {stop, {unknown_cast, Msg}, St}.


handle_info({'DOWN', Ref, process, _, _}, #st{ref = Ref} = St) ->
    {stop, shutdown, St};

handle_info({'DOWN', _Ref, process, Pid, _Reason}, St) ->
    #st{name=Name} = St,
    case khash:get(St#st.client_refs, Pid) of
        {Ref, N} when is_reference(Ref), is_integer(N), N > 0 ->
            ets:update_counter(?BY_COUNTERS, Name, -N),
            khash:del(St#st.client_refs, Pid),
            maybe_set_idle(St);
        undefined ->
            % Ignore unknown processes
            ok
    end,
    {noreply, St};

handle_info(Msg, St) ->
    {stop, {bad_info, Msg}, St}.


code_change(_OldVsn, St, _Extra) ->
    {ok, St}.


terminate(_Reason, _St) ->
    ok.


maybe_set_idle(St) ->
    case khash:size(St#st.client_refs) of
        0 ->
            % We're now idle
            ets:insert(?BY_IDLE, {St#st.name});
        N when is_integer(N), N > 0 ->
            % We have other clients
            ok
    end.
