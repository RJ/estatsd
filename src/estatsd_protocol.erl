%% ================================================================
%% @author          Zachary Hueras
%% @version         0.0.1
%% @doc             statsd protocol handler
%% ================================================================
-module(estatsd_protocol).
-export([start_link/1]).
-export([init/1, handle_data/2, terminate/2]).

start_link(LSock) ->
    estatsd_worker:start_link(?MODULE, [], LSock).

init([]) ->
    {ok, no_state}.

handle_data(_Data, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.
