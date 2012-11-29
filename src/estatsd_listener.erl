%% ================================================================
%% @author          Zachary Hueras
%% @version         0.0.1
%% @doc             Handles UDP listener/workers
%% ================================================================
-module(estatsd_listener).
-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

-record(state, {
        socket          = undefined :: inets:socket(),
        worker_count    = 4
    }).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

appvar(Key, Default) ->
    case application:get_env(estatsd, Key) of
        undefined -> Default;
        {ok, Val} -> Val
    end.

init([]) ->
    Port = appvar(port, 0),
    Workers = appvar(workers, 4),
    {ok, LSock} = gen_udp:listen(Port, [{active, false}]),
    self() ! start_workers,
    {ok, #state{socket = LSock, worker_count = Workers}}.

handle_call(_Call, _From, State) ->
    {reply, ok, State}.

handle_cast(start_workers, State = #state{socket = LSock, worker_count = Workers}) ->
    start_workers(Workers, LSock),
    {noreply, State};
handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

start_workers(0, _LSock) ->
    ok;
start_workers(N, LSock) when N > 0 ->
    supervisor:start_child(estatsd_worker_sup, [LSock]).
