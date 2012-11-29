%% ================================================================
%% @author          Zachary Hueras
%% @version         0.0.1
%% @doc             Handles UDP listener/workers
%% ================================================================
-module(estatsd_listener).
-behaviour(gen_server).
-compile([{parse_transform, ct_expand}]).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

-define(COMPILE_ONCE(RX), ct_expand:term((fun() -> {ok, Compiled} = re:compile(RX), Compiled end)())).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

appvar(Key, Default) ->
    case application:get_env(estatsd, Key) of
        undefined -> Default;
        {ok, Val} -> Val
    end.

init([]) ->
    io:format("Initializing!!!~n"),
    Port = appvar(port, 8125),
    {ok, LSock} = gen_udp:open(Port, [binary, {active, true}]),
    {ok, LSock}.

handle_call(Call, _From, Socket) ->
    io:format("Unrecognized Call: ~p~n", [Call]),
    {reply, ok, Socket}.

handle_cast(Cast, Socket) ->
    io:format("Unrecognized Cast: ~p~n", [Cast]),
    {noreply, Socket}.

handle_info({udp, Socket, _IP, _Port, <<"$estatsd.agg$\n", Rest/binary>>}, Socket) ->
    %% TODO: Make this more efficient by compiling timer/gauge stats
    parse_packet(Rest),
    {noreply, Socket};
handle_info({udp, Socket, _IP, _Port, Packet}, Socket) ->
    parse_packet(Packet),
    {noreply, Socket};
handle_info(Info, Socket) ->
    io:format("Unrecognized Info: ~p~n", [Info]),
    {noreply, Socket}.

code_change(_OldVsn, Socket, _Extra) ->
    {ok, Socket}.

terminate(_Reason, _Socket) ->
    ok.

parse_packet(Packet) ->
    Metrics = re:split(Packet, ?COMPILE_ONCE("[\\r\\n]+"), [{return, list}]),
    lists:foreach(fun parse_metric/1, Metrics).

parse_metric(Metric) ->
    case re:split(Metric, ?COMPILE_ONCE(":"), [{return, list}]) of
        [Key] -> 
            io:format("Basic counter.~n"),
            estatsd:increment(Key);
        [Key|Values] ->
            io:format("Value descriptor.~n"),
            lists:foreach(fun(Value) -> parse_value(Key, Value) end, Values)
    end.

parse_value(Key, Value) ->
    case re:split(Value, ?COMPILE_ONCE("\\|"), [{return, list}]) of
        [N, "ms"] ->
            io:format("Handling timer.~n"),
            estatsd:timing(Key, list_to_integer(N));
        [N, "g"] ->
            io:format("Handling gauge.~n"),
            estatsd:gauge(Key, list_to_integer(N));
        [N|_] ->
            io:format("Handling counter.~n"),
            estatsd:increment(Key, list_to_integer(N))
    end.
