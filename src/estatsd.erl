%% @doc Estatsd API.
%%
%% Counters describe rate of change per time unit.
%% These examples hold irrespective of the `flush_interval' setting:
%% ```
%% rate_1_per_sec() ->
%%     estatsd:increment(foo),
%%     timer:sleep(1000),
%%     rate_1_per_sec().
%%
%% rate_60_per_min() ->
%%     estatsd:increment(foo,1,1/60),
%%     timer:sleep(1000),
%%     rate_60_per_min().
%%
%% rate_100_per_10s() ->
%%     estatsd:increment(foo,1,1/10),
%%     timer:sleep(100),
%%     rate_100_per_10s().
%% '''
%%
%% Timing example:
%% ```
%% timing() ->
%%     estatsd:timing(bar, fun() -> lists:sort([random:uniform(10000) || _ <- lists:seq(1,10000)]) end),
%%     timing().
%% '''
%%
%% Gauge example:
%%
%% <b>NOTE:</b> For each call to `estatsd:gauge/2' a line of text (Carbon plaintext protocol) will be sent
%% when the `flush_interval' expires.  This can potentially generate a lot of TCP traffic!
%% ```
%% gauge(C) ->
%%     estatsd:gauge(baz, C),
%%     timer:sleep(1000),
%%     gauge(C + 1).
%% '''
-module(estatsd).

-export([
         start/0,
         gauge/2,
         increment/1, increment/2, increment/3,
         decrement/1, decrement/2, decrement/3,
         timing/2
        ]).

-define(SERVER, estatsd_server).

start() ->
    application:start(estatsd).

timing(Key, Fun) when is_function(Fun,0) ->
    Start = erlang:now(),
    Return = Fun(),
    timing(Key, Start),
    Return;

% Convenience: just give it the now() tuple when the work started
timing(Key, StartTime = {_,_,_}) ->
    Dur = erlang:round(timer:now_diff(erlang:now(), StartTime)/1000),
    timing(Key,Dur);

% Log timing information, ms
timing(Key, Duration) when is_integer(Duration) -> 
    gen_server:cast(?SERVER, {timing, Key, Duration});

timing(Key, Duration) -> 
    gen_server:cast(?SERVER, {timing, Key, erlang:round(Duration)}).




% Increments one or more stats counters
increment(Key) -> increment(Key, 1, 1).
increment(Key, Amount) -> increment(Key, Amount, 1).
increment(Key, Amount, Sample) ->
    gen_server:cast(?SERVER, {increment, Key, Amount, Sample}).

decrement(Key) -> decrement(Key, -1, 1).
decrement(Key, Amount) -> decrement(Key, Amount, 1).
decrement(Key, Amount, Sample) ->
    increment(Key, 0 - Amount, Sample).

% Sets a gauge value
gauge(Key, Value) when is_number(Value) ->
    gen_server:cast(?SERVER, {gauge, Key, Value}).
