-module(estatsd).

-export([
        aggregate/3,
         gauge/2,
         increment/1, increment/2, increment/3,
         decrement/1, decrement/2, decrement/3,
         timing/2,
         timing_fun/2, timing_fun/3,
         timing_call/3, timing_call/4, timing_call/5
        ]).

-define(SERVER, estatsd_server).

%% @doc Calculates the offset from StartTime and adds it to graphite
%% as a timing. Pure duration can also be sent.
-spec timing(Key :: atom() | string(), StartTime :: {integer(), integer(), integer()}) -> boolean()
    ; (Key :: atom() | string(), Duration :: number()) -> boolean().

aggregate(Counters, Gauges, Timers) ->
    % 1. Get Gauge and Timer tables
    GaugeTid = get_table(gauge),
    TimerTid = get_table(timer),

    % 2. Add the list of each to their respective tables
    ets:insert(GaugeTid, Gauges),
    ets:insert(TimerTid, Timers),

    % 3. Increment all the passed counter keys by the supplied amounts
    [ increment(Key, Val, 1) || {Key, Val} <- Counters ].
    
timing(Key, StartTime = {_,_,_}) ->
    Dur = erlang:round(timer:now_diff(os:timestamp(), StartTime)/1000),
    timing(Key,Dur);

timing(Key, Duration) when is_integer(Duration) -> 
    Tid = get_table(timer),
    ets:insert(Tid, {Key, Duration});
timing(Key, Duration) -> 
    timing(Key, round(Duration)).

%% @doc Calculates the duration of applying Fun and feeds the
%% result into graphite as a timing.
-spec timing_fun(Fun :: fun(), Key :: atom() | string()) -> term().
timing_fun(Fun, Key) ->
    timing_fun(Fun, Key, 1).

%% @doc Calculates the duration of applying Fun and feeds the
%% result into graphite as a timing at the specified sample rate.
-spec timing_fun(Fun :: fun(), Key :: atom() | string(), Sample :: float()) -> term().
timing_fun(Fun, Key, Sample) when Sample < 1 ->
    case crypto:rand_uniform(1, 1000) / 1000 of
        N when N < Sample ->
            timing_fun(Fun, Key, 1);
        _ ->
            Fun()
    end;
timing_fun(Fun, Key, _) ->
    {Duration, Res} = timer:tc(Fun),
    timing(Key, Duration / 1000),
    Res.

%% @doc Finds the duration of apply(M,F,A) and feeds the result 
%% into graphite as a timing.
-spec timing_call(M :: atom(), F :: atom(), A :: [term()]) -> term().
timing_call(M, F, A) ->
    timing_call(M, F, A, 1).

%% @doc Finds the duration of apply(M,F,A) and feeds the result 
%% into graphite as a timing at the specified sample rate.
-spec timing_call(M :: atom(), F :: atom(), A :: [term()], Sample :: float()) -> term().
timing_call(M, F, A, Sample) ->
    Key = string:join(["calls", atom_to_list(M), atom_to_list(F)], "."),
    timing_call(Key, M, F, A, Sample).

%% @doc Finds the duration of apply(M,F,A) and feeds the result 
%% into graphite as a timing at the specified sample rate under
%% the provided key.
-spec timing_call(Key :: atom() | string(), M :: atom(), F :: atom(), A :: [term()], Sample :: float()) -> term().
timing_call(Key, M, F, A, Sample) when Sample < 1 ->
    case crypto:rand_uniform(1, 1000) / 1000 of
        N when N < Sample ->
            timing_call(Key, M, F, A, 1);
        _ ->
            erlang:apply(M, F, A)
    end;
timing_call(Key, M, F, A, _) ->
    {Duration, Res} = timer:tc(M, F, A),
    timing(Key, Duration / 1000),
    Res.

%% @doc Find the specified table's Tid in the settings
%% table, or, failing that, crash and burn.
-spec get_table(atom()) -> term().
get_table(Table) ->
    case ets:lookup(statsd, Table) of
        [{_, Tid}] -> Tid;
        [] -> throw({no_exists, Table})
    end.

%% @doc Increments the specified key by 1
-spec increment(Key :: atom() | string()) -> integer() | ok.
increment(Key) -> increment(Key, 1, 1).

%% @doc Increments the specified key by the specified amount.
-spec increment(Key :: atom() | string(), Amount :: integer()) -> integer() | ok.
increment(Key, Amount) -> increment(Key, Amount, 1).

%% @doc Increments the key, but only at the specified sample rate, 
%% by the amount specified multiplied by the inverse of the sample 
%% rate (so graphite reflects a number closer to the truth).
-spec increment(Key :: atom() | string(), Amount :: integer(), Sample :: float()) -> integer() | ok.
increment(Key, Amount, Sample) when Sample < 1 ->
    case crypto:rand_uniform(1, 1000) / 1000 of
        N when N < Sample -> 
            increment(Key, Amount * (1 / Sample), 1);
        _ -> 
            ok
    end;
increment(Key, Amount, _Sample) ->
    Tid = get_table(stats),
    do_increment(Tid, Key, Amount).

%% @doc Attempts to update the counter for the key; if this fails,
%% assume the failure resulted because the key does not exist, then
%% use increment_new/3
-spec do_increment(Tid :: term(), Key :: atom() | string(), Amount :: integer()) -> integer().
do_increment(Tid, Key, Amount) ->
    case catch ets:update_counter(Tid, Key, {2, Amount}) of
        N when is_integer(N) ->
            N;
        _ ->
            increment_new(Tid, Key, Amount)
    end.

%% @doc Attempts to insert a new key into Tid; if this fails, assume
%% the failure resulted because another process beat us to it, then
%% go back to do_increment.
-spec increment_new(Tid :: term(), Key :: atom() | string(), Amoount :: integer()) -> integer().
increment_new(Tid, Key, Amount) ->
    case ets:insert_new(Tid, {Key, Amount}) of
        false -> 
            do_increment(Tid, Key, Amount);
        _ -> 
            Amount
    end.

%% @doc Decrements the specified key by 1.
-spec decrement(Key :: atom() | string()) -> integer() | ok.
decrement(Key) -> decrement(Key, -1, 1).

%% @doc Decrements the specified key by the specified amount.
-spec decrement(Key :: atom() | string(), Amount :: integer()) -> integer() | ok.
decrement(Key, Amount) -> decrement(Key, Amount, 1).

%% @doc Decrements the key, but only at the specified sample rate,
%% by the amount specified multiplied by the inverse of the sample 
%% rate (so graphite reflects a number closer to the truth).
-spec decrement(Key :: atom() | string(), Amount :: integer(), Sample :: float()) -> integer() | ok.
decrement(Key, Amount, Sample) ->
    increment(Key, 0 - Amount, Sample).

%% @doc Adds a reading to the gauge table with the specified key.
-spec gauge(Key :: atom() | string(), Value :: number()) -> true.
gauge(Key, Value) when is_number(Value) ->
    Tid = get_table(gauge),
    ets:insert(Tid, {Key, Value}).
