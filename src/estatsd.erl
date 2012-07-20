-module(estatsd).

-export([
         gauge/2,
         increment/1, increment/2, increment/3,
         decrement/1, decrement/2, decrement/3,
         timing/2,
         timing_fun/2, timing_fun/3,
         timing_call/3, timing_call/4, timing_call/5
        ]).

-define(SERVER, estatsd_server).

% Convenience: just give it the now() tuple when the work started
timing(Key, StartTime = {_,_,_}) ->
    Dur = erlang:round(timer:now_diff(os:timestamp(), StartTime)/1000),
    timing(Key,Dur);
timing(Key, Duration) when is_integer(Duration) -> 
    Tid = get_table(timer),
    ets:insert(Tid, {Key, Duration});
timing(Key, Duration) -> 
    timing(Key, round(Duration)).

timing_fun(Fun, Key) ->
    timing_fun(Fun, Key, 1).

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

timing_call(M, F, A) ->
    timing_call(M, F, A, 1).

timing_call(M, F, A, Sample) ->
    Key = string:join(["calls", atom_to_list(M), atom_to_list(F)], "."),
    timing_call(Key, M, F, A, Sample).

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

get_table(Table) ->
    case ets:lookup(statsd, Table) of
        [{_, Tid}] -> Tid;
        [] -> throw({no_exists, Table})
    end.

% Increments one or more stats counters
increment(Key) -> increment(Key, 1, 1).
increment(Key, Amount) -> increment(Key, Amount, 1).
increment(Key, Amount, Sample) when Sample < 1 ->
    case crypto:rand_uniform(1, 1000) / 1000 of
        N when N < Sample -> 
            increment(Key, Amount * (1 / Sample), 1);
        _ -> 
            ok
    end;
increment(Key, Amount, Sample) ->
    {Duration, Tid} = timer:tc(fun get_table/1, [stats]),
    do_increment(Tid, Key, Amount).

do_increment(Tid, Key, Amount) ->
    case catch ets:update_counter(Tid, Key, {2, Amount}) of
        N when is_integer(N) ->
            N;
        _ ->
            increment_new(Tid, Key, Amount)
    end.

increment_new(Tid, Key, Amount) ->
    case ets:insert_new(Tid, {Key, Amount}) of
        false -> 
            do_increment(Tid, Key, Amount);
        _ -> 
            Amount
    end.

decrement(Key) -> decrement(Key, -1, 1).
decrement(Key, Amount) -> decrement(Key, Amount, 1).
decrement(Key, Amount, Sample) ->
    increment(Key, 0 - Amount, Sample).

% Sets a gauge value
gauge(Key, Value) when is_number(Value) ->
    Tid = get_table(gauge),
    ets:insert(Tid, {Key, Value}).
