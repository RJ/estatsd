%% ================================================================
%% @author          Zachary Hueras
%% @version         0.0.1
%% @doc             Miscellaneous shared functions.
%% ================================================================
-module(estatsd_utils).
-export([ets_incr/3]).
-export([unixtime/0, unixtime/1]).
-export([num_to_str/1]).
-export([bin_to_hash/1]).

%% @doc Attempts to update the counter for the key; if this fails,
%% assume the failure resulted because the key does not exist, then
%% use increment_new/3
-spec ets_incr(Tid :: term(), Key :: atom() | string(), Amount :: integer()) -> integer().
ets_incr(Tid, Key, Amount) ->
    case catch ets:update_counter(Tid, Key, {2, Amount}) of
        N when is_integer(N) ->
            N;
        _ ->
            increment_new(Tid, Key, Amount)
    end.

%% @doc Attempts to insert a new key into Tid; if this fails, assume
%% the failure resulted because another process beat us to it, then
%% go back to ets_incr.
-spec increment_new(Tid :: term(), Key :: atom() | string(), Amoount :: integer()) -> integer().
increment_new(Tid, Key, Amount) ->
    case ets:insert_new(Tid, {Key, Amount}) of
        false -> 
            ets_incr(Tid, Key, Amount);
        _ -> 
            Amount
    end.

unixtime() -> unixtime(os:timestamp()).
unixtime({M, S, _}) -> (M * 1000000) + S.

num_to_str(NN) -> lists:flatten(io_lib:format("~w",[NN])).

bin_to_hash(Bin) when is_binary(Bin) ->
    bin_to_hash(Bin, []).

bin_to_hash(<<>>, R) ->
    string:to_lower(lists:flatten(lists:reverse(R)));
bin_to_hash(<<C,T/binary>>, R) ->
    bin_to_hash(T, [string:right(integer_to_list(C,16),2,$0)|R]).
