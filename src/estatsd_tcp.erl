%% ================================================================
%% @author          Zachary Hueras
%% @version         0.0.1
%% @doc             Functions relating to TCP syncing between
%%                  estatsd instances.
%% ================================================================
-module(estatsd_tcp).
-export([send/5]).
-export([handle_message/1]).
-export([make_signature/4]).
-export([compress/1,decompress/1]).
-define(COMPRESS_SEQUENCE, [
        {fun zlib:compress/1, fun zlib:uncompress/1},
        {fun base64:encode/1, fun base64:decode/1}
    ]).

send(Destination, Counters, Timers, Gauges, VMs) ->
    Message = {[
                {<<"command">>, <<"aggregate">>},
                {counters,      {[ counter_to_json(Counter) || Counter <- Counters]}},
                {timers,        {[ timer_to_json(Timer) || Timer <- Timers ]}},
                {gauges,        {[ gauge_to_json(Gauge) || Gauge <- Gauges ]}},
                {vm,            {[ vm_to_json(VM) || VM <- VMs ]}},
                {signature,     list_to_binary(make_signature(Counters, Timers, Gauges, VMs))}
            ]},
    JSON = jiffy:encode(Message),
    do_send(Destination, JSON).

counter_to_json({Key, Value}) ->
    {iolist_to_binary(Key), Value}.

counter_from_json({Key, Value}) ->
    {binary_to_list(Key), Value}.

timer_to_json({Key, Value}) ->
    {iolist_to_binary(Key), Value}.

timer_from_json({Key, Value}) ->
    {binary_to_list(Key), Value}.

gauge_to_json({Key, Values}) ->
    {iolist_to_binary(Key), [ [Value, TS] || {Value, TS} <- Values ]}.

gauge_from_json({Key, Values}) ->
    {binary_to_list(Key), [ {Value, TS} || [Value, TS] <- Values ]}.

vm_to_json({NodeKey, {StatsData, MemoryData}}) ->
    {iolist_to_binary(NodeKey), {[{stats, {StatsData}}, {memory, {MemoryData}}]}}.

vm_from_json({NodeKey, {Properties}}) ->
    case lists:keyfind(<<"stats">>, 1, Properties) of
        false ->
            StatsData = [];
        {_, {StatsData}} ->
            ok
    end,
    case lists:keyfind(<<"memory">>, 1, Properties) of
        false ->
            MemoryData = [];
        {_, {MemoryData}} ->
            ok
    end,
    {NodeKey, {StatsData, MemoryData}}.

make_signature(Counters, Timers, Gauges, VM) ->
    Context0 = crypto:hmac_init(sha, "estatsd"),
    Context1 = lists:foldl(fun (Data, ContextAcc) -> crypto:hmac_update(ContextAcc, Data) end, Context0, [ integer_to_list(I) || I <- [length(Counters), length(Timers), length(Gauges), length(VM)] ]),
    estatsd_utils:bin_to_hash(crypto:hmac_final(Context1)).

do_send({estatsd_tcpz, Host, Port}, Message) ->
    Compressed = compress(Message),
    do_send({estatsd_tcp, Host, Port}, Compressed);
do_send({estatsd_tcp, Host, Port}, Message) ->
    case gen_tcp:connect(Host, Port, [{active, false}]) of
        {ok, Socket} ->
            gen_tcp:send(Socket, [Message, "\n"]),
            gen_tcp:close(Socket);
        {error, Reason} ->
            error_logger:error_msg("Failed to forward statistics to ~w:~w: ~w", [Host, Port, Reason])
    end,
    ok.

compress(Data) when is_list(Data) ->
    compress(iolist_to_binary(Data));
compress(Data) when is_binary(Data) ->
    lists:foldl(fun ({Encode, _Decode}, S) -> Encode(S) end, Data, ?COMPRESS_SEQUENCE).

decompress(Data) when is_binary(Data) ->
    lists:foldl(fun ({_Encode, Decode}, S) -> Decode(S) end, Data, lists:reverse(?COMPRESS_SEQUENCE)).

handle_message(Message) ->
    {Properties} = jiffy:decode(Message),
    case lists:keyfind(<<"command">>, 1, Properties) of
        false ->
            error_logger:error_msg("Bad Command Format: ~p~n", [Message]),
            ok;
        {_, <<"aggregate">>} ->
            {_, {Counters0}}    = lists:keyfind(<<"counters">>, 1, Properties),
            {_, {Timers0}}      = lists:keyfind(<<"timers">>, 1, Properties),
            {_, {Gauges0}}      = lists:keyfind(<<"gauges">>, 1, Properties),
            {_, {VMs0}}         = lists:keyfind(<<"vm">>, 1, Properties),


            Counters            = [ counter_from_json(Counter) || Counter <- Counters0 ],
            Timers              = [ timer_from_json(Timer) || Timer <- Timers0 ],
            Gauges              = [ gauge_from_json(Gauge) || Gauge <- Gauges0 ],
            VMs                 = [ vm_from_json(VM) || VM <- VMs0 ],

            estatsd:aggregate(Counters, Timers, Gauges, VMs)
    end.
