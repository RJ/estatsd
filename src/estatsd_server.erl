%% Stats aggregation process that periodically dumps data to graphite
%% Will calculate 90th percentile etc.
%% Inspired by etsy statsd:
%% http://codeascraft.etsy.com/2011/02/15/measure-anything-measure-everything/
%%
%% This could be extended to take a callback for reporting mechanisms.
%% Right now it's hardcoded to stick data into graphite.
%%
%% Richard Jones <rj@metabrew.com>
%%
-module(estatsd_server).
-behaviour(gen_leader).
-compile([{parse_transform, ct_expand}]).
-export([start_link/0]).
-include("estatsd.hrl").

-export([node_key/0,key2str/1]).%,flush/0]). %% export for debugging 

-export([init/1, handle_call/4, handle_cast/3, handle_info/3,
         handle_leader_call/4, handle_leader_cast/3, handle_DOWN/3,
         elected/3, surrendered/3, from_leader/3, terminate/2,
         code_change/4]).

-record(state, {
        flush_interval      = 10000,            % ms interval between stats flushing
        last_flush          = os:timestamp(),   % erlang-style timestamp of last flush (or start time)
        flush_timer         = undefined,        % TRef of interval timer
        destination         = undefined,        % What to do every flush interval
        is_leader           = false,
        aggregate           = [],
        enable_node_tagging = false,
        node_tagging        = [],
        cluster_tagging     = [],
        vm_metrics          = true,             % flag to enable sending VM metrics on flush
        stats_tables        = undefined,        % Evantually a tuple with two Tids for stats
        gauge_tables        = undefined,        % Eventually a tuple with two Tids for gauges
        timer_tables        = undefined         % Eventually a tuple with two Tids for timers
    }).

start_link() ->
    Nodes = case application:get_env(estatsd, peers) of
        undefined -> [node()];
        {ok, Peers} -> Peers
    end,
    gen_leader:start_link(?MODULE, Nodes, [], ?MODULE, [], [{spawn_opt, [{priority, high}]}]).

%%

init([]) ->
    % 1. Initialize our state.
    State = #state{flush_interval = FlushInterval} = init_state(),

    % 2. Create a table to hold the current table to use. Optimize for read operations,
    % as we'll only be writing once every FlushIntervalMs
    ets:new(statsd, [named_table, set, public, {read_concurrency, true}]),

    % 3. Create two tables each for gauges and counters; double-buffer mentality :)
    % Use duplicate bags to accomodate multiple entries for each key in gauge and timer tables
    % Optimize for write operations
    ets:new(statsd_counters_agg, [named_table, set, public, {write_concurrency, true}]),
    ets:new(statsd_gauge_agg, [named_table, set, public, {write_concurrency, true}]),
    ets:new(statsd_timer_agg, [named_table, set, public, {write_concurrency, true}]),
    TidStatsA   = ets:new(statsd_counters_a, [set, public, {write_concurrency, true}]),
    TidStatsB   = ets:new(statsd_counters_b, [set, public, {write_concurrency, true}]),
    TidGaugeA   = ets:new(statsd_gauge_a, [duplicate_bag, public, {write_concurrency, true}]),
    TidGaugeB   = ets:new(statsd_gauge_b, [duplicate_bag, public, {write_concurrency, true}]),
    TidTimerA   = ets:new(statsd_timer_a, [set, public, {write_concurrency, true}]),
    TidTimerB   = ets:new(statsd_timer_b, [set, public, {write_concurrency, true}]),

    % 4. Indicate which tables are currently our "write" buffers
    ets:insert(statsd, {stats, TidStatsA}),
    ets:insert(statsd, {gauge, TidGaugeA}),
    ets:insert(statsd, {timer, TidTimerA}),

    % 5. Set a timer to flush stats
    {ok, Tref} = timer:apply_interval(FlushInterval, gen_leader, cast, [?MODULE, flush]),

    {ok, State#state{
            flush_timer     = Tref,
            stats_tables    = {TidStatsA, TidStatsB},   % See? I told you they'd be tuples with two Tids
            gauge_tables    = {TidGaugeA, TidGaugeB},
            timer_tables    = {TidTimerA, TidTimerB}
        }}.

init_state() ->
    NodeTagging     = parse_tagging(estatsd_utils:appvar(node_tagging, [])),
    ClusterTagging  = parse_tagging(estatsd_utils:appvar(cluster_tagging, [])),
    #state{ 
        flush_interval      = estatsd_utils:appvar(flush_interval, 10000),
        destination         = estatsd_utils:appvar(destination,  {graphite, "127.0.0.1", 2003}),
        vm_metrics          = estatsd_utils:appvar(vm_metrics,  false),
        enable_node_tagging = estatsd_utils:appvar(enable_node_tagging, false),
        node_tagging        = NodeTagging,
        cluster_tagging     = ClusterTagging
    }.

parse_tagging(Tagging) ->
    [ parse_tag(Tag) || Tag <- Tagging ].

parse_tag({KeyRX, Type, Position, Affix}) ->
    {ok, RX} = re:compile(KeyRX),
    {RX, Type, Position, parse_affix(Affix)}.

parse_affix(node_key)   -> node_key();
parse_affix(String)     -> String.

elected(State, _Election, undefined) ->
    Synch = [],
    {ok, Synch, State#state{is_leader = true}};
elected(State, _Election, _Node) ->
    {reply, [], State}.

surrendered(State = #state{aggregate = VMs}, _Sync, _Election) ->
    Counters    = ets:tab2list(statsd_counters_agg),
    Gauges      = ets:tab2list(statsd_gauge_agg),
    Timers      = ets:tab2list(statsd_timer_agg),

    estatsd:aggregate(Counters, Gauges, Timers, VMs),

    {ok, State#state{is_leader = false, aggregate = []}}.

handle_cast(flush, State = #state{aggregate = Aggregate, stats_tables = {CurrentStats, NewStats}, gauge_tables = {CurrentGauge, NewGauge}, timer_tables = {CurrentTimer, NewTimer}}, _Election) ->
    % 1. Flip tables externally
    ets:insert(statsd, [{stats, NewStats}, {gauge, NewGauge}, {timer, NewTimer}]),

    % 2. Sleep for a little bit to allow pending operations to finish
    timer:sleep(100),

    % 3. Gather data
    All     = get_counters(CurrentStats, State),
    Gauges  = get_gauges(CurrentGauge, State),
    Timers  = get_timers(CurrentTimer, State),
    VM      = get_vm_metrics(Aggregate, State),

    % 4. Do reports
    CurrTime = os:timestamp(),
    do_report(All, Timers, Gauges, VM, CurrTime, State),

    % 5. Clear our back-buffers
    ets:delete_all_objects(CurrentStats),
    ets:delete_all_objects(CurrentGauge),
    ets:delete_all_objects(CurrentTimer),

    % 6. Update state to flip tables internally
    NewState = State#state{
        last_flush      = CurrTime,                     % Also, update the last flush so our calculations are, you know, accurate.
        aggregate       = [],
        stats_tables    = {NewStats, CurrentStats}, 
        gauge_tables    = {NewGauge, CurrentGauge}, 
        timer_tables    = {NewTimer, CurrentTimer}
    },
    {noreply, NewState}.

handle_leader_cast({aggregate, Counters, Timers, Gauges, VMs}, State = #state{aggregate = Aggregate}, _Election) ->
    spawn(fun() ->
                lists:foreach(fun({K, V}) -> estatsd_utils:ets_incr(statsd_counters_agg, K, V) end, Counters),
                lists:foreach(fun(Gauge) -> ets:insert(statsd_gauge_agg, Gauge) end, Gauges),
                lists:foreach(fun({K, Vs}) -> lists:foreach(fun({Dur, C}) -> estatsd_utils:ets_incr(statsd_timer_agg, {K,Dur}, C) end, Vs) end, Timers)
        end),
    {noreply, State#state{aggregate = Aggregate ++ VMs}};
handle_leader_cast(_Cast, State, _Election) ->
    {noreply, State}.

handle_call(_Call,_,State, _Election) -> 
    {reply, ok, State}.

handle_leader_call(_Call, _From, State, _Election) ->
    {reply, ok, State}.

handle_info(_Msg, State, _Election) -> 
    {noreply, State}.

handle_DOWN(_Node, State, _Election) ->
    {ok, State}.

from_leader(_Synch, State, _Election) ->
    {ok, State}.

code_change(_, _, State, _Election) -> 
    {noreply, State}.

terminate(_, _) -> 
    ok.

%% INTERNAL STUFF
get_counters(Tid, _State = #state{is_leader = false, enable_node_tagging = false}) ->
    [ {key2str(K),V} || {K,V} <- ets:tab2list(Tid) ];
get_counters(Tid, _State = #state{is_leader = false, enable_node_tagging = true, node_tagging = []}) ->
    [ {key2str(K),V} || {K,V} <- ets:tab2list(Tid) ];
get_counters(Tid, _State = #state{is_leader = false, enable_node_tagging = true, node_tagging = NodeTagging}) ->
    tag_metrics(ets:tab2list(Tid), NodeTagging);
get_counters(Tid, State = #state{is_leader = true}) ->
    LocalCounters = get_counters(Tid, State#state{is_leader = false}),
    lists:foreach(fun({K, V}) -> estatsd_utils:ets_incr(statsd_counters_agg, K, V) end, LocalCounters),
    Counters = ets:tab2list(statsd_counters_agg),
    ets:delete_all_objects(statsd_counters_agg),
    Counters.

get_timers(Tid, _State = #state{is_leader = false, enable_node_tagging = false}) ->
    accumulate_timers(ets:tab2list(Tid));
get_timers(Tid, _State = #state{is_leader = false, enable_node_tagging = true, node_tagging = []}) ->
    accumulate_timers(ets:tab2list(Tid));
get_timers(Tid, State = #state{is_leader = false, enable_node_tagging = true, node_tagging = NodeTagging}) ->
    Timers = get_timers(Tid, State#state{enable_node_tagging = false}),
    tag_metrics(Timers, NodeTagging);
get_timers(Tid, State = #state{is_leader = true}) ->
    LocalTimers = get_timers(Tid, State#state{is_leader = false}),
    lists:foreach(fun({K,V}) -> lists:foreach(fun({D,C}) -> estatsd_utils:ets_incr(statsd_timer_agg, {K,D}, C) end, V) end, LocalTimers),
    Timers = accumulate_timers(ets:tab2list(statsd_timer_agg)),
    ets:delete_all_objects(statsd_timer_agg),
    Timers.
    
merge_accumulation({K, Values}, Acc) ->
    case lists:keyfind(K, 1, Acc) of
        false -> [{K, Values}|Acc];
        {K, OldValues} -> lists:keystore(K, 1, Acc, {K, OldValues ++ Values})
    end.

get_gauges(Tid, _State = #state{is_leader = false, enable_node_tagging = false}) ->
    [ {key2str(K), V} || {K,V} <- accumulate(ets:tab2list(Tid)) ];
get_gauges(Tid, _State = #state{is_leader = false, enable_node_tagging = true, node_tagging = []}) ->
    [ {key2str(K), V} || {K,V} <- accumulate(ets:tab2list(Tid)) ];
get_gauges(Tid, _State = #state{is_leader = false, enable_node_tagging = true, node_tagging = NodeTagging}) ->
    tag_metrics(accumulate(ets:tab2list(Tid)), NodeTagging);
get_gauges(Tid, State = #state{is_leader = true}) ->
    LocalGauges = get_gauges(Tid, State#state{is_leader = false}),
    Gauges = accumulate(ets:tab2list(statsd_gauge_agg)),
    ets:delete_all_objects(statsd_gauge_agg),
    lists:foldl(fun merge_accumulation/2, Gauges, LocalGauges).

%% Don't apply node tagging rules for VM stats. They are not
%% user-generated keys.
get_vm_metrics(Aggregate, _State = #state{vm_metrics = false}) ->
    Aggregate;
get_vm_metrics(_Aggregate, _State = #state{is_leader = false}) ->
    [{node_key(), get_local_metrics()}];
get_vm_metrics(Aggregate, State = #state{is_leader = true}) ->
    [LocalMetrics] = get_vm_metrics([], State#state{is_leader = false}),
    [LocalMetrics|Aggregate].

get_local_metrics() ->
    {TotalReductions, Reductions} = erlang:statistics(reductions),
    {NumberOfGCs, WordsReclaimed, _} = erlang:statistics(garbage_collection),
    {{input, Input}, {output, Output}} = erlang:statistics(io),
    RunQueue = erlang:statistics(run_queue),
    StatsData = [
                 {'process_count', erlang:system_info(process_count)},
                 {'reductions', Reductions},
                 {'total_reductions', TotalReductions},
                 {'number_of_gcs', NumberOfGCs},
                 {'words_reclaimed', WordsReclaimed},
                 {'input', Input},
                 {'output', Output},
                 {'run_queue', RunQueue}
                ],
    MemoryData = erlang:memory(),
    {StatsData, MemoryData}.

accumulate(List) ->
    lists:foldl(fun do_accumulate/2, [], List).

do_accumulate({Key, Value}, L) ->
    case lists:keyfind(Key, 1, L) of
        false -> 
            [{Key, [Value]}|L];
        {Key, Values} ->
            lists:keystore(Key, 1, L, {Key, [Value|Values]})
    end.

accumulate_timers(List) ->
    Res =[ {key2str(Key), dict:to_list(Values)} || {Key, Values} <- dict:to_list(lists:foldl(fun do_accumulate_timers/2, dict:new(), List)) ],
    Res.

do_accumulate_timers({{Key, Dur}, C}, Timers) ->
    dict:update(Key, fun(Durations) ->
                dict:update(Dur, fun(Count) ->
                            Count + C
                    end, C, Durations)
        end, dict:from_list([{Dur,C}]), Timers).

send_to_graphite(Msg, GraphiteHost, GraphitePort) ->
    case gen_tcp:connect(GraphiteHost, GraphitePort, [list, {packet, 0}]) of
        {ok, Sock} ->
            gen_tcp:send(Sock, Msg),
            gen_tcp:close(Sock),
            ok;
        E ->
%            error_logger:error_msg("Failed to connect to graphite: ~p", [E]),
            E
    end.

% this string munging is damn ugly compared to javascript :(
key2str(K) when is_atom(K) -> 
    atom_to_list(K);
key2str(K) when is_binary(K) -> 
    key2str(binary_to_list(K));
key2str(K) when is_list(K) ->
    Opts = [global, {return, list}],
    lists:foldl(fun({Rx, Replace}, S) -> re:replace(S, Rx, Replace, Opts) end, K, [
            {?COMPILE_ONCE("\\s+"), "_"},
            {?COMPILE_ONCE("/"), "-"},
            {?COMPILE_ONCE("[^a-zA-Z_\\-0-9\\.]"), ""}
        ]).

do_report(All, Timers, Gauges, VM, CurrTime, State = #state{is_leader = true, cluster_tagging = ClusterTagging = [_|_]}) ->
    {All1, Timers1, Gauges1} = tag_metrics({All, Timers, Gauges}, ClusterTagging),
    do_report(All1, Timers1, Gauges1, VM, CurrTime, State#state{cluster_tagging = []});
do_report(All, Timers, Gauges, VM, CurrTime, State = #state{is_leader = true, destination = {graphite, GraphiteHost, GraphitePort}}) ->
    % One time stamp string used in all stats lines:
    Duration                        = timer:now_diff(CurrTime, State#state.last_flush) / 1000000,
    TsStr                           = estatsd_utils:num_to_str(estatsd_utils:unixtime(CurrTime)),
    {MsgCounters, NumCounters}      = do_report_counters(TsStr, All, Duration),
    {MsgTimers, NumTimers}          = do_report_timers(TsStr, Timers),
    {MsgGauges, NumGauges}          = do_report_gauges(Gauges),
    {MsgVmMetrics, NumVmMetrics}    = do_report_vm_metrics(VM, TsStr, State),
    %% REPORT TO GRAPHITE
    case NumTimers + NumCounters + NumGauges + NumVmMetrics of
        0 -> 
            nothing_to_report;
        NumStats ->
            FinalMsg = [ MsgCounters,
                         MsgTimers,
                         MsgGauges,
                         MsgVmMetrics,
                         %% Also graph the number of graphs we're graphing:
                         "statsd.num_stats ", estatsd_utils:num_to_str(NumStats), " ", TsStr, "\n"
                       ],
            send_to_graphite(FinalMsg, GraphiteHost, GraphitePort)
    end;
%% TODO: Make everything below this point less atrocious.
do_report(All, Timers, Gauges, VM, _CurrTime, _State = #state{is_leader = true, destination = Destination}) ->
    estatsd_tcp:send(Destination, All, Timers, Gauges, VM);
do_report(All, Timers, Gauges, VM, _CurrTime, _State = #state{is_leader = false}) ->
    estatsd:aggregate(All, Timers, Gauges, VM).

tag_metrics(Metrics, Tags) when is_list(Metrics) ->
    lists:foldl(fun apply_tags/2, Metrics, Tags);
tag_metrics(Metrics, Tags) when is_tuple(Metrics) ->
    list_to_tuple([ lists:foldl(fun apply_tags/2, Values, Tags) || Values <- tuple_to_list(Metrics) ]).

apply_tags(Tag, Values) ->
    lists:foldl(fun(Value, Acc) -> apply_tag(Tag, Value, Acc) end, [], Values).

apply_tag({KeyPattern, copy, Position, Affix}, {Key0, Value}, Acc) ->
    Key = key2str(Key0),
    case re:run(Key, KeyPattern, [{capture, none}]) of
        match   -> [{affix(Key, Position, Affix), Value}, {Key, Value} | Acc];
        nomatch -> [{Key, Value} | Acc]
    end;
apply_tag({KeyPattern, replace, Position, Affix}, {Key0, Value}, Acc) ->
    Key = key2str(Key0),
    case re:run(Key, KeyPattern, [{capture, none}]) of
        match   -> [{affix(Key, Position, Affix), Value}|Acc];
        nomatch -> [{Key, Value}|Acc]
    end.

affix(Key, prefix, Affix) ->
    key2str([Affix, ".", Key]);
affix(Key, suffix, Affix) ->
    key2str([Key, ".", Affix]).

do_report_counters(TsStr, All, Duration) ->
    Msg = lists:foldl(
                fun({Key, Val0}, Acc) ->
                        KeyS = key2str(Key),
                        Val = Val0 / Duration,
                        %% Build stats string for graphite
                        Fragment = [ "stats.", KeyS, " ", 
                                     io_lib:format("~w", [Val]), " ", 
                                     TsStr, "\n",

                                     "stats_counts.", KeyS, " ", 
                                     io_lib:format("~w",[Val0]), " ", 
                                     TsStr, "\n"
                                   ],
                        [ Fragment | Acc ]                    
                end, [], All),
    {Msg, length(All)}.

do_report_timers(TsStr, Timings) ->
    Msg = lists:foldl(
        fun({Key, Vals}, Acc) ->
                KeyS            = key2str(Key),
                Values          = lists:keysort(1, Vals),
                {Min,_}         = hd(Values),
                {Max,_}         = lists:last(Values),
                Count           = get_timer_count(Vals),
                PctThreshold    = 90,
                ThresholdIndex  = erlang:round(((100-PctThreshold)/100)*Count),
                NumInThreshold  = Count - ThresholdIndex,
                {MaxAtThreshold, Mean} = get_timer_mean(Values, NumInThreshold),
                %% Build stats string for graphite
                Startl          = [ "stats.timers.", KeyS, "." ],
                Endl            = [" ", TsStr, "\n"],
                Fragment        = [ [Startl, Name, " ", estatsd_utils:num_to_str(Val), Endl] || {Name,Val} <-
                                  [ {"mean", Mean},
                                    {"upper", Max},
                                    {"upper_"++estatsd_utils:num_to_str(PctThreshold), MaxAtThreshold},
                                    {"lower", Min},
                                    {"count", Count}
                                  ]],
                [ Fragment | Acc ]
        end, [], Timings),
    {Msg, length(Msg)}.

get_timer_count(Vals) ->
    lists:foldl(fun({_Dur, C}, Acc) -> Acc + C end, 0, Vals).

get_timer_mean(Vals, N) ->
    {Max, Sum} = do_get_timer_mean(Vals, N, 0),
    case N of
        0 -> {Max, Sum};
        _ -> {Max, Sum / N}
    end.

do_get_timer_mean([{Max,_}|_], 0, Sum) ->
    {Max, Sum};
do_get_timer_mean([{Dur, C}|T], Point, Acc) ->
    case Point - C of
        N when N > 0 -> do_get_timer_mean(T, N, Acc + (Dur * C));
        N when N =< 0 -> do_get_timer_mean([{Dur, C + N}|T], 0, Acc + (Dur * (C + N)))
    end;
do_get_timer_mean([], _Point, _Sum) ->
    exit(badarg).

do_report_gauges(Gauges) ->
    Msg = lists:foldl(
        fun({Key, Vals}, Acc) ->
            KeyS = key2str(Key),
            Fragments = lists:foldl(
                fun ({Val, TS}, KeyAcc) ->
                    %% Build stats string for graphite
                    Fragment = [
                        "stats.gauges.", KeyS, " ",
                        io_lib:format("~w", [Val]), " ",
                        integer_to_list(TS), "\n"
                    ],
                    [ Fragment | KeyAcc ]
                end, [], Vals
            ),
            [ Fragments | Acc ]
        end, [], Gauges
    ),
    {Msg, length(Gauges)}.

do_report_vm_metrics(VMs, TsStr, State) ->
    case State#state.vm_metrics of
        true ->
            {TsStr, Msg, C} = lists:foldl(fun build_vm_stats/2, {TsStr, [], 0}, VMs),
            {Msg, C};
        false ->
            {[], 0}
    end.

build_vm_stats({NodeKey, {StatsData, MemoryData}}, {TsStr, Acc, C}) ->
    StatsMsg = lists:map(fun({Key, Val}) ->
        [
         "stats.vm.", NodeKey, ".stats.", key2str(Key), " ",
         io_lib:format("~w", [Val]), " ",
         TsStr, "\n"
        ]
    end, StatsData),
    MemoryMsg = lists:map(fun({Key, Val}) ->
        [
         "stats.vm.", NodeKey, ".memory.", key2str(Key), " ",
         io_lib:format("~w", [Val]), " ",
         TsStr, "\n"
        ]
    end, MemoryData),
    NumStats = length(StatsData) + length(MemoryData),
    Msg = [StatsMsg, MemoryMsg],
    {TsStr, [Acc, Msg], C + NumStats}.

node_key() ->
    NodeList = atom_to_list(node()),
    {ok, R} = re:compile("[\@\.]"),
    Opts = [global, {return, list}],
    S = re:replace(NodeList,  R, "_", Opts),
    key2str(S).
