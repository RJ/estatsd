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
-behaviour(gen_server).

-export([start_link/4]).

%-export([key2str/1,flush/0]). %% export for debugging 

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, 
         terminate/2, code_change/3]).

-record(state, {flush_interval,     % ms interval between stats flushing
                flush_timer,        % TRef of interval timer
                graphite_host,      % graphite server host
                graphite_port,      % graphite server port
                vm_metrics,         % flag to enable sending VM metrics on flush
                stats_tables,
                gauge_tables,
                timer_tables
               }).

start_link(FlushIntervalMs, GraphiteHost, GraphitePort, VmMetrics) ->
    gen_server:start_link({local, ?MODULE}, 
                          ?MODULE, 
                          [FlushIntervalMs, GraphiteHost, GraphitePort, VmMetrics], 
                          []).

%%

init([FlushIntervalMs, GraphiteHost, GraphitePort, VmMetrics]) ->
    error_logger:info_msg("estatsd will flush stats to ~p:~w every ~wms\n", 
                          [ GraphiteHost, GraphitePort, FlushIntervalMs ]),

    % 1. Create a table to hold the current table to use.
    ets:new(statsd, [named_table, set, public, {read_concurrency, true}]),

    % 2. Create two tables each for gauges and counters; double-buffer mentality :)
    TidStatsA   = ets:new(statsd_counters_a, [set, public, {write_concurrency, true}]),
    TidStatsB   = ets:new(statsd_counters_b, [set, public, {write_concurrency, true}]),
    TidGaugeA   = ets:new(statsd_gauge_a, [duplicate_bag, public, {write_concurrency, true}]),
    TidGaugeB   = ets:new(statsd_gauge_b, [duplicate_bag, public, {write_concurrency, true}]),
    TidTimerA   = ets:new(statsd_timer_a, [duplicate_bag, public, {write_concurrency, true}]),
    TidTimerB   = ets:new(statsd_timer_b, [duplicate_bag, public, {write_concurrency, true}]),

    % 3. Indicate which tables are currently our "write" buffers
    ets:insert(statsd, {stats, TidStatsA}),
    ets:insert(statsd, {gauge, TidGaugeA}),
    ets:insert(statsd, {timer, TidTimerA}),

    % 4. Set a timer to flush stats
    {ok, Tref} = timer:apply_interval(FlushIntervalMs, gen_server, cast, 
                                                       [?MODULE, flush]),
    State = #state{ 
        flush_interval  = FlushIntervalMs,
        flush_timer     = Tref,
        graphite_host   = GraphiteHost,
        graphite_port   = GraphitePort,
        vm_metrics      = VmMetrics,
        stats_tables    = {TidStatsA, TidStatsB},
        gauge_tables    = {TidGaugeA, TidGaugeB},
        timer_tables    = {TidTimerA, TidTimerB}
    },
    {ok, State}.

handle_cast(flush, State = #state{stats_tables = {CurrentStats, NewStats}, gauge_tables = {CurrentGauge, NewGauge}, timer_tables = {CurrentTimer, NewTimer}}) ->
    % 1. Flip tables externally
    ets:insert(statsd, [{stats, NewStats}, {gauge, NewGauge}, {timer, NewTimer}]),
    timer:sleep(500),    % Sleep to let any pending operations on the ets table complete.

    % 2. Gather data
    All     = ets:tab2list(CurrentStats),
    Gauges  = accumulate(ets:tab2list(CurrentGauge)),       % Gauges and timers are duplicate_bag tables; we need to consolidate
    Timers  = accumulate(ets:tab2list(CurrentTimer)),       % the objects the contain

    % 3. Do reports
    spawn(fun() -> do_report(All, Gauges, Timers, State) end),

    % 4. Clear our back-buffers
    ets:delete_all_objects(CurrentStats),
    ets:delete_all_objects(CurrentGauge),
    ets:delete_all_objects(CurrentTimer),

    % 5. Update state to flip tables internally
    NewState = State#state{stats_tables = {NewStats, CurrentStats}, gauge_tables = {NewGauge, CurrentGauge}, timer_tables = {NewTimer, CurrentTimer}},
    {noreply, NewState}.

handle_call(_,_,State)      -> {reply, ok, State}.

handle_info(_Msg, State)    -> {noreply, State}.

code_change(_, _, State)    -> {noreply, State}.

terminate(_, _)             -> ok.

%% INTERNAL STUFF
accumulate(List) ->
    lists:foldl(fun do_accumulate/2, [], List).

do_accumulate({Key, Value}, L) ->
    case lists:keyfind(Key, 1, L) of
        false -> 
            [{Key, [Value]}|L];
        {Key, Values} ->
            lists:keystore(Key, 1, L, {Key, [Value|Values]})
    end.

send_to_graphite(Msg, State) ->
    % io:format("SENDING: ~s\n", [Msg]),
    case gen_tcp:connect(State#state.graphite_host,
                         State#state.graphite_port,
                         [list, {packet, 0}]) of
        {ok, Sock} ->
            gen_tcp:send(Sock, Msg),
            gen_tcp:close(Sock),
            ok;
        E ->
            error_logger:error_msg("Failed to connect to graphite: ~p", [E]),
            E
    end.

% this string munging is damn ugly compared to javascript :(
key2str(K) when is_atom(K) -> 
    atom_to_list(K);
key2str(K) when is_binary(K) -> 
    key2str(binary_to_list(K));
key2str(K) when is_list(K) ->
    {ok, R1} = re:compile("\\s+"),
    {ok, R2} = re:compile("/"),
    {ok, R3} = re:compile("[^a-zA-Z_\\-0-9\\.]"),
    Opts = [global, {return, list}],
    S1 = re:replace(K,  R1, "_", Opts),
    S2 = re:replace(S1, R2, "-", Opts),
    S3 = re:replace(S2, R3, "", Opts),
    S3.

num2str(NN) -> lists:flatten(io_lib:format("~w",[NN])).

unixtime()  -> {Meg,S,_Mic} = erlang:now(), Meg*1000000 + S.

%% Aggregate the stats and generate a report to send to graphite
do_report(All, Gauges, Timers, State) ->
    % One time stamp string used in all stats lines:
    TsStr = num2str(unixtime()),
    {MsgCounters, NumCounters}          = do_report_counters(TsStr, All, State),
    {MsgTimers, NumTimers}              = do_report_timers(TsStr, Timers, State),
    {MsgGauges, NumGauges}              = do_report_gauges(Gauges),
    {MsgVmMetrics, NumVmMetrics}        = do_report_vm_metrics(TsStr, State),
    %% REPORT TO GRAPHITE
    case NumTimers + NumCounters + NumGauges + NumVmMetrics of
        0 -> nothing_to_report;
        NumStats ->
            FinalMsg = [ MsgCounters,
                         MsgTimers,
                         MsgGauges,
                         MsgVmMetrics,
                         %% Also graph the number of graphs we're graphing:
                         "stats.num_stats ", num2str(NumStats), " ", TsStr, "\n"
                       ],
            send_to_graphite(FinalMsg, State)
    end.

do_report_counters(TsStr, All, State) ->
    Msg = lists:foldl(
                fun({Key, Val0}, Acc) ->
                        KeyS = key2str(Key),
                        Val = Val0 / (State#state.flush_interval/1000),
                        %% Build stats string for graphite
                        Fragment = [ "stats.counters.", KeyS, " ", 
                                     io_lib:format("~w", [Val]), " ", 
                                     TsStr, "\n",

                                     "stats.counters.counts.", KeyS, " ", 
                                     io_lib:format("~w",[Val0]), " ", 
                                     TsStr, "\n"
                                   ],
                        [ Fragment | Acc ]                    
                end, [], All),
    {Msg, length(All)}.

do_report_timers(TsStr, Timings, State) ->
    Msg = lists:foldl(
        fun({Key, Vals}, Acc) ->
                KeyS = key2str(Key),
                Values          = lists:sort(Vals),
                Count           = length(Values),
                Min             = hd(Values),
                Max             = lists:last(Values),
                PctThreshold    = 90,
                ThresholdIndex  = erlang:round(((100-PctThreshold)/100)*Count),
                NumInThreshold  = Count - ThresholdIndex,
                Values1         = lists:sublist(Values, NumInThreshold),
                MaxAtThreshold  = lists:nth(NumInThreshold, Values),
                Mean            = lists:sum(Values1) / NumInThreshold,
                %% Build stats string for graphite
                Startl          = [ "stats.timers.", KeyS, "." ],
                Endl            = [" ", TsStr, "\n"],
                Fragment        = [ [Startl, Name, " ", num2str(Val), Endl] || {Name,Val} <-
                                  [ {"mean", Mean},
                                    {"upper", Max},
                                    {"upper_"++num2str(PctThreshold), MaxAtThreshold},
                                    {"lower", Min},
                                    {"count", Count}
                                  ]],
                [ Fragment | Acc ]
        end, [], Timings),
    {Msg, length(Msg)}.

do_report_gauges(Gauges) ->
    Msg = lists:foldl(
        fun({Key, Vals}, Acc) ->
            KeyS = key2str(Key),
            Fragments = lists:foldl(
                fun ({Val, TsStr}, KeyAcc) ->
                    %% Build stats string for graphite
                    Fragment = [
                        "stats.gauges.", KeyS, " ",
                        io_lib:format("~w", [Val]), " ",
                        TsStr, "\n"
                    ],
                    [ Fragment | KeyAcc ]
                end, [], Vals
            ),
            [ Fragments | Acc ]
        end, [], Gauges
    ),
    {Msg, length(Gauges)}.

do_report_vm_metrics(TsStr, State) ->
    case State#state.vm_metrics of
        true ->
            NodeKey = node_key(),
            {TotalReductions, Reductions} = erlang:statistics(reductions),
            {NumberOfGCs, WordsReclaimed, _} = erlang:statistics(garbage_collection),
            {{input, Input}, {output, Output}} = erlang:statistics(io),
            RunQueue = erlang:statistics(run_queue),
            StatsData = [
                         {process_count, erlang:system_info(process_count)},
                         {reductions, Reductions},
                         {total_reductions, TotalReductions},
                         {number_of_gcs, NumberOfGCs},
                         {words_reclaimed, WordsReclaimed},
                         {input, Input},
                         {output, Output},
                         {run_queue, RunQueue}
                        ],
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
            end, erlang:memory()),
            Msg = StatsMsg ++ MemoryMsg;
        false ->
            Msg = []
    end,
    {Msg, length(Msg)}.

node_key() ->
    NodeList = atom_to_list(node()),
    {ok, R} = re:compile("[\@\.]"),
    Opts = [global, {return, list}],
    S = re:replace(NodeList,  R, "_", Opts),
    key2str(S).
