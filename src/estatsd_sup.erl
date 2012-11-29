-module(estatsd_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, start_link/1, start_link/3, start_link/4, start_link/5]).

%% Supervisor callbacks
-export([init/1]).

-define(FLUSH_INTERVAL, appvar(flush_interval, 10000)).
-define(GRAPHITE_HOST,  appvar(graphite_host,  "127.0.0.1")).
-define(GRAPHITE_PORT,  appvar(graphite_port,  2003)).
-define(VM_METRICS,     appvar(vm_metrics,  true)).
-define(MASTER_NODE,    appvar(master_node, undefined)).

%% ===================================================================
%% API functions
%% ===================================================================


start_link() ->
    start_link( ?FLUSH_INTERVAL, ?GRAPHITE_HOST, ?GRAPHITE_PORT, ?VM_METRICS).

start_link(FlushIntervalMs) ->
    start_link( FlushIntervalMs, ?GRAPHITE_HOST, ?GRAPHITE_PORT, ?VM_METRICS).

start_link(FlushIntervalMs, GraphiteHost, GraphitePort) ->
    start_link( FlushIntervalMs, GraphiteHost, GraphitePort, ?VM_METRICS).

start_link(FlushIntervalMs, GraphiteHost, GraphitePort, VmMetrics) ->
    start_link(FlushIntervalMs, GraphiteHost, GraphitePort, VmMetrics, ?MASTER_NODE).

start_link(FlushIntervalMs, GraphiteHost, GraphitePort, VmMetrics, MasterNode) ->
    supervisor:start_link({local, ?MODULE}, 
                          ?MODULE, 
                          [FlushIntervalMs, GraphiteHost, GraphitePort, VmMetrics, MasterNode]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([FlushIntervalMs, GraphiteHost, GraphitePort, VmMetrics, MasterNode]) ->
    Children = [
        {estatsd_server, 
         {estatsd_server, start_link, 
             [FlushIntervalMs, GraphiteHost, GraphitePort, VmMetrics, MasterNode]},
         permanent, 5000, worker, [estatsd_server]},
        {
            estatsd_listener,
            {estatsd_listener, start_link, []},
            permanent,
            5000,
            worker,
            [estatsd_listener]
        }
    ],
    {ok, { {one_for_one, 10000, 10}, Children} }.

appvar(K, Def) ->
    case application:get_env(estatsd, K) of
        {ok, Val} -> Val;
        undefined -> Def
    end.
