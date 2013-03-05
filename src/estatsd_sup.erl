-module(estatsd_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-define(FLUSH_INTERVAL, appvar(flush_interval, 10000)).
-define(GRAPHITE_HOST,  appvar(graphite_host,  "127.0.0.1")).
-define(GRAPHITE_PORT,  appvar(graphite_port,  2003)).
-define(VM_METRICS,     appvar(vm_metrics,  true)).
-define(PATH_PREFIX,    appvar(path_prefix,  "stats")).

%% ===================================================================
%% API functions
%% ===================================================================


start_link() ->
    start_link([]).

start_link(Options) ->
    supervisor:start_link({local, ?MODULE}, 
                          ?MODULE, 
                          [Options]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([Options]) ->
    Config = [
        proplists:get_value(flush_interval, Options, ?FLUSH_INTERVAL),
        proplists:get_value(graphite_host,  Options, ?GRAPHITE_HOST),
        proplists:get_value(graphite_port,  Options, ?GRAPHITE_PORT),
        proplists:get_value(vm_metrics,     Options, ?VM_METRICS),
        proplists:get_value(path_prefix,    Options, ?PATH_PREFIX)
    ],

    Children = [
        {estatsd_server, 
         {estatsd_server, start_link, [Config]},
         permanent, 5000, worker, [estatsd_server]}
    ],
    {ok, { {one_for_one, 10000, 10}, Children} }.

appvar(K, Def) ->
    case application:get_env(estatsd, K) of
        {ok, Val} -> Val;
        undefined -> Def
    end.
