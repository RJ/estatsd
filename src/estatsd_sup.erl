-module(estatsd_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    Children = [
        {
            estatsd_server,
            {estatsd_server, start_link, []},
            permanent, 
            5000, 
            worker, 
            [estatsd_server]
        },
        {
            estatsd_listener,
            {estatsd_listener, start_link, []},
            permanent,
            5000,
            worker,
            [estatsd_listener]
        },
        ranch:child_spec(estatsd_tcp, 32, ranch_tcp, [{port, estatsd_utils:appvar(tcp_port, 8128)}], estatsd_tcp_protocol, []),
        ranch:child_spec(estatsd_tcpz, 32, ranch_tcp, [{port, estatsd_utils:appvar(tcpz_port, 8129)}], estatsd_tcpz_protocol, [])
    ],
    {ok, { {one_for_one, 10000, 10}, Children} }.
