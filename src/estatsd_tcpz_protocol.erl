%% ================================================================
%% @author          Zachary Hueras
%% @version         0.0.1
%% @doc             Basic TCP protocol for backend operations
%% ================================================================
-module(estatsd_tcpz_protocol).
-extends(estatsd_tcp_protocol).
-compile([{parse_transform, ct_expand}]).

-export([start_link/4]).
-export([init/4]).

start_link(ListenerPid, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [ListenerPid, Socket, Transport, Opts]),
    {ok, Pid}.

init(ListenerPid, Socket, Transport, _Opts) ->
    ok = ranch:accept_ack(ListenerPid),
    loop(Socket, Transport, []).

loop(Socket, Transport, Buffer) ->
    case Transport:recv(Socket, 0, 5000) of
        {ok, Data} ->
            {Messages, NewBuffer} = ?MODULE:get_messages([Buffer, Data]),
            lists:foreach(fun estatsd_tcp:handle_message/1, [ estatsd_tcp:decompress(Message) || Message <- Messages ]),
            loop(Socket, Transport, NewBuffer);
        SomethingElse ->
            ok = Transport:close(Socket)
    end.

