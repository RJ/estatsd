%% ================================================================
%% @author          Zachary Hueras
%% @version         0.0.1
%% @doc             Basic TCP protocol for backend operations
%% ================================================================
-module(estatsd_tcp_protocol).
-compile([{parse_transform, ct_expand}]).
-include("estatsd.hrl").

-export([start_link/4]).
-export([init/4]).
-export([get_messages/1]).

start_link(ListenerPid, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [ListenerPid, Socket, Transport, Opts]),
    {ok, Pid}.

init(ListenerPid, Socket, Transport, _Opts) ->
    ok = ranch:accept_ack(ListenerPid),
    loop(Socket, Transport, []).

loop(Socket, Transport, Buffer) ->
    case Transport:recv(Socket, 0, 5000) of
        {ok, Data} ->
            {Messages, NewBuffer} = get_messages([Buffer, Data]),
            lists:foreach(fun estatsd_tcp:handle_message/1, Messages),
            loop(Socket, Transport, NewBuffer);
        _ ->
            ok = Transport:close(Socket)
    end.

get_messages([]) ->
    {[], []};
get_messages(Blob) ->
    case re:run(Blob, ?COMPILE_ONCE("\n$"), [{capture, none}]) of
        nomatch -> % Incomplete message at end
            Messages = re:split(Blob, ?COMPILE_ONCE("\n"), [trim]),
            [Incomplete|Complete] = lists:reverse(Messages),
            {Complete, Incomplete};
        match ->
            Messages = re:split(Blob, ?COMPILE_ONCE("\n"), [trim]),
            {Messages, []}
    end.
