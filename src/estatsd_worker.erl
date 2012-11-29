%% ================================================================
%% @author      Zachary Hueras
%% @version     0.0.1
%% @doc         Protocol handler for estatsd
%% ================================================================
-module(estatsd_udp_worker).
-export([start_link/1]).
-export([init/3]).

start_link(Mod, LSock) ->
    start_link(Mod, [], LSock).

start_link(Mod, Arg, LSock) ->
    proc_lib:start_link(?MODULE, init, [self(), Mod, Arg, LSock]).

init(Parent, Mod, Arg, LSock) ->
    case catch Mod:init(Arg) of
        ignore ->
            proc_lib:init_ack(Parent, ignore),
            exit(normal);
        {stop, Reason} ->
            proc_lib:init_ack(Parent, {error, Reason}),
            exit(Reason);
        {'EXIT', Reason} ->
            proc_lib:init_ack(Parent, {error, Reason}),
            exit(Reason);
        {ok, State} ->
            proc_lib:init_ack(Parent, {ok, self()}),
            loop(LSock, Mod, State);
        Else ->
            Error = {bad_return_value, Else},
            proc_lib:init_ack(Parent, {error, Error}),
            exit(Error)
    end.

loop(Mod, State, LSock) ->
    case catch gen_udp:recv(LSock, 0) of
        {ok, {_Address, _Port, Packet}} ->
            State2 = do_callback(Mod, handle_data, Packet, State),
            loop(Mod, State2, LSock);
        {error, Reason} ->
            Mod:terminate(Reason, State),
            exit(Reason)
    end.

do_callback(Module, Function, Arg, State) ->
    case catch Module:Function(Arg, State) of
        {ok, State} -> 
            State;
        {stop, Reason} ->
            Module:terminate(Reason, State),
            exit(Reason);
        {stop, Reason, State2} ->
            Module:terminate(Reason, State2),
            exit(Reason);
        {'EXIT', Reason} ->
            Module:terminate(Reason, State),
            exit(Reason)
    end.
