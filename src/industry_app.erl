%%%-------------------------------------------------------------------
%%% @author Martin Kristiansen <msk@ajour.io>
%%% @copyright (C) 2015, Martin Kristiansen
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(industry_app).

-behaviour(application).

%% Application callbacks
-export([start/0, start/2, stop/1]).

%%%===================================================================
%%% Application callbacks
%%%===================================================================

%% @spec start(StartType, StartArgs) -> {ok, Pid} |
%%                                      {ok, Pid, State} |
%%                                      {error, Reason}
%%      StartType = normal | {takeover, Node} | {failover, Node}
%%      StartArgs = term()
start() ->  application:start(industry).

start(_StartType, _StartArgs) ->
    lager:warning("Starting App"),

    ok = application:start(druuid),
    ok = application:start(rafter),
    
    case industry_sup:start_link() of
	{ok, Pid} -> {ok, Pid};
	Error -> Error
    end.

%% @spec stop(State) -> void()
stop(_State) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================
