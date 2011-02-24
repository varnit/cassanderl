-module(cassanderl_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    cassanderl_deps:ensure(),
    cassanderl:start_link().

stop(_State) ->
    ok.
