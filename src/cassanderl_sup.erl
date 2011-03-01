-module(cassanderl_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Internal API
-export([call/2]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, Options} = application:get_env(cassanderl, options),

    PoolOptions = [{name,
                   {local, cassandra}},
                   {worker_module, cassanderl_worker}
                   | Options],

    PoolSpecs = [{cassandra,
                 {poolboy, start_link, [PoolOptions]},
                 permanent,
                 5000,
                 worker,
                 [poolboy]}],

    {ok, {{one_for_all, 10, 10}, PoolSpecs}}.

%% ===================================================================
%% Internal API
%% ===================================================================

call(Function, Args) ->
    Worker = poolboy:checkout(cassandra),
    Reply = gen_server:call(Worker, {call, Function, Args}),
    poolboy:checkin(cassandra, Worker),
    Reply.

