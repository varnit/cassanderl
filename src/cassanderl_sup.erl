-module(cassanderl_sup).
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
    {ok, Size} = application:get_env(cassanderl, worker_pool_size),
    {ok, GroupName} = application:get_env(cassanderl, pg2_group_name),

    pg2:create(GroupName),

    Workers = [worker_spec(I) || I <- lists:seq(1, Size)],
    {ok, {{one_for_one, 10, 1}, Workers}}.

%% ===================================================================
%% Internal API
%% ===================================================================

worker_spec(N) ->
    Name = list_to_atom("cassanderl_" ++ integer_to_list(N)),

    {Name,
        {cassanderl_worker, start_link, [Name]},
        permanent, 1000, worker,
        [cassanderl]
    }.