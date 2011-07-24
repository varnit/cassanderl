-module(cassanderl).

%% TODO: app should start supervisor which will spawn the workers

%% API
-export([get/3, get/4]).

-include_lib("cassandra_thrift/include/cassandra_types.hrl").

%%====================================================================
%% API
%%====================================================================

get(Keyspace, ColumnFamily, Key) ->
    get(Keyspace, ColumnFamily, Key, []).

get(Keyspace, ColumnFamily, Key, Column) ->
    %% TODO: figure out if we can send an empty list instead of column name
    ColumnPath = #columnPath{column_family=ColumnFamily, column=Column},

    case gen_server_stub({call, Keyspace, get, [Key, ColumnPath, 1]}) of
        {ok, {ok, R1}} ->
            {R1#columnOrSuperColumn.column#column.name, R1#columnOrSuperColumn.column#column.value};
        {exception, notFoundException} ->
            undefined
    end.

%% ------------------------------------------------------------------
%% Internal Functions
%% ------------------------------------------------------------------
gen_server_stub(Request) ->
    {ok, GroupName} = application:get_env(cassanderl, pg2_group_name),
    Pid = pg2:get_closest_pid(GroupName),

    case is_pid(Pid) of
       true ->
         gen_server:call(Pid, Request);
       _ ->
         []
    end.
