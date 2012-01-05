-module(cassanderl).

%% TODO: app should start supervisor which will spawn the workers

-export([get/4, get_slice/3, get_slice/4, get_count/3]).

-include_lib("cassandra_thrift/include/cassandra_types.hrl").
-define(LIMIT, 100).


%%====================================================================
%% API
%%====================================================================

get(Keyspace, ColumnFamily, Key, Column) ->
    %% TODO: figure out if we can send an empty list instead of column name
    ColumnPath = #columnPath{column_family=ColumnFamily, column=Column},

    run_query(get, Keyspace, [Key, ColumnPath, 1]).

get_slice(Keyspace, ColumnFamily, Key) ->
    get_slice(Keyspace, ColumnFamily, Key, ?LIMIT).

get_slice(Keyspace, ColumnFamily, Key, Limit) ->
    ColumnPath = #columnParent{column_family=ColumnFamily},
    Slice = #sliceRange{start="", finish="", reversed=false, count=Limit},
    SlicePredicate = #slicePredicate{slice_range=Slice},

    run_query(get_slice, Keyspace, [Key, ColumnPath, SlicePredicate, 1]).

get_count(Keyspace, ColumnFamily, Key) ->
    ColumnPath = #columnParent{column_family=ColumnFamily},
    Slice = #sliceRange{start="", finish="", reversed=false, count=2147483647},
    SlicePredicate = #slicePredicate{slice_range=Slice},

    run_query(get_count, Keyspace, [Key, ColumnPath, SlicePredicate, 1]).

%%====================================================================
%% Internal Functions
%%====================================================================
gen_server_stub(Request) ->
    {ok, GroupName} = application:get_env(cassanderl, pg2_group_name),
    Pid = pg2:get_closest_pid(GroupName),

    case is_pid(Pid) of
       true ->
         gen_server:call(Pid, Request);
       _ ->
         []
    end.

run_query(Function, Keyspace, Args) ->
    case gen_server_stub({Function, Keyspace, Args}) of
        {ok, {ok, R1}} ->
            [Key | _] = Args,
            {Key, parse_column(R1)};
        {exception, notFoundException} ->
            undefined
    end.

parse_column(Count) when is_integer(Count) ->
    Count;
parse_column(L) when is_list(L) ->
    lists:map(fun(I) -> [H | _ ] = parse_column(I), H end, L);
parse_column(L) ->
    [{L#columnOrSuperColumn.column#column.name, L#columnOrSuperColumn.column#column.value}].
