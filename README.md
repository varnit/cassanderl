## Getting Started

```bash
$ rebar get-deps
$ rebar compile
```
## Cassanderl version 0.2 ##

```erlang
-include("cassandra_thrift/include/cassandra_types.hrl").

ColumnPath = #columnPath{column_family="emails", column="username"},

Username =
    case cassanderl_sup:call(get, ["example@example.com", ColumnPath, 1]) of
        {ok, {ok, R1}} ->
            R1#columnOrSuperColumn.column#column.value;
        {exception, notFoundException} ->
            undefined
    end.
```
