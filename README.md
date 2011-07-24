## Warning
Alpha version.

## Compile

```bash
$ rebar get-deps
$ rebar compile
```
## Run

```erlang
$ erl -pa ebin deps/*/ebin -boot start_sasl -sname cassanderl -eval 'application:start(cassanderl).'

(cassanderl@localhost)1> cassanderl:get(<<"MyKeyspace">>, <<"MyCF">>, <<"Key">>, <<"Column">>).
```
