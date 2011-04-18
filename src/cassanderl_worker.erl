-module(cassanderl_worker).

-behaviour(gen_server).

-export([start_link/1, stop/0, init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-record(state, {conn}).

start_link(Args) -> gen_server:start_link(?MODULE, Args, []).
stop() -> gen_server:cast(?MODULE, stop).

init(Args) ->
    process_flag(trap_exit, true),
    Hostname = proplists:get_value(hostname, Args),
    Port = proplists:get_value(port, Args),
    {ok, Conn} = thrift_client_util:new(Hostname, Port, cassandra_thrift, [{framed, true}]), ok,
    {ok, #state{conn=Conn}}.

handle_call({call, Function, Args}, _From, #state{conn=Conn}=State) ->
    try thrift_client:call(Conn, Function, Args) of
        {NewConn, Response} ->
            NewState = State#state{conn=NewConn},
            {reply, {ok, Response}, NewState}
    catch
        {NewConn, {exception, {Exception}}} ->
            NewState = State#state{conn=NewConn},
            {reply, {exception, Exception}, NewState}
    end;
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(stop, State) ->
    {stop, shutdown, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', _, _}, State) ->
    {stop, shutdown, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{conn=Conn}) ->
    thrift_client:close(Conn),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.