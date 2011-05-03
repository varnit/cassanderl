-module(cassanderl).

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {conn}).
-define(SERVER, ?MODULE).

%%====================================================================
%% API
%%====================================================================

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Open Connection
    {ok, Hostname} = application:get_env(cassanderl, hostname),
    {ok, Port} = application:get_env(cassanderl, port),
    {ok, Conn} = thrift_client_util:new(Hostname, Port, cassandra_thrift, [{framed, true}]), ok,

    %% Set KeySpace
    {ok, KeySpace} = application:get_env(keyspace),
    {Conn2, {ok, ok}} = thrift_client:call(Conn, set_keyspace, [KeySpace]),
    {ok, #state{conn = Conn2}}.

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

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{conn=Conn}) ->
    thrift_client:close(Conn),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.