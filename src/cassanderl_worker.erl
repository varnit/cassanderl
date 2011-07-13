-module(cassanderl_worker).
-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {conn}).
-define(SERVER, ?MODULE).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([Name]) ->
    {ok, Hostname} = application:get_env(cassanderl, hostname),
    {ok, Port} = application:get_env(cassanderl, port),
    {ok, GroupName} = application:get_env(cassanderl, pg2_group_name),

    %% Open Connection
    {ok, Conn} = thrift_client_util:new(Hostname, Port, cassandra_thrift, [{framed, true}]), ok,

    %% Join pg2 group
    pg2:join(GroupName, self())

    {ok, #state{conn = Conn}}.

handle_call({call, Function, Args}, _From, #state{conn=Conn}=State) ->
    %% Set KeySpace
    {Conn2, {ok, ok}} = thrift_client:call(Conn, set_keyspace, [Keyspace]),

    try thrift_client:call(Conn2, Function, Args) of
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
