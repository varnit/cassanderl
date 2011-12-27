-module(cassanderl_worker).
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
    {ok, Hostname} = application:get_env(cassanderl, hostname),
    {ok, Port} = application:get_env(cassanderl, port),
    {ok, GroupName} = application:get_env(cassanderl, pg2_group_name),

    %% Open Connection
    {ok, Conn} = thrift_client_util:new(Hostname, Port, cassandra_thrift, [{framed, true}]), ok,

    %% Join pg2 group
    pg2:join(GroupName, self()),

    {ok, #state{conn = Conn}}.

handle_call({get, Keyspace, Args}, _From, State) ->
    thrift_call(get, Keyspace, Args, State);

handle_call({get_slice, Keyspace, Args}, _From, State) ->
    thrift_call(get_slice, Keyspace, Args, State);

handle_call({get_count, Keyspace, Args}, _From, State) ->
    thrift_call(get_count, Keyspace, Args, State);

handle_call(_Request, _From, State) ->
    {reply, unknown, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{conn=Conn}) ->
    thrift_client:close(Conn),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal Functions
%%====================================================================
set_keyspace(Keyspace, Conn) ->
    %% figure out what thrift_client:call really does?
    %% does it connect to the cluster or is it a local call.
    {Conn2, {ok, ok}} = thrift_client:call(Conn, set_keyspace, [Keyspace]),
    Conn2.

thrift_call(Call, Keyspace, Args, #state{conn=Conn}=State) ->
    Conn2 = set_keyspace(Keyspace, Conn),
    try thrift_client:call(Conn2, Call, Args) of
        {NewConn, Response} ->
            NewState = State#state{conn=NewConn},
            {reply, {ok, Response}, NewState}
    catch
        {NewConn, {exception, {Exception}}} ->
            NewState = State#state{conn=NewConn},
            {reply, {exception, Exception}, NewState}
    end.
