%%%-------------------------------------------------------------------
%%% @author Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>
%%% @copyright (C) 2013 Mahesh Paolini-Subramanya
%%% @doc type definitions and records.
%%% @end
%%%
%%% This source file is subject to the New BSD License. You should have received
%%% a copy of the New BSD license with this software. If not, it can be
%%% retrieved from: http://www.opensource.org/licenses/bsd-license.php
%%%-------------------------------------------------------------------
-module(erlasticsearch).
-author('Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>').

-behaviour(gen_server).

-include("erlasticsearch.hrl").

%% API
-export([start/0, stop/0]).
-export([start_link/0, start_link/1]).

%% ElasticSearch
% Tests
-export([is_index/1]).

% Status
-export([health/0]).

% CRUD
-export([create_index/1, create_index/2]).
-export([delete_index/1]).
-export([insert_doc/4, insert_doc/5]).
-export([get_doc/3, get_doc/4]).
-export([delete_doc/3, delete_doc/4]).

-export([is_200/1, is_200_or_201/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(APP, ?MODULE).

-record(state, {
        connection          :: connection()
        }).


%% ------------------------------------------------------------------
%% API
%% ------------------------------------------------------------------
%% @doc Start the application and all its dependencies.
-spec start() -> ok.
start() ->
    d_util:start_deps(?APP).

%% @doc Stop the application and all its dependencies.
-spec stop() -> ok.
stop() ->
    d_util:stop_deps(?APP).

%% @doc Get the health of the ElasticSearch cluster
-spec health() -> response().
health() ->
    gen_server:call(?MODULE, {health}).

%% @equiv create_index(Index, <<>>)
-spec create_index(index()) -> response().
create_index(Index) ->
    create_index(Index, <<>>).

%% @doc Create an index in the the ElasticSearch cluster
-spec create_index(index(), doc()) -> response().
create_index(Index, Doc) ->
    gen_server:call(?MODULE, {create_index, Index, Doc}).

%% @doc Delete an index in the the ElasticSearch cluster
-spec delete_index(index()) -> response().
delete_index(Index) -> 
    gen_server:call(?MODULE, {delete_index, Index}).

%% @doc Check if an index exists in the the ElasticSearch cluster
-spec is_index(index()) -> boolean().
is_index(Index) -> 
    gen_server:call(?MODULE, {is_index, Index}).

%% @equiv insert_doc(Index, Type, Id, Doc, []).
-spec insert_doc(index(), type(), id(), doc()) -> response().
insert_doc(Index, Type, Id, Doc) ->
    insert_doc(Index, Type, Id, Doc, []).

%% @doc Insert a doc into the the ElasticSearch cluster
-spec insert_doc(index(), type(), id(), doc(), params()) -> response().
insert_doc(Index, Type, Id, Doc, Params) ->
    gen_server:call(?MODULE, {insert_doc, Index, Type, Id, Doc, Params}).

%% @equiv get_doc(Index, Type, Id, []).
-spec get_doc(index(), type(), id()) -> response().
get_doc(Index, Type, Id) ->
    get_doc(Index, Type, Id, []).

%% @doc Get a doc from the the ElasticSearch cluster
-spec get_doc(index(), type(), id(), params()) -> response().
get_doc(Index, Type, Id, Params) ->
    gen_server:call(?MODULE, {get_doc, Index, Type, Id, Params}).

%% @equiv delete_doc(Index, Type, Id, []).
-spec delete_doc(index(), type(), id()) -> response().
delete_doc(Index, Type, Id) ->
    delete_doc(Index, Type, Id, []).
%% @doc Delete a doc from the the ElasticSearch cluster
-spec delete_doc(index(), type(), id(), params()) -> response().
delete_doc(Index, Type, Id, Params) ->
    gen_server:call(?MODULE, {delete_doc, Index, Type, Id, Params}).


%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    StartOptions = get_env(thrift_options, ?DEFAULT_THRIFT_OPTIONS),
    start_link(StartOptions).

start_link(StartOptions) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [StartOptions], []).

init([StartOptions]) ->
    Connection = connection(StartOptions),
    {ok, #state{connection = Connection}}.

handle_call({Request = health}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request(Request, undefined),
    {Connection1, RestResponse} = process_request(Connection0, RestRequest),
    {reply, RestResponse, State#state{connection = Connection1}};

handle_call({Request = create_index, Index, Doc}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request(Request, {Index, Doc}),
    {Connection1, RestResponse} = process_request(Connection0, RestRequest),
    {reply, RestResponse, State#state{connection = Connection1}};

handle_call({Request = delete_index, Index}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request(Request, {Index}),
    {Connection1, RestResponse} = process_request(Connection0, RestRequest),
    {reply, RestResponse, State#state{connection = Connection1}};

handle_call({Request = is_index, Index}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request(Request, {Index}),
    {Connection1, RestResponse} = process_request(Connection0, RestRequest),
    % Check if the result is 200 (true) or 404 (false)
    Result = is_200(RestResponse),
    {reply, Result, State#state{connection = Connection1}};

handle_call({Request = insert_doc, Index, Type, Id, Doc, Params}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request(Request, {Index, Type, Id, Doc, Params}),
    {Connection1, RestResponse} = process_request(Connection0, RestRequest),
    {reply, RestResponse, State#state{connection = Connection1}};

handle_call({Request = get_doc, Index, Type, Id, Params}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request(Request, {Index, Type, Id, Params}),
    {Connection1, RestResponse} = process_request(Connection0, RestRequest),
    {reply, RestResponse, State#state{connection = Connection1}};

handle_call({Request = delete_doc, Index, Type, Id, Params}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request(Request, {Index, Type, Id, Params}),
    {Connection1, RestResponse} = process_request(Connection0, RestRequest),
    {reply, RestResponse, State#state{connection = Connection1}};

handle_call(_Request, _From, State) ->
    {stop, unhandled_call, State}.

handle_cast(_Request, State) ->
    {stop, unhandled_info, State}.

handle_info(_Info, State) ->
    {stop, unhandled_info, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.





%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
%% @doc Build a new connection
-spec connection(params()) -> connection().
connection(StartOptions) ->
    ThriftHost = get_env(thrift_host, ?DEFAULT_THRIFT_HOST),
    ThriftPort = get_env(thrift_port, ?DEFAULT_THRIFT_PORT),
    {ok, Connection} = thrift_client_util:new(ThriftHost, ThriftPort, elasticsearch_thrift, StartOptions),
    Connection.

%% @doc Process the request over thrift
-spec process_request(connection(), request()) -> {connection(), response()}.
process_request(Connection, Request) ->
    thrift_client:call(Connection, 'execute', [Request]).


%% @doc Build a new rest request
-spec rest_request(method(), any()) -> request().
rest_request(health, _) ->
    #restRequest{method = ?elasticsearch_Method_GET,
                 uri = ?HEALTH};

rest_request(create_index, {Index, Doc}) when is_binary(Index),
                                              is_binary(Doc) ->
    Uri = bstr:join([Index], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_PUT,
                 uri = Uri,
                 body = Doc};

rest_request(delete_index, {Index}) when is_binary(Index) ->
    Uri = bstr:join([Index], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_DELETE,
                 uri = Uri};

rest_request(is_index, {Index}) when is_binary(Index) ->
    Uri = bstr:join([Index], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_HEAD,
                 uri = Uri};

rest_request(insert_doc, {Index, Type, undefined, Doc, Params}) when is_binary(Index),
                                                      is_binary(Type),
                                                      is_binary(Doc),
                                                      is_list(Params) ->
    Uri = make_uri([Index, Type], Params),
    #restRequest{method = ?elasticsearch_Method_POST,
                 uri = Uri,
                 body = Doc};

rest_request(insert_doc, {Index, Type, Id, Doc, Params}) when is_binary(Index),
                                                      is_binary(Type),
                                                      is_binary(Id),
                                                      is_binary(Doc),
                                                      is_list(Params) ->
    Uri = make_uri([Index, Type, Id], Params),
    #restRequest{method = ?elasticsearch_Method_PUT,
                 uri = Uri,
                 body = Doc};

rest_request(get_doc, {Index, Type, Id, Params}) when is_binary(Index),
                                                   is_binary(Type),
                                                   is_binary(Id),
                                                   is_list(Params) ->
    Uri = make_uri([Index, Type, Id], Params),
    #restRequest{method = ?elasticsearch_Method_GET,
                 uri = Uri};

rest_request(delete_doc, {Index, Type, Id, Params}) when is_binary(Index),
                                                   is_binary(Type),
                                                   is_binary(Id),
                                                   is_list(Params) ->
    Uri = make_uri([Index, Type, Id], Params),
    #restRequest{method = ?elasticsearch_Method_DELETE,
                 uri = Uri}.

%% @doc Make a complete URI based on the tokens and props
make_uri(BaseList, PropList) ->
    Base = bstr:join(BaseList, <<"/">>),
    case PropList of
        [] ->
            Base;
        PropList ->
            Props = d_uri:uri_params_encode(PropList),
            bstr:join([Base, Props], <<"?">>)
    end.

%% @doc The official way to get a value from this application's env.
%%      Will return Default if that key is unset.
-spec get_env(Key :: atom(), Default :: term()) -> term().
get_env(Key, Default) ->
    case application:get_env(?APP, Key) of
        {ok, Value} ->
            Value;
        _ ->
            Default
    end.
-spec is_200(response()) -> boolean().
is_200({ok, Response}) ->
    case Response#restResponse.status of
        200 -> true;
        _ -> false
    end.

-spec is_200_or_201(response()) -> boolean().
is_200_or_201({ok, Response}) ->
    case Response#restResponse.status of
        200 -> true;
        201 -> true;
        _ -> false
    end.
