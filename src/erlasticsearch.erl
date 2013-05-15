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
%%%
-module(erlasticsearch).
-author('Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>').

-behaviour(gen_server).

-include("erlasticsearch.hrl").

%% API
-export([start/0, stop/0]).
-export([start_link/2]).
-export([start_client/1, start_client/2]).
-export([stop_client/1]).
-export([registered_name/1]).
-export([get_target/1]).

%% ElasticSearch
% Tests
-export([is_index/2]).
-export([is_type/3]).

% Cluster helpers
-export([health/1]).

% Index CRUD
-export([create_index/2, create_index/3]).
-export([delete_index/2]).
-export([open_index/2]).
-export([close_index/2]).

% Doc CRUD
-export([insert_doc/5, insert_doc/6]).
-export([get_doc/4, get_doc/5]).
-export([delete_doc/4, delete_doc/5]).
-export([search/4, search/5]).

%% Index helpers
-export([status/2]).
-export([refresh/1, refresh/2]).
-export([flush/1, flush/2]).
-export([optimize/1, optimize/2]).

-export([is_200/1, is_200_or_201/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(APP, ?MODULE).

-record(state, {
        client_name         :: client_name(),
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

%% @doc Name used to register the client process
-spec registered_name(client_name()) -> registered_name().
registered_name(ClientName) ->
    binary_to_atom(<<?REGISTERED_NAME_PREFIX, ClientName/binary, ".client">>, utf8).

%% @equiv start_client(ClientName, []).
-spec start_client(client_name()) -> supervisor:startchild_ret().
start_client(ClientName) when is_binary(ClientName) ->
    start_client(ClientName, []).

%% @doc Start a client process
-spec start_client(client_name(), params()) -> supervisor:startchild_ret().
start_client(ClientName, Options) when is_binary(ClientName),
                                       is_list(Options) ->
    erlasticsearch_sup:start_client(ClientName, Options).


%% @doc Stop a client process
-spec stop_client(client_name()) -> ok | error().
stop_client(ClientName) ->
    erlasticsearch_sup:stop_client(ClientName).

%% @doc Get the health the  ElasticSearch cluster
-spec health(server_ref()) -> response().
health(ServerRef) ->
    gen_server:call(get_target(ServerRef), {health}).

%% @doc Get the status of an index/indices in the  ElasticSearch cluster
-spec status(server_ref(), [index() | [index()]]) -> response().
status(ServerRef, Index) ->
    gen_server:call(get_target(ServerRef), {status, Index}).

%% @equiv create_index(ServerRef, Index, <<>>)
-spec create_index(server_ref(), index()) -> response().
create_index(ServerRef, Index) ->
    create_index(ServerRef, Index, <<>>).

%% @doc Create an index in the ElasticSearch cluster
-spec create_index(server_ref(), index(), doc()) -> response().
create_index(ServerRef, Index, Doc) ->
    gen_server:call(get_target(ServerRef), {create_index, Index, Doc}).

%% @doc Delete an index in the ElasticSearch cluster
-spec delete_index(server_ref(), index()) -> response().
delete_index(ServerRef, Index) -> 
    gen_server:call(get_target(ServerRef), {delete_index, Index}).

%% @doc Open an index in the ElasticSearch cluster
-spec open_index(server_ref(), index()) -> response().
open_index(ServerRef, Index) -> 
    gen_server:call(get_target(ServerRef), {open_index, Index}).

%% @doc Close an index in the ElasticSearch cluster
-spec close_index(server_ref(), index()) -> response().
close_index(ServerRef, Index) -> 
    gen_server:call(get_target(ServerRef), {close_index, Index}).

%% @doc Check if an index/indices exists in the ElasticSearch cluster
-spec is_index(server_ref(), [index() | [index()]]) -> boolean().
is_index(ServerRef, Index) -> 
    gen_server:call(get_target(ServerRef), {is_index, Index}).

%% @doc Check if a type exists in an index/indices in the ElasticSearch cluster
-spec is_type(server_ref(), [index() | [index()]], [type() | [type()]]) -> boolean().
is_type(ServerRef, Index, Type) -> 
    gen_server:call(get_target(ServerRef), {is_type, Index, Type}).

%% @equiv insert_doc(Index, Type, Id, Doc, []).
-spec insert_doc(server_ref(), index(), type(), id(), doc()) -> response().
insert_doc(ServerRef, Index, Type, Id, Doc) ->
    insert_doc(ServerRef, Index, Type, Id, Doc, []).

%% @doc Insert a doc into the ElasticSearch cluster
-spec insert_doc(server_ref(), index(), type(), id(), doc(), params()) -> response().
insert_doc(ServerRef, Index, Type, Id, Doc, Params) ->
    gen_server:call(get_target(ServerRef), {insert_doc, Index, Type, Id, Doc, Params}).

%% @equiv get_doc(ServerRef, Index, Type, Id, []).
-spec get_doc(server_ref(), index(), type(), id()) -> response().
get_doc(ServerRef, Index, Type, Id) ->
    get_doc(ServerRef, Index, Type, Id, []).

%% @doc Get a doc from the ElasticSearch cluster
-spec get_doc(server_ref(), index(), type(), id(), params()) -> response().
get_doc(ServerRef, Index, Type, Id, Params) ->
    gen_server:call(get_target(ServerRef), {get_doc, Index, Type, Id, Params}).

%% @equiv delete_doc(ServerRef, Index, Type, Id, []).
-spec delete_doc(server_ref(), index(), type(), id()) -> response().
delete_doc(ServerRef, Index, Type, Id) ->
    delete_doc(ServerRef, Index, Type, Id, []).
%% @doc Delete a doc from the ElasticSearch cluster
-spec delete_doc(server_ref(), index(), type(), id(), params()) -> response().
delete_doc(ServerRef, Index, Type, Id, Params) ->
    gen_server:call(get_target(ServerRef), {delete_doc, Index, Type, Id, Params}).

%% @equiv search(ServerRef, Index, Type, Doc, []).
-spec search(server_ref(), index(), type(), doc()) -> response().
search(ServerRef, Index, Type, Doc) ->
    search(ServerRef, Index, Type, Doc, []).
%% @doc Search for docs in the ElasticSearch cluster
-spec search(server_ref(), index(), type(), doc(), params()) -> response().
search(ServerRef, Index, Type, Doc, Params) ->
    gen_server:call(get_target(ServerRef), {search, Index, Type, Doc, Params}).

%% @doc Refresh all indices
%-spec refresh(server_ref()) -> response().
refresh(ServerRef) ->
    gen_server:call(get_target(ServerRef), {refresh}).

%% @doc Refresh one or more indices
%-spec refresh(server_ref(), [index() | [index()]]) -> response().
refresh(ServerRef, Index) ->
    gen_server:call(get_target(ServerRef), {refresh, Index}).

%% @doc Flush all indices
-spec flush(server_ref()) -> response().
flush(ServerRef) ->
    gen_server:call(get_target(ServerRef), {flush}).

%% @doc Flush one or more indices
-spec flush(server_ref(), [index() | [index()]]) -> response().
flush(ServerRef, Index) ->
    gen_server:call(get_target(ServerRef), {flush, Index}).

%% @doc Optimize all indices
-spec optimize(server_ref()) -> response().
optimize(ServerRef) ->
    gen_server:call(get_target(ServerRef), {optimize}).

%% @doc Optimize one or more indices
-spec optimize(server_ref(), [index() | [index()]]) -> response().
optimize(ServerRef, Index) ->
    gen_server:call(get_target(ServerRef), {optimize, Index}).



%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

start_link(ClientName, StartOptions) ->
    gen_server:start_link({local, registered_name(ClientName)}, ?MODULE, [ClientName, StartOptions], []).

init([ClientName, StartOptions]) ->
    Connection = connection(StartOptions),
    {ok, #state{client_name = ClientName, 
                connection = Connection}}.

handle_call({_Request = health}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_health(),
    {Connection1, RestResponse} = process_request(Connection0, RestRequest),
    {reply, RestResponse, State#state{connection = Connection1}};

handle_call({_Request = status, Index}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_status(Index),
    {Connection1, RestResponse} = process_request(Connection0, RestRequest),
    {reply, RestResponse, State#state{connection = Connection1}};

handle_call({_Request = create_index, Index, Doc}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_create_index(Index, Doc),
    {Connection1, RestResponse} = process_request(Connection0, RestRequest),
    {reply, RestResponse, State#state{connection = Connection1}};

handle_call({_Request = delete_index, Index}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_delete_index(Index),
    {Connection1, RestResponse} = process_request(Connection0, RestRequest),
    {reply, RestResponse, State#state{connection = Connection1}};

handle_call({_Request = open_index, Index}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_open_index(Index),
    {Connection1, RestResponse} = process_request(Connection0, RestRequest),
    {reply, RestResponse, State#state{connection = Connection1}};

handle_call({_Request = close_index, Index}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_close_index(Index),
    {Connection1, RestResponse} = process_request(Connection0, RestRequest),
    {reply, RestResponse, State#state{connection = Connection1}};

handle_call({_Request = is_index, Index}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_is_index(Index),
    {Connection1, RestResponse} = process_request(Connection0, RestRequest),
    % Check if the result is 200 (true) or 404 (false)
    Result = is_200(RestResponse),
    {reply, Result, State#state{connection = Connection1}};

handle_call({_Request = is_type, Index, Type}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_is_type(Index, Type),
    {Connection1, RestResponse} = process_request(Connection0, RestRequest),
    % Check if the result is 200 (true) or 404 (false)
    Result = is_200(RestResponse),
    {reply, Result, State#state{connection = Connection1}};

handle_call({_Request = insert_doc, Index, Type, Id, Doc, Params}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_insert_doc(Index, Type, Id, Doc, Params),
    {Connection1, RestResponse} = process_request(Connection0, RestRequest),
    {reply, RestResponse, State#state{connection = Connection1}};

handle_call({_Request = get_doc, Index, Type, Id, Params}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_get_doc(Index, Type, Id, Params),
    {Connection1, RestResponse} = process_request(Connection0, RestRequest),
    {reply, RestResponse, State#state{connection = Connection1}};

handle_call({_Request = delete_doc, Index, Type, Id, Params}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_delete_doc(Index, Type, Id, Params),
    {Connection1, RestResponse} = process_request(Connection0, RestRequest),
    {reply, RestResponse, State#state{connection = Connection1}};

handle_call({_Request = search, Index, Type, Doc, Params}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_search(Index, Type, Doc, Params),
    {Connection1, RestResponse} = process_request(Connection0, RestRequest),
    {reply, RestResponse, State#state{connection = Connection1}};

handle_call({_Request = refresh}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_refresh(),
    {Connection1, RestResponse} = process_request(Connection0, RestRequest),
    {reply, RestResponse, State#state{connection = Connection1}};

handle_call({_Request = refresh, Index}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_refresh(Index),
    {Connection1, RestResponse} = process_request(Connection0, RestRequest),
    {reply, RestResponse, State#state{connection = Connection1}};

handle_call({_Request = flush}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_flush(),
    {Connection1, RestResponse} = process_request(Connection0, RestRequest),
    {reply, RestResponse, State#state{connection = Connection1}};

handle_call({_Request = flush, Index}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_flush(Index),
    {Connection1, RestResponse} = process_request(Connection0, RestRequest),
    {reply, RestResponse, State#state{connection = Connection1}};

handle_call({_Request = optimize}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_optimize(),
    {Connection1, RestResponse} = process_request(Connection0, RestRequest),
    {reply, RestResponse, State#state{connection = Connection1}};

handle_call({_Request = optimize, Index}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_optimize(Index),
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
rest_request_health() ->
    #restRequest{method = ?elasticsearch_Method_GET,
                 uri = ?HEALTH}.

rest_request_status(Index) when is_binary(Index) ->
    Uri = bstr:join([Index, ?STATUS], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_GET,
                 uri = Uri};

rest_request_status(Index) when is_list(Index) ->
    IndexList = bstr:join(Index, <<",">>),
    Uri = bstr:join([IndexList, ?STATUS], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_GET,
                 uri = Uri}.

rest_request_create_index(Index, Doc) when is_binary(Index),
                                              is_binary(Doc) ->
    #restRequest{method = ?elasticsearch_Method_PUT,
                 uri = Index,
                 body = Doc}.

rest_request_delete_index(Index) when is_binary(Index) ->
    #restRequest{method = ?elasticsearch_Method_DELETE,
                 uri = Index}.

rest_request_open_index(Index) when is_binary(Index) ->
    Uri = bstr:join([Index, ?OPEN], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_POST,
                 uri = Uri}.

rest_request_close_index(Index) when is_binary(Index) ->
    Uri = bstr:join([Index, ?CLOSE], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_POST,
                 uri = Uri}.

rest_request_is_index(Index) when is_binary(Index) ->
    #restRequest{method = ?elasticsearch_Method_HEAD,
                 uri = Index};

rest_request_is_index(Index) when is_list(Index) ->
    IndexList = bstr:join(Index, <<",">>),
    #restRequest{method = ?elasticsearch_Method_HEAD,
                 uri = IndexList}.

rest_request_is_type(Index, Type) when is_binary(Index),
                                        is_binary(Type) ->
    Uri = bstr:join([Index, Type], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_HEAD,
                 uri = Uri};


rest_request_is_type(Index, Type) when is_list(Index),
                                        is_binary(Type) ->
    IndexList = bstr:join(Index, <<",">>),
    rest_request_is_type(IndexList, Type);

rest_request_is_type(Index, Type) when is_binary(Index),
                                        is_list(Type) ->
    TypeList = bstr:join(Type, <<",">>),
    rest_request_is_type(Index, TypeList);

rest_request_is_type(Index, Type) when is_list(Index),
                                        is_list(Type) ->
    IndexList = bstr:join(Index, <<",">>),
    TypeList = bstr:join(Type, <<",">>),
    rest_request_is_type(IndexList, TypeList).

rest_request_insert_doc(Index, Type, undefined, Doc, Params) when is_binary(Index),
                                                      is_binary(Type),
                                                      is_binary(Doc),
                                                      is_list(Params) ->
    Uri = make_uri([Index, Type], Params),
    #restRequest{method = ?elasticsearch_Method_POST,
                 uri = Uri,
                 body = Doc};

rest_request_insert_doc(Index, Type, Id, Doc, Params) when is_binary(Index),
                                                      is_binary(Type),
                                                      is_binary(Id),
                                                      is_binary(Doc),
                                                      is_list(Params) ->
    Uri = make_uri([Index, Type, Id], Params),
    #restRequest{method = ?elasticsearch_Method_PUT,
                 uri = Uri,
                 body = Doc}.

rest_request_get_doc(Index, Type, Id, Params) when is_binary(Index),
                                                   is_binary(Type),
                                                   is_binary(Id),
                                                   is_list(Params) ->
    Uri = make_uri([Index, Type, Id], Params),
    #restRequest{method = ?elasticsearch_Method_GET,
                 uri = Uri}.

rest_request_delete_doc(Index, Type, Id, Params) when is_binary(Index),
                                                   is_binary(Type),
                                                   is_binary(Id),
                                                   is_list(Params) ->
    Uri = make_uri([Index, Type, Id], Params),
    #restRequest{method = ?elasticsearch_Method_DELETE,
                 uri = Uri}.

rest_request_search(Index, Type, Doc, Params) when is_binary(Index),
                                                   is_binary(Type),
                                                   is_binary(Doc),
                                                   is_list(Params) ->
    Uri = make_uri([Index, Type, ?SEARCH], Params),
    #restRequest{method = ?elasticsearch_Method_GET,
                 uri = Uri,
                 body = Doc}.

rest_request_refresh() ->
    #restRequest{method = ?elasticsearch_Method_POST,
                 uri = ?REFRESH}.

rest_request_refresh(Index) when is_binary(Index) ->
    Uri = bstr:join([Index, ?REFRESH], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_POST,
                 uri = Uri};

rest_request_refresh(Index) when is_list(Index) ->
    IndexList = bstr:join(Index, <<",">>),
    Uri = bstr:join([IndexList, ?REFRESH], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_POST,
                 uri = Uri}.

rest_request_flush() ->
    #restRequest{method = ?elasticsearch_Method_POST,
                 uri = ?FLUSH}.

rest_request_flush(Index) when is_binary(Index) ->
    Uri = bstr:join([Index, ?FLUSH], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_POST,
                 uri = Uri};

rest_request_flush(Index) when is_list(Index) ->
    IndexList = bstr:join(Index, <<",">>),
    Uri = bstr:join([IndexList, ?FLUSH], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_POST,
                 uri = Uri}.

rest_request_optimize() ->
    #restRequest{method = ?elasticsearch_Method_POST,
                 uri = ?OPTIMIZE}.

rest_request_optimize(Index) when is_binary(Index) ->
    Uri = bstr:join([Index, ?OPTIMIZE], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_POST,
                 uri = Uri};

rest_request_optimize(Index) when is_list(Index) ->
    IndexList = bstr:join(Index, <<",">>),
    Uri = bstr:join([IndexList, ?OPTIMIZE], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_POST,
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


%% @doc Get the target for the server_ref
-spec get_target(server_ref()) -> target().
get_target(ServerRef) when is_pid(ServerRef) ->
    ServerRef;
get_target(ServerRef) when is_atom(ServerRef) ->
    ServerRef;
get_target(ClientName) when is_binary(ClientName) ->
    whereis(registered_name(ClientName)).
