-module(erlasticsearch_worker).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([is_200/1]).
-export([is_200_or_201/1]).

-include("erlasticsearch.hrl").

-record(state, {
        binary_response = false :: boolean(),
        connection              :: connection(),
        connection_options = [] :: params(),
        pool_name               :: pool_name(),
        retries_left = 1        :: non_neg_integer(),
        retry_interval = 0      :: non_neg_integer()}).

-type state() :: #state{}.

start_link(ConnectionOptions) ->
    gen_server:start_link(?MODULE, [?DEFAULT_POOL_NAME, ConnectionOptions], []).

init([PoolName, Options0]) ->
    process_flag(trap_exit, true),
    {DecodeResponse, ConnectionOptions} =
        case lists:keytake(binary_response, 1, Options0) of
            {value, {binary_response, Decode}, Options1} -> {Decode, Options1};
            false -> {true, Options0}
        end,
    {RetryInterval, ConnectionOptions1} =
        case lists:keytake(retry_interval, 1, ConnectionOptions) of
            {value, {retry_interval, Interval}, Options2} ->
                {Interval, Options2};
            false -> {0, ConnectionOptions}
        end,
    {RetryAmount, ConnectionOptions2} =
        case lists:keytake(retry_amount, 1, ConnectionOptions1) of
            {value, {retry_amount, Amount}, Options3} ->
                {Amount, Options3};
            false -> {1, ConnectionOptions1}
        end,
    Connection = connection(ConnectionOptions2),
    {ok, #state{pool_name = PoolName,
                binary_response = DecodeResponse,
                connection_options = ConnectionOptions,
                connection = Connection,
                retries_left = RetryAmount,
                retry_interval = RetryInterval}}.

handle_call({stop}, _From, State) ->
    thrift_client:close(State#state.connection),
    {stop, normal, ok, State};

handle_call({_Request = health}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_health(),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = state, Params}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_state(Params),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = nodes_info, NodeNames, Params}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_nodes_info(NodeNames, Params),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = nodes_stats, NodeNames, Params}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_nodes_stats(NodeNames, Params),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = status, Index}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_status(Index),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = indices_stats, Index}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_indices_stats(Index),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = create_index, Index, Doc}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_create_index(Index, Doc),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = delete_index, Index}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_delete_index(Index),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = open_index, Index}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_open_index(Index),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = close_index, Index}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_close_index(Index),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = count, Index, Type, Doc, Params}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_count(Index, Type, Doc, Params),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = delete_by_query, Index, Type, Doc, Params}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_delete_by_query(Index, Type, Doc, Params),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = is_index, Index}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_is_index(Index),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    Result = make_boolean_response(Response, State),
    {reply, Result, State#state{connection = Connection1}};

handle_call({_Request = is_type, Index, Type}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_is_type(Index, Type),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    Result = make_boolean_response(Response, State),

    {reply, Result, State#state{connection = Connection1}};

handle_call({_Request = insert_doc, Index, Type, Id, Doc, Params}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_insert_doc(Index, Type, Id, Doc, Params),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = update_doc, Index, Type, Id, Doc, Params}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_update_doc(Index, Type, Id, Doc, Params),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = get_doc, Index, Type, Id, Params}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_get_doc(Index, Type, Id, Params),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = mget_doc, Index, Type, Doc}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_mget_doc(Index, Type, Doc),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = is_doc, Index, Type, Id}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_is_doc(Index, Type, Id),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    Result = make_boolean_response(Response, State),
    {reply, Result, State#state{connection = Connection1}};

handle_call({_Request = delete_doc, Index, Type, Id, Params}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_delete_doc(Index, Type, Id, Params),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = search, Index, Type, Doc, Params}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_search(Index, Type, Doc, Params),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = bulk, Index, Type, Doc}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_bulk(Index, Type, Doc),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = refresh, Index}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_refresh(Index),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = flush, Index}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_flush(Index),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = optimize, Index}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_optimize(Index),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = segments, Index}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_segments(Index),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = clear_cache, Index, Params}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_clear_cache(Index, Params),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = put_mapping, Indexes, Type, Doc}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_put_mapping(Indexes, Type, Doc),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = get_mapping, Indexes, Type}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_get_mapping(Indexes, Type),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = delete_mapping, Indexes, Type}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_delete_mapping(Indexes, Type),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = aliases, Doc}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_aliases(Doc),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = insert_alias, Index, Alias}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_insert_alias(Index, Alias),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = insert_alias, Index, Alias, Doc}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_insert_alias(Index, Alias, Doc),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = delete_alias, Index, Alias}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_delete_alias(Index, Alias),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call({_Request = is_alias, Index, Alias}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_is_alias(Index, Alias),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    Result = make_boolean_response(Response, State),
    {reply, Result, State#state{connection = Connection1}};

handle_call({_Request = get_alias, Index, Alias}, _From, State = #state{connection = Connection0}) ->
    RestRequest = rest_request_get_alias(Index, Alias),
    {Connection1, Response} = process_request(Connection0, RestRequest, State),
    {reply, Response, State#state{connection = Connection1}};

handle_call(_Request, _From, State) ->
    thrift_client:close(State#state.connection),
    {stop, unhandled_call, State}.

handle_cast(_Request, State) ->
    thrift_client:close(State#state.connection),
    {stop, unhandled_info, State}.

handle_info(_Info, State) ->
    thrift_client:close(State#state.connection),
    {stop, unhandled_info, State}.

terminate(_Reason, State) ->
    thrift_client:close(State#state.connection),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec connection(params()) -> connection().
connection(ConnectionOptions) ->
    ThriftHost = proplists:get_value(thrift_host, ConnectionOptions, ?DEFAULT_THRIFT_HOST),
    ThriftPort = proplists:get_value(thrift_port, ConnectionOptions, ?DEFAULT_THRIFT_PORT),
    ThriftOptions = case lists:keyfind(thrift_options, 1, ConnectionOptions) of
        {thrift_options, Options} -> Options;
        false -> []
    end,
    {ok, Connection} = thrift_client_util:new(ThriftHost, ThriftPort, elasticsearch_thrift, ThriftOptions),
    Connection.

-spec process_request(connection(), rest_request(), #state{}) -> {connection(), response()}.
process_request(undefined, Request, State = #state{connection_options = ConnectionOptions}) ->
    Connection = connection(ConnectionOptions),
    process_request(Connection, Request, State);
process_request(Connection, Request, State = #state{binary_response = BinaryResponse}) ->
    case do_request(Connection, {'execute', [Request]}, State) of
        {error, closed, NewState} ->
            error_or_retry({error, closed}, Connection, Request, NewState);
        {error, econnrefused, NewState} ->
            error_or_retry({error, econnrefused}, Connection, Request, NewState);
        {Connection1, RestResponse} ->
            {Connection1, process_response(BinaryResponse, RestResponse)}
    end.

-spec increase_reconnect_interval(state()) -> state().
increase_reconnect_interval(#state{retry_interval = Interval} = State) ->
    if Interval < ?MAX_RECONNECT_INTERVAL ->
            NewInterval = min(Interval + Interval, ?MAX_RECONNECT_INTERVAL),
            State#state{retry_interval = NewInterval};
       true ->
            State
    end.

-spec update_reconnect_state(state()) -> state().
update_reconnect_state(State) ->
    State1 = increase_reconnect_interval(State),
    State2 = decrease_retries_left(State1),
    State2.

-spec decrease_retries_left(state()) -> state().
decrease_retries_left(#state{retries_left = N} = State) ->
    State#state{retries_left = N - 1}.

-spec error_or_retry({error, atom()}, connection(), rest_request(), state()) ->
                            {error, atom()} | {connection(), response()}.
error_or_retry({error, Reason}, Connection,
               Request, #state{retries_left = N, retry_interval = W} = State)
  when N > 0 andalso W >= 0
       andalso (Reason =:= closed orelse Reason =:= econnrefused) ->
    timer:sleep(W),
    ShorterRetryState = update_reconnect_state(State),
    thrift_client:close(Connection),
    process_request(undefined, Request, ShorterRetryState);
error_or_retry(Error, _Connection, _Request, _State) ->
    Error.

-spec do_request(connection(), {'execute', [rest_request()]}, #state{}) ->
                        {connection(),  {ok, rest_response()} | error()}
                            | {error, closed, state()}
                            | {error, econnrefused, state()}.
do_request(Connection, {Function, Args}, State) ->
    Args2 =
        case Args of
            [#restRequest{body=Body}] when is_binary(Body) ->
                Args;
            [A=#restRequest{body=Body}] when is_list(Body) ->
                [A#restRequest{body=jsx:encode(Body, [repeat_keys])}]
        end,
    try thrift_client:call(Connection, Function, Args2) of
        {Connection1, Response = {ok, _}} ->
            {Connection1, Response};
        {Connection1, Response = {error, _}} ->
            {Connection1, Response}
    catch
        throw:{Connection1, Response = {exception, _}} ->
            {Connection1, Response};
        error:badarg ->
            {Connection, {error, badarg}};
        error:{case_clause, {error, closed}} ->
            {error, closed, State};
        error:{case_clause, {error, econnrefused}} ->
            {error, econnrefused, State}
    end.

-spec process_response(boolean(), {ok, rest_response()} | error() | exception()) -> response().
process_response(_, {error, _} = Response) ->
    Response;
process_response(true, {ok, #restResponse{status = Status, body = undefined}}) ->
    [{status, erlang:integer_to_binary(Status)}];
process_response(false, {ok, #restResponse{status = Status, body = undefined}}) ->
    [{status, Status}];
process_response(true, {ok, #restResponse{status = Status, body = Body}}) ->
    [{status, erlang:integer_to_binary(Status)}, {body, Body}];
process_response(false, {ok, #restResponse{status = Status, body = Body}}) ->
    try
        [{status, Status}, {body, jsx:decode(Body)}]
    catch
        error:badarg ->
            [{status, Status}, {body, Body}]
    end.

rest_request_health() ->
    #restRequest{method = ?elasticsearch_Method_GET,
                 uri = ?HEALTH}.

rest_request_state(Params) when is_list(Params) ->
    Uri = make_uri([?STATE], Params),
    #restRequest{method = ?elasticsearch_Method_GET,
                 uri = Uri}.

rest_request_nodes_info(NodeNames, Params) when is_list(NodeNames),
                                                is_list(Params) ->
    NodeNameList = erlasticsearch:join(NodeNames, <<",">>),
    Uri = make_uri([?NODES, NodeNameList], Params),
    #restRequest{method = ?elasticsearch_Method_GET,
                 uri = Uri}.

rest_request_nodes_stats(NodeNames, Params) when is_list(NodeNames),
                                                is_list(Params) ->
    NodeNameList = erlasticsearch:join(NodeNames, <<",">>),
    Uri = make_uri([?NODES, NodeNameList, ?STATS], Params),
    #restRequest{method = ?elasticsearch_Method_GET,
                 uri = Uri}.

rest_request_status(Index) when is_list(Index) ->
    IndexList = erlasticsearch:join(Index, <<",">>),
    Uri = erlasticsearch:join([IndexList, ?STATUS], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_GET,
                 uri = Uri}.

rest_request_indices_stats(Index) when is_list(Index) ->
    IndexList = erlasticsearch:join(Index, <<",">>),
    Uri = erlasticsearch:join([IndexList, ?INDICES_STATS], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_GET,
                 uri = Uri}.

rest_request_create_index(Index, Doc) when is_binary(Index) andalso
                                              (is_binary(Doc) orelse is_list(Doc)) ->
    #restRequest{method = ?elasticsearch_Method_PUT,
                 uri = Index,
                 body = Doc}.

rest_request_delete_index(Index) when is_list(Index) ->
    IndexList = erlasticsearch:join(Index, <<",">>),
    #restRequest{method = ?elasticsearch_Method_DELETE,
                 uri = IndexList}.

rest_request_open_index(Index) when is_binary(Index) ->
    Uri = erlasticsearch:join([Index, ?OPEN], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_POST,
                 uri = Uri}.

rest_request_close_index(Index) when is_binary(Index) ->
    Uri = erlasticsearch:join([Index, ?CLOSE], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_POST,
                 uri = Uri}.

rest_request_count(Index, Type, Doc, Params) when is_list(Index) andalso
                                        is_list(Type) andalso
                                        (is_binary(Doc) orelse is_list(Doc)) andalso
                                        is_list(Params) ->
    IndexList = erlasticsearch:join(Index, <<",">>),
    TypeList = erlasticsearch:join(Type, <<",">>),
    Uri = make_uri([IndexList, TypeList, ?COUNT], Params),
    #restRequest{method = ?elasticsearch_Method_GET,
                 uri = Uri,
                 body = Doc}.

rest_request_delete_by_query(Index, Type, Doc, Params) when is_list(Index) andalso
                                        is_list(Type) andalso
                                        (is_binary(Doc) orelse is_list(Doc)) andalso
                                        is_list(Params) ->
    IndexList = erlasticsearch:join(Index, <<",">>),
    TypeList = erlasticsearch:join(Type, <<",">>),
    Uri = make_uri([IndexList, TypeList, ?QUERY], Params),
    #restRequest{method = ?elasticsearch_Method_DELETE,
                 uri = Uri,
                 body = Doc}.

rest_request_is_index(Index) when is_list(Index) ->
    IndexList = erlasticsearch:join(Index, <<",">>),
    #restRequest{method = ?elasticsearch_Method_HEAD,
                 uri = IndexList}.

rest_request_is_type(Index, Type) when is_list(Index),
                                        is_list(Type) ->
    IndexList = erlasticsearch:join(Index, <<",">>),
    TypeList = erlasticsearch:join(Type, <<",">>),
    Uri = erlasticsearch:join([IndexList, TypeList], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_HEAD,
                 uri = Uri}.

rest_request_insert_doc(Index, Type, undefined, Doc, Params) when is_binary(Index) andalso
                                                      is_binary(Type) andalso
                                                      (is_binary(Doc) orelse is_list(Doc)) andalso
                                                      is_list(Params) ->
    Uri = make_uri([Index, Type], Params),
    #restRequest{method = ?elasticsearch_Method_POST,
                 uri = Uri,
                 body = Doc};

rest_request_insert_doc(Index, Type, Id, Doc, Params) when is_binary(Index) andalso
                                                      is_binary(Type) andalso
                                                      is_binary(Id) andalso
                                                      (is_binary(Doc) orelse is_list(Doc)) andalso
                                                      is_list(Params) ->
    Uri = make_uri([Index, Type, Id], Params),
    #restRequest{method = ?elasticsearch_Method_PUT,
                 uri = Uri,
                 body = Doc}.

rest_request_update_doc(Index, Type, Id, Doc, Params) when is_binary(Index) andalso
                                                      is_binary(Type) andalso
                                                      is_binary(Id) andalso
                                                      (is_binary(Doc) orelse is_list(Doc)) andalso
                                                      is_list(Params) ->
    Uri = make_uri([Index, Type, Id, ?UPDATE], Params),
    #restRequest{method = ?elasticsearch_Method_POST,
                 uri = Uri,
                 body = Doc}.

rest_request_is_doc(Index, Type, Id) when is_binary(Index),
                                                   is_binary(Type),
                                                   is_binary(Id) ->
    Uri = make_uri([Index, Type, Id], []),
    #restRequest{method = ?elasticsearch_Method_HEAD,
                 uri = Uri}.

rest_request_get_doc(Index, Type, Id, Params) when is_binary(Index),
                                                   is_binary(Type),
                                                   is_binary(Id),
                                                   is_list(Params) ->
    Uri = make_uri([Index, Type, Id], Params),
    #restRequest{method = ?elasticsearch_Method_GET,
                 uri = Uri}.

rest_request_mget_doc(Index, Type, Doc) when is_binary(Index) andalso
                                                   is_binary(Type) andalso
                                                   (is_binary(Doc) orelse is_list(Doc)) ->
    Uri = make_uri([Index, Type, ?MGET], []),
    #restRequest{method = ?elasticsearch_Method_GET,
                 uri = Uri,
                 body = Doc}.

rest_request_delete_doc(Index, Type, Id, Params) when is_binary(Index),
                                                   is_binary(Type),
                                                   is_binary(Id),
                                                   is_list(Params) ->
    Uri = make_uri([Index, Type, Id], Params),
    #restRequest{method = ?elasticsearch_Method_DELETE,
                 uri = Uri}.

rest_request_search(Index, Type, Doc, Params) when is_binary(Index) andalso
                                                   is_binary(Type) andalso
                                                   (is_binary(Doc) orelse is_list(Doc)) andalso
                                                   is_list(Params) ->
    Uri = make_uri([Index, Type, ?SEARCH], Params),
    #restRequest{method = ?elasticsearch_Method_GET,
                 uri = Uri,
                 body = Doc}.

rest_request_bulk(<<>>, <<>>, Doc) when (is_binary(Doc) orelse is_list(Doc)) ->
    Uri = make_uri([?BULK], []),
    #restRequest{method = ?elasticsearch_Method_POST,
                 uri = Uri,
                 body = Doc};

rest_request_bulk(Index, <<>>, Doc) when is_binary(Index) andalso
                                        (is_binary(Doc) orelse is_list(Doc)) ->
    Uri = make_uri([Index, ?BULK], []),
    #restRequest{method = ?elasticsearch_Method_POST,
                 uri = Uri,
                 body = Doc};

rest_request_bulk(Index, Type, Doc) when is_binary(Index) andalso
                                         is_binary(Type) andalso
                                         (is_binary(Doc) orelse is_list(Doc)) ->
    Uri = make_uri([Index, Type, ?BULK], []),
    #restRequest{method = ?elasticsearch_Method_POST,
                 uri = Uri,
                 body = Doc}.

rest_request_refresh(Index) when is_list(Index) ->
    IndexList = erlasticsearch:join(Index, <<",">>),
    Uri = erlasticsearch:join([IndexList, ?REFRESH], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_POST,
                 uri = Uri}.

rest_request_flush(Index) when is_list(Index) ->
    IndexList = erlasticsearch:join(Index, <<",">>),
    Uri = erlasticsearch:join([IndexList, ?FLUSH], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_POST,
                 uri = Uri}.

rest_request_optimize(Index) when is_list(Index) ->
    IndexList = erlasticsearch:join(Index, <<",">>),
    Uri = erlasticsearch:join([IndexList, ?OPTIMIZE], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_POST,
                 uri = Uri}.

rest_request_segments(Index) when is_list(Index) ->
    IndexList = erlasticsearch:join(Index, <<",">>),
    Uri = erlasticsearch:join([IndexList, ?SEGMENTS], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_GET,
                 uri = Uri}.

rest_request_clear_cache(Index, Params) when is_list(Index) ->
    IndexList = erlasticsearch:join(Index, <<",">>),
    Uri = make_uri([IndexList, ?CLEAR_CACHE], Params),
    #restRequest{method = ?elasticsearch_Method_POST,
                 uri = Uri}.

rest_request_put_mapping(Index, Type, Doc) when is_list(Index),
                                                is_binary(Type),
                                                (is_binary(Doc) orelse is_list(Doc)) ->
    IndexList = erlasticsearch:join(Index, <<",">>),
    Uri = erlasticsearch:join([IndexList, ?MAPPING, Type], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_PUT,
                 uri = Uri,
                 body = Doc}.

rest_request_get_mapping(Index, Type) when is_list(Index),
                                           is_binary(Type) ->
    IndexList = erlasticsearch:join(Index, <<",">>),
    Uri = erlasticsearch:join([IndexList, ?MAPPING, Type], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_GET,
                 uri = Uri}.

rest_request_delete_mapping(Index, Type) when is_list(Index),
                                              is_binary(Type) ->
    IndexList = erlasticsearch:join(Index, <<",">>),
    Uri = erlasticsearch:join([IndexList, ?MAPPING, Type], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_DELETE,
                 uri = Uri}.

rest_request_aliases(Doc) when is_binary(Doc) orelse is_list(Doc) ->
    Uri = ?ALIASES,
    #restRequest{method = ?elasticsearch_Method_POST,
                 uri = Uri,
                 body = Doc}.

rest_request_insert_alias(Index, Alias) when is_binary(Index),
                                             is_binary(Alias) ->
    Uri = erlasticsearch:join([Index, ?ALIAS, Alias], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_PUT,
                 uri = Uri}.

rest_request_insert_alias(Index, Alias, Doc) when is_binary(Index),
                                                  is_binary(Alias),
                                                  (is_binary(Doc) orelse is_list(Doc)) ->
    Uri = erlasticsearch:join([Index, ?ALIAS, Alias], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_PUT,
                 uri = Uri,
                 body = Doc}.

rest_request_delete_alias(Index, Alias) when is_binary(Index),
                                             is_binary(Alias) ->
    Uri = erlasticsearch:join([Index, ?ALIAS, Alias], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_DELETE,
                 uri = Uri}.

rest_request_is_alias(Index, Alias) when is_binary(Index),
                                         is_binary(Alias) ->
    Uri = erlasticsearch:join([Index, ?ALIAS, Alias], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_HEAD,
                 uri = Uri}.

rest_request_get_alias(Index, Alias) when is_binary(Index),
                                          is_binary(Alias) ->
    Uri = erlasticsearch:join([Index, ?ALIAS, Alias], <<"/">>),
    #restRequest{method = ?elasticsearch_Method_GET,
                 uri = Uri}.

make_uri(BaseList, PropList) ->
    Base = erlasticsearch:join(BaseList, <<"/">>),
    case PropList of
        [] ->
            Base;
        PropList ->
            Props = uri_params_encode(PropList),
            erlasticsearch:join([Base, Props], <<"?">>)
    end.

-spec uri_params_encode([tuple()]) -> string().
uri_params_encode(Params) ->
  intercalate("&", [uri_join([K, V], "=") || {K, V} <- Params]).

uri_join(Values, Separator) ->
  string:join([uri_encode(Value) || Value <- Values], Separator).

intercalate(Sep, Xs) ->
  lists:concat(intersperse(Sep, Xs)).

intersperse(_, []) ->
  [];
intersperse(_, [X]) ->
  [X];
intersperse(Sep, [X | Xs]) ->
  [X, Sep | intersperse(Sep, Xs)].

uri_encode(Term) when is_binary(Term) ->
    uri_encode(binary_to_list(Term));
uri_encode(Term) when is_integer(Term) ->
    uri_encode(integer_to_list(Term));
uri_encode(Term) when is_atom(Term) ->
  uri_encode(atom_to_list(Term));
uri_encode(Term) when is_list(Term) ->
  uri_encode(lists:reverse(Term, []), []).

-define(is_alphanum(C), C >= $A, C =< $Z; C >= $a, C =< $z; C >= $0, C =< $9).

uri_encode([X | T], Acc) when ?is_alphanum(X); X =:= $-; X =:= $_; X =:= $.; X =:= $~ ->
  uri_encode(T, [X | Acc]);
uri_encode([X | T], Acc) ->
  NewAcc = [$%, dec2hex(X bsr 4), dec2hex(X band 16#0f) | Acc],
  uri_encode(T, NewAcc);
uri_encode([], Acc) ->
  Acc.

-compile({inline, [{dec2hex, 1}]}).

dec2hex(N) when N >= 10 andalso N =< 15 ->
  N + $A - 10;
dec2hex(N) when N >= 0 andalso N =< 9 ->
  N + $0.

-spec make_boolean_response(response(), #state{}) -> response().
make_boolean_response({error, _} = Response, _) -> Response;
make_boolean_response(Response, #state{binary_response = false}) when is_list(Response) ->
    case is_200(Response) of
        true -> [{result, true} | Response];
        false -> [{result, false} | Response]
    end;
make_boolean_response(Response, #state{binary_response = true}) when is_list(Response) ->
    case is_200(Response) of
        true -> [{result, <<"true">>} | Response];
        false -> [{result, <<"false">>} | Response]
    end.

-spec is_200(response()) -> boolean().
is_200({error, _} = Response) -> Response;
is_200(Response) ->
    case lists:keyfind(status, 1, Response) of
        {status, 200} -> true;
        {status, <<"200">>} -> true;
        _ -> false
    end.

-spec is_200_or_201(response()) -> boolean().
is_200_or_201({error, _} = Response) -> Response;
is_200_or_201(Response) ->
    case lists:keyfind(status, 1, Response) of
        {status, 200} -> true;
        {status, <<"200">>} -> true;
        {status, 201} -> true;
        {status, <<"201">>} -> true;
        _ -> false
    end.
