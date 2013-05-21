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
-module(erlasticsearch_pool_SUITE).
-author('Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>').

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").

-compile(export_all).

-define(CHECKSPEC(M,F,N), true = proper:check_spec({M,F,N})).
-define(PROPTEST(A), true = proper:quickcheck(A())).

-define(NUMTESTS, 500).
-define(DOCUMENT_DEPTH, 5).
-define(THREE_SHARDS, <<"{\"settings\":{\"number_of_shards\":3}}">>).


suite() ->
    [{ct_hooks,[cth_surefire]}, {timetrap,{seconds,320}}].

init_per_suite(Config) ->
    setup_lager(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_GroupName, Config0) ->
    ok = start(),

    Config1 = [{client, erlasticsearch_pool} | Config0],

    Index = random_name(<<"index_">>),
    IndexWithShards = bstr:join([Index, <<"with_shards">>], <<"_">>),

    Config2 = [{index, Index}, {index_with_shards, IndexWithShards}]
                ++ Config1,
    % Clear out any existing indices w/ this name
    delete_all_indices(Config2),

    Type = random_name(<<"type_">>),

    [{type, Type}] ++ Config2.

end_per_group(_GroupName, Config) ->
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    IndexWithShards = ?config(index_with_shards, Config),
    delete_all_indices(Client, Index, true),
    delete_all_indices(Client, IndexWithShards, true),
    stop(Config),
    ok.


init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

groups() ->
    [{crud_index, [],
       [t_is_index_1,
        t_is_index_all,
        t_is_type_1,
        t_is_type_all,
        t_create_index,
        t_create_index_with_shards,
        t_open_index
      ]},
     {index_helpers, [],
        [t_flush_1,
        t_flush_list,
        t_flush_all,
        t_refresh_1,
        t_refresh_list,
        t_refresh_all,
        t_optimize_1,
        t_optimize_list,
        t_optimize_all,
        t_segments_1,
        t_segments_list,
        t_segments_all,
        t_status_1,
        t_status_all,
        t_clear_cache_1,
        t_clear_cache_list,
        t_clear_cache_all

       ]},
    % These three _MUST_ be in this sequence, and by themselves
    {crud_doc, [],
      [ t_insert_doc,
       t_get_doc,
       t_delete_doc
      ]},
    {doc_helpers, [],
       [t_is_doc,
        t_mget_index,
        t_mget_type,
        t_mget_id
      ]},
     {test, [],
      [t_mget_index,
       t_mget_type,
       t_mget_id
      ]},
     {cluster_helpers, [],
      [t_health,
       t_state,
       t_nodes_info,
       t_nodes_stats
      ]},

     {search, [],
      [t_search,
       t_count,
       t_delete_by_query_param,
       t_delete_by_query_doc
      ]}
    ].

all() ->
    [
%        {group, test}
        {group, crud_index},
        {group, crud_doc},
        {group, search},
        {group, index_helpers},
        {group, cluster_helpers},
        {group, doc_helpers}
    ].

t_health(Config) ->
    Client = ?config(client, Config),
    Response = erlasticsearch_poolboy:health(Client),
    true = erlasticsearch_poolboy:is_200(Response).

t_state(Config) ->
    Client = ?config(client, Config),
    Response1 = erlasticsearch_poolboy:state(Client),
    true = erlasticsearch_poolboy:is_200(Response1),
    Response2 = erlasticsearch_poolboy:state(Client, [{filter_nodes, true}]),
    true = erlasticsearch_poolboy:is_200(Response2).

t_nodes_info(Config) ->
    Client = ?config(client, Config),
    Response1 = erlasticsearch_poolboy:nodes_info(Client),
    true = erlasticsearch_poolboy:is_200(Response1).

t_nodes_stats(Config) ->
    Client = ?config(client, Config),
    Response1 = erlasticsearch_poolboy:nodes_stats(Client),
    true = erlasticsearch_poolboy:is_200(Response1).

t_status_1(Config) ->
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    create_indices(Client, Index),
    check_status_1(Client, Index),
    delete_all_indices(Client, Index).

t_status_all(Config) ->
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    create_indices(Client, Index),
    check_status_all(Client, Index),
    delete_all_indices(Client, Index).

t_clear_cache_1(Config) ->
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    create_indices(Client, Index),
    clear_cache_1(Client, Index),
    delete_all_indices(Client, Index).

t_clear_cache_list(Config) ->
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    create_indices(Client, Index),
    clear_cache_list(Client, Index),
    delete_all_indices(Client, Index).

t_clear_cache_all(Config) ->
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    create_indices(Client, Index),
    clear_cache_all(Client, Index),
    delete_all_indices(Client, Index).

t_is_index_1(Config) ->
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    create_indices(Client, Index),
    are_indices_1(Client, Index),
    delete_all_indices(Client, Index).

t_is_index_all(Config) ->
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    create_indices(Client, Index),
    are_indices_all(Client, Index),
    delete_all_indices(Client, Index).

t_is_type_1(Config) ->
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    build_data(Client, Index, Type),
    are_types_1(Client, Index, Type),
    clear_data(Client, Index).

t_is_type_all(Config) ->
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    build_data(Client, Index, Type),
    are_types_all(Client, Index, Type).
%    clear_data(Client, Index).

check_status_1(Client, Index) ->
    lists:foreach(fun(X) ->
                FullIndex = enumerated(Index, X),
                Response = erlasticsearch_poolboy:status(Client, FullIndex),
                true = erlasticsearch_poolboy:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

check_status_all(Client, Index) ->
    FullIndexList =
    lists:map(fun(X) ->
                enumerated(Index, X)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    Response = erlasticsearch_poolboy:status(Client, FullIndexList),
    true = erlasticsearch_poolboy:is_200(Response).

clear_cache_1(Client, Index) ->
    lists:foreach(fun(X) ->
                FullIndex = enumerated(Index, X),
                Response1 = erlasticsearch_poolboy:clear_cache(Client, FullIndex),
                true = erlasticsearch_poolboy:is_200(Response1),
                Response2 = erlasticsearch_poolboy:clear_cache(Client, FullIndex, [{filter, true}]),
                true = erlasticsearch_poolboy:is_200(Response2)
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

clear_cache_list(Client, Index) ->
    FullIndexList =
    lists:map(fun(X) ->
                enumerated(Index, X)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    Response1 = erlasticsearch_poolboy:clear_cache(Client, FullIndexList),
    true = erlasticsearch_poolboy:is_200(Response1),
    Response2 = erlasticsearch_poolboy:clear_cache(Client, FullIndexList, [{filter, true}]),
    true = erlasticsearch_poolboy:is_200(Response2).

clear_cache_all(Client, _Index) ->
    Response1 = erlasticsearch_poolboy:clear_cache(Client),
    true = erlasticsearch_poolboy:is_200(Response1),
    Response2 = erlasticsearch_poolboy:clear_cache(Client, [], [{filter, true}]),
    true = erlasticsearch_poolboy:is_200(Response2).


are_types_1(Client, Index, Type) ->
    lists:foreach(fun(X) ->
                FullIndex = enumerated(Index, X),
                lists:foreach(fun(Y) ->
                            FullType = enumerated(Type, Y),
                            true = erlasticsearch_poolboy:is_type(Client, FullIndex, FullType)
                    end, lists:seq(1, ?DOCUMENT_DEPTH))
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

are_types_all(Client, Index, Type) ->
    FullIndexList =
    lists:map(fun(X) ->
                enumerated(Index, X)
            end, lists:seq(1, ?DOCUMENT_DEPTH)),
    FullTypeList =
    lists:map(fun(X) ->
                enumerated(Type, X)
            end, lists:seq(1, ?DOCUMENT_DEPTH)),
    % List of indices
    lists:foreach(fun(X) ->
                FullType = enumerated(Type, X),
                true = erlasticsearch_poolboy:is_type(Client, FullIndexList, FullType)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    % List of types
    lists:foreach(fun(X) ->
                FullIndex = enumerated(Index, X),
                true = erlasticsearch_poolboy:is_type(Client, FullIndex, FullTypeList)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    % List of indices and types
    true = erlasticsearch_poolboy:is_type(Client, FullIndexList, FullTypeList).

build_data(Client, Index, Type) ->
    lists:foreach(fun(X) ->
                FullIndex = enumerated(Index, X),
                lists:foreach(fun(Y) ->
                            FullType = enumerated(Type, Y),
                            BX = list_to_binary(integer_to_list(X)),
                            erlasticsearch_poolboy:insert_doc(Client, FullIndex,
                                                      FullType, BX, json_document(X)),
                            erlasticsearch_poolboy:flush(Client)
                    end, lists:seq(1, ?DOCUMENT_DEPTH))
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

clear_data(Client, Index) ->
    lists:foreach(fun(X) ->
                FullIndex = enumerated(Index, X),
                erlasticsearch_poolboy:delete_index(Client, FullIndex)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    erlasticsearch_poolboy:flush(Client).

% Also deletes indices
t_create_index(Config) ->
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    create_indices(Client, Index),
    delete_all_indices(Client, Index).

t_create_index_with_shards(Config) ->
    Client = ?config(client, Config),
    Index = ?config(index_with_shards, Config),
    create_indices(Client, Index),
    delete_all_indices(Client, Index).

t_flush_1(Config) ->
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    create_indices(Client, Index),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = bstr:join([Index, BX], <<"_">>),
                Response = erlasticsearch_poolboy:flush(Client, FullIndex),
                true = erlasticsearch_poolboy:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    delete_all_indices(Client, Index).

t_flush_list(Config) ->
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    create_indices(Client, Index),
    Indexes =
    lists:map(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                bstr:join([Index, BX], <<"_">>)
            end, lists:seq(1, ?DOCUMENT_DEPTH)),
    Response = erlasticsearch_poolboy:flush(Client, Indexes),
    true = erlasticsearch_poolboy:is_200(Response),
    delete_all_indices(Client, Index).

t_flush_all(Config) ->
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    create_indices(Client, Index),
    Response = erlasticsearch_poolboy:flush(Client),
    true = erlasticsearch_poolboy:is_200(Response),
    delete_all_indices(Client, Index).

t_refresh_1(Config) ->
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    create_indices(Client, Index),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = bstr:join([Index, BX], <<"_">>),
                Response = erlasticsearch_poolboy:refresh(Client, FullIndex),
                true = erlasticsearch_poolboy:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    delete_all_indices(Client, Index).

t_refresh_list(Config) ->
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    create_indices(Client, Index),
    Indexes =
    lists:map(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                bstr:join([Index, BX], <<"_">>)
            end, lists:seq(1, ?DOCUMENT_DEPTH)),
    Response = erlasticsearch_poolboy:refresh(Client, Indexes),
    true = erlasticsearch_poolboy:is_200(Response),
    delete_all_indices(Client, Index).

t_refresh_all(Config) ->
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    create_indices(Client, Index),
    Response = erlasticsearch_poolboy:refresh(Client),
    true = erlasticsearch_poolboy:is_200(Response),
    delete_all_indices(Client, Index).

t_optimize_1(Config) ->
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    create_indices(Client, Index),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = bstr:join([Index, BX], <<"_">>),
                Response = erlasticsearch_poolboy:optimize(Client, FullIndex),
                true = erlasticsearch_poolboy:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    delete_all_indices(Client, Index).

t_optimize_list(Config) ->
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    create_indices(Client, Index),
    Indexes =
    lists:map(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                bstr:join([Index, BX], <<"_">>)
            end, lists:seq(1, ?DOCUMENT_DEPTH)),
    Response = erlasticsearch_poolboy:optimize(Client, Indexes),
    true = erlasticsearch_poolboy:is_200(Response),
    delete_all_indices(Client, Index).

t_optimize_all(Config) ->
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    create_indices(Client, Index),
    Response = erlasticsearch_poolboy:optimize(Client),
    true = erlasticsearch_poolboy:is_200(Response),
    delete_all_indices(Client, Index).

t_segments_1(Config) ->
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    create_indices(Client, Index),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = bstr:join([Index, BX], <<"_">>),
                Response = erlasticsearch_poolboy:segments(Client, FullIndex),
                true = erlasticsearch_poolboy:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    delete_all_indices(Client, Index).

t_segments_list(Config) ->
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    create_indices(Client, Index),
    Indexes =
    lists:map(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                bstr:join([Index, BX], <<"_">>)
            end, lists:seq(1, ?DOCUMENT_DEPTH)),
    Response = erlasticsearch_poolboy:segments(Client, Indexes),
    true = erlasticsearch_poolboy:is_200(Response),
    delete_all_indices(Client, Index).

t_segments_all(Config) ->
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    create_indices(Client, Index),
    Response = erlasticsearch_poolboy:segments(Client),
    true = erlasticsearch_poolboy:is_200(Response),
    delete_all_indices(Client, Index).

t_open_index(Config) ->
        t_insert_doc(Config),
        Client = ?config(client, Config),
        Index = ?config(index, Config),
        Response = erlasticsearch_poolboy:close_index(Client, Index),
        true = erlasticsearch_poolboy:is_200(Response),
        Response = erlasticsearch_poolboy:open_index(Client, Index),
        true = erlasticsearch_poolboy:is_200(Response),
        t_delete_doc(Config).


t_mget_id(Config) ->
    t_insert_doc(Config),
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    Query = id_query(),
    Result = erlasticsearch_poolboy:mget_doc(Client, Index, Type, Query),
    ?DOCUMENT_DEPTH  = docs_from_result(Result),
    t_delete_doc(Config).

t_mget_type(Config) ->
    t_insert_doc(Config),
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    Query = id_query(Type),
    Result = erlasticsearch_poolboy:mget_doc(Client, Index, Query),
    ?DOCUMENT_DEPTH  = docs_from_result(Result),
    t_delete_doc(Config).

t_mget_index(Config) ->
    t_insert_doc(Config),
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    Query = id_query(Index, Type),
    Result = erlasticsearch_poolboy:mget_doc(Client, Query),
    ?DOCUMENT_DEPTH  = docs_from_result(Result),
    t_delete_doc(Config).

t_search(Config) ->
    t_insert_doc(Config),
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    lists:foreach(fun(X) ->
                Query = param_query(X),
                Result = erlasticsearch_poolboy:search(Client, Index, Type, <<>>, [{q, Query}]),
                % The document is structured so that the number of top level
                % keys is as (?DOCUMENT_DEPTH + 1 - X)
                ?DOCUMENT_DEPTH  = hits_from_result(Result) + X - 1
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    t_delete_doc(Config).

t_count(Config) ->
    t_insert_doc(Config),
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    lists:foreach(fun(X) ->
                Query1 = param_query(X),
                Query2 = json_query(X),

                % query as parameter
                Result1 = erlasticsearch_poolboy:count(Client, Index, Type, <<>>, [{q, Query1}]),
                % The document is structured so that the number of top level
                % keys is as (?DOCUMENT_DEPTH + 1 - X)
                ?DOCUMENT_DEPTH  = count_from_result(Result1) + X - 1,

                % query as doc
                Result2 = erlasticsearch_poolboy:count(Client, Index, Type, Query2, []),
                % The document is structured so that the number of top level
                % keys is as (?DOCUMENT_DEPTH + 1 - X)
                ?DOCUMENT_DEPTH  = count_from_result(Result2) + X - 1

        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    t_delete_doc(Config).

t_delete_by_query_param(Config) ->
    % One Index
    t_insert_doc(Config),
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    Query1 = param_query(1),
    Result1 = erlasticsearch_poolboy:count(Client, Index, Type, <<>>, [{q, Query1}]),
    5 = count_from_result(Result1),
    DResult1 = erlasticsearch_poolboy:delete_by_query(Client, Index, Type, <<>>, [{q, Query1}]),
    true = erlasticsearch_poolboy:is_200(DResult1),
    erlasticsearch_poolboy:flush(Client, Index),
    DResult1a = erlasticsearch_poolboy:count(Client, Index, Type, <<>>, [{q, Query1}]),
    0  = count_from_result(DResult1a),

    % All Indices
    t_insert_doc(Config),
    ADResult1 = erlasticsearch_poolboy:delete_by_query(Client, <<>>, [{q, Query1}]),
    true = erlasticsearch_poolboy:is_200(ADResult1),
    erlasticsearch_poolboy:flush(Client, Index),
    ADResult1a = erlasticsearch_poolboy:count(Client, <<>>, [{q, Query1}]),
    0  = count_from_result(ADResult1a).
    % Don't need to delete docs, 'cos they are already deleted
%    t_delete_doc(Config).

t_delete_by_query_doc(Config) ->
    t_insert_doc(Config),
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    Query1 = param_query(1),
    Query2 = json_query(1),
    Result1 = erlasticsearch_poolboy:count(Client, Index, Type, <<>>, [{q, Query1}]),
    5 = count_from_result(Result1),
    DResult1 = erlasticsearch_poolboy:delete_by_query(Client, Index, Type, Query2, []),
    true = erlasticsearch_poolboy:is_200(DResult1),
    erlasticsearch_poolboy:flush(Client, Index),
    DResult1a = erlasticsearch_poolboy:count(Client, Index, Type, <<>>, [{q, Query1}]),
    0  = count_from_result(DResult1a),

    % All Indices
    t_insert_doc(Config),
    ADResult1 = erlasticsearch_poolboy:delete_by_query(Client, Query2),
    true = erlasticsearch_poolboy:is_200(ADResult1),
    erlasticsearch_poolboy:flush(Client, Index),
    ADResult1a = erlasticsearch_poolboy:count(Client, <<>>, [{q, Query1}]),
    0  = count_from_result(ADResult1a).
    % Don't need to delete docs, 'cos they are already deleted
%    t_delete_doc(Config).


id_query() ->
    Ids = lists:map(fun(X) ->
                    BX = list_to_binary(integer_to_list(X)),
                    [{<<"_id">>, BX}]
            end, lists:seq(1, ?DOCUMENT_DEPTH)),
    jsx:encode([{docs, Ids}]).

id_query(Type) ->
    Ids = lists:map(fun(X) ->
                    BX = list_to_binary(integer_to_list(X)),
                    [{<<"_type">>, Type}, {<<"_id">>, BX}]
            end, lists:seq(1, ?DOCUMENT_DEPTH)),
    jsx:encode([{docs, Ids}]).

id_query(Index, Type) ->
    Ids = lists:map(fun(X) ->
                    BX = list_to_binary(integer_to_list(X)),
                    [{<<"_index">>, Index}, {<<"_type">>, Type}, {<<"_id">>, BX}]
            end, lists:seq(1, ?DOCUMENT_DEPTH)),
    jsx:encode([{docs, Ids}]).


param_query(X) ->
    Key = key(X),
    Value = value(X),
    bstr:join([Key, Value], <<":">>).

json_query(X) ->
    Key = key(X),
    Value = value(X),
    jsx:encode([{term, [{Key, Value}]}]).

hits_from_result({ok, {_, _, _, JSON}}) ->
    case lists:keyfind(<<"hits">>, 1, jsx:decode(JSON)) of
        false -> throw(false);
        {_, Result} ->
            case lists:keyfind(<<"total">>, 1, Result) of
                false -> throw(false);
                {_, Data} -> Data
            end
    end.

docs_from_result({ok, {_, _, _, JSON}}) ->
    case lists:keyfind(<<"docs">>, 1, jsx:decode(JSON)) of
        false -> throw(false);
        {_, Result} ->
            length(Result)
    end.


count_from_result({ok, {_, _, _, JSON}}) ->
    case lists:keyfind(<<"count">>, 1, jsx:decode(JSON)) of
        false -> throw(false);
        {_, Data} -> Data
    end.

t_insert_doc(Config) ->
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                Response = erlasticsearch_poolboy:insert_doc(Client, Index,
                                                     Type, BX, json_document(X)),
                true = erlasticsearch_poolboy:is_200_or_201(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    erlasticsearch_poolboy:flush(Client, Index).

t_is_doc(Config) ->
    t_insert_doc(Config),
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                true = erlasticsearch_poolboy:is_doc(Client, Index, Type, BX)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    t_delete_doc(Config).

t_get_doc(Config) ->
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                Response = erlasticsearch_poolboy:get_doc(Client, Index, Type, BX),
                true = erlasticsearch_poolboy:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

t_delete_doc(Config) ->
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                Response = erlasticsearch_poolboy:delete_doc(Client, Index, Type, BX),
                true = erlasticsearch_poolboy:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    erlasticsearch_poolboy:flush(Client, Index).

%% Test helpers
% Create a bunch-a indices
create_indices(Client, Index) ->
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = bstr:join([Index, BX], <<"_">>),
                erlasticsearch_poolboy:create_index(Client, FullIndex),
                erlasticsearch_poolboy:flush(Client, Index)
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

are_indices_1(Client, Index) ->
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = bstr:join([Index, BX], <<"_">>),
                true = erlasticsearch_poolboy:is_index(Client, FullIndex)
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

are_indices_all(Client, Index) ->
    FullIndexList =
    lists:map(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                bstr:join([Index, BX], <<"_">>)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    true = erlasticsearch_poolboy:is_index(Client, FullIndexList).

delete_all_indices(Config) ->
    Client = ?config(client, Config),
    Index = ?config(index, Config),
    IndexWithShards = bstr:join([Index, <<"with_shards">>], <<"_">>),
    delete_all_indices(Client, Index, true),
    delete_all_indices(Client, IndexWithShards, true),
    erlasticsearch_poolboy:flush(Client).

% By default, blindly delete
delete_all_indices(Client, Index) ->
    delete_all_indices(Client, Index, false).

% Optionally check to see if the indices exist before trying to delete
delete_all_indices(Client, Index, CheckIndex) ->
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = bstr:join([Index, BX], <<"_">>),

                case CheckIndex of
                    true ->
                        case erlasticsearch_poolboy:is_index(Client, FullIndex) of
                            % Only delete if the index exists
                            true ->
                                delete_this_index(Client, FullIndex);
                            false ->
                                true
                        end;
                    false ->
                        % Blindly Delete the indices
                        delete_this_index(Client, FullIndex)
                end
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

delete_this_index(Client, Index) ->
    Response = erlasticsearch_poolboy:delete_index(Client, Index),
    true = erlasticsearch_poolboy:is_200(Response).

json_document(N) ->
    jsx:to_json(document(N)).

document(N) ->
    lists:foldl(fun(X, Acc) ->
                Tuple = case X of
                    1 ->
                        [{key(1), value(1)}];
                    X ->
                        [{key(X), value(X)},
                         {sub(X), document(N-1)}]
                end,
               Tuple ++ Acc
        end, [], lists:seq(1, N)).

key(N) -> data_index(key, N).
value(N) -> data_index(value, N).
sub(N) -> data_index(sub, N).
enumerated(Item, N) when is_binary(Item) ->
    data_index(list_to_atom(binary_to_list(Item)), N).

-spec data_index(atom(), integer()) -> binary().
data_index(Data, Index) ->
    BData = list_to_binary(atom_to_list(Data)),
    BIndex = list_to_binary(integer_to_list(Index)),
    bstr:join([BData, BIndex], <<"_">>).

random_name(Name) ->
    random:seed(erlang:now()),
    Id = list_to_binary(integer_to_list(random:uniform(999999999))),
    <<Name/binary, Id/binary>>.

setup_lager() ->
    application:start(crypto),
    application:start(compiler),
    application:start(syntax_tools),
    application:start(lager),
    lager:set_loglevel(lager_console_backend, debug),
    lager:set_loglevel(lager_file_backend, "console.log", debug).

start() ->
    application:start(kernel),
    application:start(stdlib),
    application:start(crypto),
    application:start(compiler),
    application:start(syntax_tools),
    application:start(sasl),
    application:start(bstr),
    application:start(jsx),
    application:start(poolboy),
    application:start(erlasticsearch),
    ok.

stop(_Config) ->
    ok.
%    application:stop(jsx),
%    application:stop(bstr),
%    application:stop(sasl),
%    application:stop(syntax_tools),
%    application:stop(compiler),
%    application:stop(crypto),
%    application:stop(stdlib),
%    application:stop(kernel).
