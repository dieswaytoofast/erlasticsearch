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
-module(erlasticsearch_SUITE).
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
%    setup_lager(),
    setup_environment(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_GroupName, Config0) ->
    ClientName = random_name(<<"client_">>),
    PoolName = random_name(<<"pool_">>),
    Config1 = [{client_name, ClientName},
               {pool_name, PoolName},
               {pool, {pool, PoolName}} | Config0],
    start(Config1),


    Index = random_name(<<"index_">>),
    IndexWithShards = erlasticsearch:join([Index, <<"with_shards">>], <<"_">>),

    Config2 = [{index, Index}, {index_with_shards, IndexWithShards}]
                ++ Config1,
    % Clear out any existing indices w/ this name
    delete_all_indices(ClientName, Config2),

    Type = random_name(<<"type_">>),

    [{type, Type}] ++ Config2.

end_per_group(_GroupName, Config) ->
    ClientName = ?config(client_name, Config),
    Index = ?config(index, Config),
    IndexWithShards = ?config(index_with_shards, Config),
    delete_all_indices(ClientName, Index, true),
    delete_all_indices(ClientName, IndexWithShards, true),
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
      [
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
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_health(Pool, Config),
    process_t_health(ClientName, Config).

process_t_health(ServerRef, _Config) ->
    Response = erlasticsearch:health(ServerRef),
    true = erlasticsearch:is_200(Response).

t_state(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_state(Pool, Config),
    process_t_state(ClientName, Config).

process_t_state(ServerRef, _Config) ->
    Response1 = erlasticsearch:state(ServerRef),
    true = erlasticsearch:is_200(Response1),
    Response2 = erlasticsearch:state(ServerRef, [{filter_nodes, true}]),
    true = erlasticsearch:is_200(Response2).

t_nodes_info(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_nodes_info(Pool, Config),
    process_t_nodes_info(ClientName, Config).

process_t_nodes_info(ServerRef, _Config) ->
    Response1 = erlasticsearch:nodes_info(ServerRef),
    true = erlasticsearch:is_200(Response1).

t_nodes_stats(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_nodes_stats(Pool, Config),
    process_t_nodes_stats(ClientName, Config).

process_t_nodes_stats(ServerRef, _Config) ->
    Response1 = erlasticsearch:nodes_stats(ServerRef),
    true = erlasticsearch:is_200(Response1).

t_status_1(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_status_1(Pool, Config),
    process_t_status_1(ClientName, Config).

process_t_status_1(ServerRef, Config) ->
    Index = ?config(index, Config),
    create_indices(ServerRef, Index),
    check_status_1(ServerRef, Index),
    delete_all_indices(ServerRef, Index, true).

t_status_all(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_status_all(Pool, Config),
    process_t_status_all(ClientName, Config).

process_t_status_all(ServerRef, Config) ->
    Index = ?config(index, Config),
    create_indices(ServerRef, Index),
    check_status_all(ServerRef, Index),
    delete_all_indices(ServerRef, Index, true).

t_clear_cache_1(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_clear_cache_1(Pool, Config),
    process_t_clear_cache_1(ClientName, Config).

process_t_clear_cache_1(ServerRef, Config) ->
    Index = ?config(index, Config),
    create_indices(ServerRef, Index),
    clear_cache_1(ServerRef, Index),
    delete_all_indices(ServerRef, Index, true).

t_clear_cache_list(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_clear_cache_list(Pool, Config),
    process_t_clear_cache_list(ClientName, Config).

process_t_clear_cache_list(ServerRef, Config) ->
    Index = ?config(index, Config),
    create_indices(ServerRef, Index),
    clear_cache_list(ServerRef, Index),
    delete_all_indices(ServerRef, Index, true).

t_clear_cache_all(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_clear_cache_all(Pool, Config),
    process_t_clear_cache_all(ClientName, Config).

process_t_clear_cache_all(ServerRef, Config) ->
    Index = ?config(index, Config),
    create_indices(ServerRef, Index),
    clear_cache_all(ServerRef, Index),
    delete_all_indices(ServerRef, Index, true).

t_is_index_1(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_is_index_1(Pool, Config),
    process_t_is_index_1(ClientName, Config).

process_t_is_index_1(ServerRef, Config) ->
    Index = ?config(index, Config),
    create_indices(ServerRef, Index),
    are_indices_1(ServerRef, Index),
    delete_all_indices(ServerRef, Index, true).

t_is_index_all(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_is_index_all(Pool, Config),
    process_t_is_index_all(ClientName, Config).

process_t_is_index_all(ServerRef, Config) ->
    Index = ?config(index, Config),
    create_indices(ServerRef, Index),
    are_indices_all(ServerRef, Index),
    delete_all_indices(ServerRef, Index, true).

t_is_type_1(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_is_type_1(Pool, Config),
    process_t_is_type_1(ClientName, Config).

process_t_is_type_1(ServerRef, Config) ->
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    build_data(ServerRef, Index, Type),
    are_types_1(ServerRef, Index, Type),
    clear_data(ServerRef, Index).

t_is_type_all(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_is_type_all(Pool, Config),
    process_t_is_type_all(ClientName, Config).

process_t_is_type_all(ServerRef, Config) ->
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    build_data(ServerRef, Index, Type),
    are_types_all(ServerRef, Index, Type).
%    clear_data(ServerRef, Index).

check_status_1(ServerRef, Index) ->
    lists:foreach(fun(X) ->
                FullIndex = enumerated(Index, X),
                Response = erlasticsearch:status(ServerRef, FullIndex),
                true = erlasticsearch:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

check_status_all(ServerRef, Index) ->
    FullIndexList = 
    lists:map(fun(X) ->
                enumerated(Index, X)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    Response = erlasticsearch:status(ServerRef, FullIndexList),
    true = erlasticsearch:is_200(Response).

clear_cache_1(ServerRef, Index) ->
    lists:foreach(fun(X) ->
                FullIndex = enumerated(Index, X),
                Response1 = erlasticsearch:clear_cache(ServerRef, FullIndex),
                true = erlasticsearch:is_200(Response1),
                Response2 = erlasticsearch:clear_cache(ServerRef, FullIndex, [{filter, true}]),
                true = erlasticsearch:is_200(Response2)
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

clear_cache_list(ServerRef, Index) ->
    FullIndexList = 
    lists:map(fun(X) ->
                enumerated(Index, X)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    Response1 = erlasticsearch:clear_cache(ServerRef, FullIndexList),
    true = erlasticsearch:is_200(Response1),
    Response2 = erlasticsearch:clear_cache(ServerRef, FullIndexList, [{filter, true}]),
    true = erlasticsearch:is_200(Response2).

clear_cache_all(ServerRef, _Index) ->
    Response1 = erlasticsearch:clear_cache(ServerRef),
    true = erlasticsearch:is_200(Response1),
    Response2 = erlasticsearch:clear_cache(ServerRef, [], [{filter, true}]),
    true = erlasticsearch:is_200(Response2).


are_types_1(ServerRef, Index, Type) ->
    lists:foreach(fun(X) ->
                FullIndex = enumerated(Index, X),
                lists:foreach(fun(Y) ->
                            FullType = enumerated(Type, Y),
                            true = erlasticsearch:is_type(ServerRef, FullIndex, FullType)
                    end, lists:seq(1, ?DOCUMENT_DEPTH))
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

are_types_all(ServerRef, Index, Type) ->
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
                true = erlasticsearch:is_type(ServerRef, FullIndexList, FullType)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    % List of types
    lists:foreach(fun(X) ->
                FullIndex = enumerated(Index, X),
                true = erlasticsearch:is_type(ServerRef, FullIndex, FullTypeList)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    % List of indices and types
    true = erlasticsearch:is_type(ServerRef, FullIndexList, FullTypeList).

build_data(ServerRef, Index, Type) ->
    lists:foreach(fun(X) ->
                FullIndex = enumerated(Index, X),
                lists:foreach(fun(Y) ->
                            FullType = enumerated(Type, Y),
                            BX = list_to_binary(integer_to_list(X)),
                            erlasticsearch:insert_doc(ServerRef, FullIndex, 
                                                      FullType, BX, json_document(X)),
                            erlasticsearch:flush(ServerRef)
                    end, lists:seq(1, ?DOCUMENT_DEPTH))
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

clear_data(ServerRef, Index) ->
    lists:foreach(fun(X) ->
                FullIndex = enumerated(Index, X),
                erlasticsearch:delete_index(ServerRef, FullIndex)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    erlasticsearch:flush(ServerRef).

% Also deletes indices
t_create_index(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_create_index(Pool, Config),
    process_t_create_index(ClientName, Config).

process_t_create_index(ServerRef, Config) ->
    Index = ?config(index, Config),
    create_indices(ServerRef, Index),
    delete_all_indices(ServerRef, Index, true).

t_create_index_with_shards(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_create_index_with_shards(Pool, Config),
    process_t_create_index_with_shards(ClientName, Config).

process_t_create_index_with_shards(ServerRef, Config) ->
    Index = ?config(index_with_shards, Config),
    create_indices(ServerRef, Index),
    delete_all_indices(ServerRef, Index, true).

t_flush_1(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_flush_1(Pool, Config),
    process_t_flush_1(ClientName, Config).

process_t_flush_1(ServerRef, Config) ->
    Index = ?config(index, Config),
    create_indices(ServerRef, Index),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = erlasticsearch:join([Index, BX], <<"_">>),
                Response = erlasticsearch:flush(ServerRef, FullIndex),
                true = erlasticsearch:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    delete_all_indices(ServerRef, Index, true).

t_flush_list(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_flush_list(Pool, Config),
    process_t_flush_list(ClientName, Config).

process_t_flush_list(ServerRef, Config) ->
    Index = ?config(index, Config),
    create_indices(ServerRef, Index),
    Indexes = 
    lists:map(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                erlasticsearch:join([Index, BX], <<"_">>)
            end, lists:seq(1, ?DOCUMENT_DEPTH)),
    Response = erlasticsearch:flush(ServerRef, Indexes),
    true = erlasticsearch:is_200(Response),
    delete_all_indices(ServerRef, Index, true).

t_flush_all(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_flush_all(Pool, Config),
    process_t_flush_all(ClientName, Config).

process_t_flush_all(ServerRef, Config) ->
    Index = ?config(index, Config),
    create_indices(ServerRef, Index),
    Response = erlasticsearch:flush(ServerRef),
    true = erlasticsearch:is_200(Response),
    delete_all_indices(ServerRef, Index, true).

t_refresh_1(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_refresh_1(Pool, Config),
    process_t_refresh_1(ClientName, Config).

process_t_refresh_1(ServerRef, Config) ->
    Index = ?config(index, Config),
    create_indices(ServerRef, Index),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = erlasticsearch:join([Index, BX], <<"_">>),
                Response = erlasticsearch:refresh(ServerRef, FullIndex),
                true = erlasticsearch:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    delete_all_indices(ServerRef, Index, true).

t_refresh_list(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_refresh_list(Pool, Config),
    process_t_refresh_list(ClientName, Config).

process_t_refresh_list(ServerRef, Config) ->
    Index = ?config(index, Config),
    create_indices(ServerRef, Index),
    Indexes = 
    lists:map(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                erlasticsearch:join([Index, BX], <<"_">>)
            end, lists:seq(1, ?DOCUMENT_DEPTH)),
    Response = erlasticsearch:refresh(ServerRef, Indexes),
    true = erlasticsearch:is_200(Response),
    delete_all_indices(ServerRef, Index, true).

t_refresh_all(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_refresh_all(Pool, Config),
    process_t_refresh_all(ClientName, Config).

process_t_refresh_all(ServerRef, Config) ->
    Index = ?config(index, Config),
    create_indices(ServerRef, Index),
    Response = erlasticsearch:refresh(ServerRef),
    true = erlasticsearch:is_200(Response),
    delete_all_indices(ServerRef, Index, true).

t_optimize_1(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_optimize_1(Pool, Config),
    process_t_optimize_1(ClientName, Config).

process_t_optimize_1(ServerRef, Config) ->
    Index = ?config(index, Config),
    create_indices(ServerRef, Index),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = erlasticsearch:join([Index, BX], <<"_">>),
                Response = erlasticsearch:optimize(ServerRef, FullIndex),
                true = erlasticsearch:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    delete_all_indices(ServerRef, Index, true).

t_optimize_list(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_optimize_list(Pool, Config),
    process_t_optimize_list(ClientName, Config).

process_t_optimize_list(ServerRef, Config) ->
    Index = ?config(index, Config),
    create_indices(ServerRef, Index),
    Indexes = 
    lists:map(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                erlasticsearch:join([Index, BX], <<"_">>)
            end, lists:seq(1, ?DOCUMENT_DEPTH)),
    Response = erlasticsearch:optimize(ServerRef, Indexes),
    true = erlasticsearch:is_200(Response),
    delete_all_indices(ServerRef, Index, true).

t_optimize_all(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_optimize_all(Pool, Config),
    process_t_optimize_all(ClientName, Config).

process_t_optimize_all(ServerRef, Config) ->
    Index = ?config(index, Config),
    create_indices(ServerRef, Index),
    Response = erlasticsearch:optimize(ServerRef),
    true = erlasticsearch:is_200(Response),
    delete_all_indices(ServerRef, Index, true).

t_segments_1(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_segments_1(Pool, Config),
    process_t_segments_1(ClientName, Config).

process_t_segments_1(ServerRef, Config) ->
    Index = ?config(index, Config),
    create_indices(ServerRef, Index),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = erlasticsearch:join([Index, BX], <<"_">>),
                Response = erlasticsearch:segments(ServerRef, FullIndex),
                true = erlasticsearch:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    delete_all_indices(ServerRef, Index, true).

t_segments_list(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_segments_list(Pool, Config),
    process_t_segments_list(ClientName, Config).

process_t_segments_list(ServerRef, Config) ->
    Index = ?config(index, Config),
    create_indices(ServerRef, Index),
    Indexes = 
    lists:map(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                erlasticsearch:join([Index, BX], <<"_">>)
            end, lists:seq(1, ?DOCUMENT_DEPTH)),
    Response = erlasticsearch:segments(ServerRef, Indexes),
    true = erlasticsearch:is_200(Response),
    delete_all_indices(ServerRef, Index, true).

t_segments_all(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_segments_all(Pool, Config),
    process_t_segments_all(ClientName, Config).

process_t_segments_all(ServerRef, Config) ->
    Index = ?config(index, Config),
    create_indices(ServerRef, Index),
    Response = erlasticsearch:segments(ServerRef),
    true = erlasticsearch:is_200(Response),
    delete_all_indices(ServerRef, Index, true).

t_open_index(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_open_index(Pool, Config),
    process_t_open_index(ClientName, Config).

process_t_open_index(ServerRef, Config) ->
        process_t_insert_doc(ServerRef, Config),
        Index = ?config(index, Config),
        Response = erlasticsearch:close_index(ServerRef, Index),
        true = erlasticsearch:is_200(Response),
        Response = erlasticsearch:open_index(ServerRef, Index),
        true = erlasticsearch:is_200(Response),
        process_t_delete_doc(ServerRef, Config).
        

t_mget_id(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_mget_id(Pool, Config),
    process_t_mget_id(ClientName, Config).

process_t_mget_id(ServerRef, Config) ->
    process_t_insert_doc(ServerRef, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    Query = id_query(),
    Result = erlasticsearch:mget_doc(ServerRef, Index, Type, Query),
    ?DOCUMENT_DEPTH  = docs_from_result(Result),
    process_t_delete_doc(ServerRef, Config).

t_mget_type(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_mget_type(Pool, Config),
    process_t_mget_type(ClientName, Config).

process_t_mget_type(ServerRef, Config) ->
    process_t_insert_doc(ServerRef, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    Query = id_query(Type),
    Result = erlasticsearch:mget_doc(ServerRef, Index, Query),
    ?DOCUMENT_DEPTH  = docs_from_result(Result),
    process_t_delete_doc(ServerRef, Config).

t_mget_index(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_mget_index(Pool, Config),
    process_t_mget_index(ClientName, Config).

process_t_mget_index(ServerRef, Config) ->
    process_t_insert_doc(ServerRef, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    Query = id_query(Index, Type),
    Result = erlasticsearch:mget_doc(ServerRef, Query),
    ?DOCUMENT_DEPTH  = docs_from_result(Result),
    process_t_delete_doc(ServerRef, Config).

t_search(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_search(Pool, Config),
    process_t_search(ClientName, Config).

process_t_search(ServerRef, Config) ->
    process_t_insert_doc(ServerRef, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    lists:foreach(fun(X) ->
                Query = param_query(X),
                Result = erlasticsearch:search(ServerRef, Index, Type, <<>>, [{q, Query}]),
                % The document is structured so that the number of top level
                % keys is as (?DOCUMENT_DEPTH + 1 - X)
                ?DOCUMENT_DEPTH  = hits_from_result(Result) + X - 1
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    process_t_delete_doc(ServerRef, Config).

t_count(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_count(Pool, Config),
    process_t_count(ClientName, Config).

process_t_count(ServerRef, Config) ->
    process_t_insert_doc(ServerRef, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    lists:foreach(fun(X) ->
                Query1 = param_query(X),
                Query2 = json_query(X),

                % query as parameter
                Result1 = erlasticsearch:count(ServerRef, Index, Type, <<>>, [{q, Query1}]),
                % The document is structured so that the number of top level
                % keys is as (?DOCUMENT_DEPTH + 1 - X)
                ?DOCUMENT_DEPTH  = count_from_result(Result1) + X - 1,

                % query as doc
                Result2 = erlasticsearch:count(ServerRef, Index, Type, Query2, []),
                % The document is structured so that the number of top level
                % keys is as (?DOCUMENT_DEPTH + 1 - X)
                ?DOCUMENT_DEPTH  = count_from_result(Result2) + X - 1

        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    process_t_delete_doc(ServerRef, Config).

t_delete_by_query_param(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_delete_by_query_param(Pool, Config),
    process_t_delete_by_query_param(ClientName, Config).

process_t_delete_by_query_param(ServerRef, Config) ->
    % One Index
    process_t_insert_doc(ServerRef, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    Query1 = param_query(1),
    Result1 = erlasticsearch:count(ServerRef, Index, Type, <<>>, [{q, Query1}]),
    5 = count_from_result(Result1),
    DResult1 = erlasticsearch:delete_by_query(ServerRef, Index, Type, <<>>, [{q, Query1}]),
    true = erlasticsearch:is_200(DResult1),
    erlasticsearch:flush(ServerRef, Index),
    DResult1a = erlasticsearch:count(ServerRef, Index, Type, <<>>, [{q, Query1}]),
    0  = count_from_result(DResult1a),

    % All Indices
    process_t_insert_doc(ServerRef, Config),
    ADResult1 = erlasticsearch:delete_by_query(ServerRef, <<>>, [{q, Query1}]),
    true = erlasticsearch:is_200(ADResult1),
    erlasticsearch:flush(ServerRef, Index),
    ADResult1a = erlasticsearch:count(ServerRef, <<>>, [{q, Query1}]),
    0  = count_from_result(ADResult1a).
    % Don't need to delete docs, 'cos they are already deleted
%    process_t_delete_doc(ServerRef, Config).

t_delete_by_query_doc(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_delete_by_query_doc(Pool, Config),
    process_t_delete_by_query_doc(ClientName, Config).

process_t_delete_by_query_doc(ServerRef, Config) ->
    process_t_insert_doc(ServerRef, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    Query1 = param_query(1),
    Query2 = json_query(1),
    Result1 = erlasticsearch:count(ServerRef, Index, Type, <<>>, [{q, Query1}]),
    5 = count_from_result(Result1),
    DResult1 = erlasticsearch:delete_by_query(ServerRef, Index, Type, Query2, []),
    true = erlasticsearch:is_200(DResult1),
    erlasticsearch:flush(ServerRef, Index),
    DResult1a = erlasticsearch:count(ServerRef, Index, Type, <<>>, [{q, Query1}]),
    0  = count_from_result(DResult1a),
%    process_t_delete_doc(ServerRef, Config).

    % All Indices
    process_t_insert_doc(ServerRef, Config),
    ADResult1 = erlasticsearch:delete_by_query(ServerRef, Query2),
    true = erlasticsearch:is_200(ADResult1),
    erlasticsearch:flush(ServerRef, Index),
    ADResult1a = erlasticsearch:count(ServerRef, <<>>, [{q, Query1}]),
    0  = count_from_result(ADResult1a).
    % Don't need to delete docs, 'cos they are already deleted
%    process_t_delete_doc(ServerRef, Config).


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
    erlasticsearch:join([Key, Value], <<":">>).

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
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_insert_doc(Pool, Config),
    process_t_delete_doc(Pool, Config),
    process_t_insert_doc(ClientName, Config),
    process_t_delete_doc(ClientName, Config).

process_t_insert_doc(ServerRef, Config) ->
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                Response = erlasticsearch:insert_doc(ServerRef, Index, 
                                                     Type, BX, json_document(X)),
                true = erlasticsearch:is_200_or_201(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    erlasticsearch:flush(ServerRef, Index).

t_is_doc(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_is_doc(Pool, Config),
    process_t_is_doc(ClientName, Config).

process_t_is_doc(ServerRef, Config) ->
    process_t_insert_doc(ServerRef, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                true = erlasticsearch:is_doc(ServerRef, Index, Type, BX)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    process_t_delete_doc(ServerRef, Config).

t_get_doc(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_insert_doc(Pool, Config),
    process_t_get_doc(Pool, Config),
    process_t_get_doc(ClientName, Config),
    process_t_delete_doc(ClientName, Config).

process_t_get_doc(ServerRef, Config) ->
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                Response = erlasticsearch:get_doc(ServerRef, Index, Type, BX),
                true = erlasticsearch:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

t_delete_doc(Config) ->
    ClientName = ?config(client_name, Config),
    Pool = ?config(pool, Config),
    process_t_insert_doc(Pool, Config),
    process_t_delete_doc(Pool, Config),
    process_t_insert_doc(ClientName, Config),
    process_t_delete_doc(ClientName, Config).

process_t_delete_doc(ServerRef, Config) ->
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                Response = erlasticsearch:delete_doc(ServerRef, Index, Type, BX),
                true = erlasticsearch:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    erlasticsearch:flush(ServerRef, Index).

%% Test helpers
% Create a bunch-a indices
create_indices(ServerRef, Index) ->
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = erlasticsearch:join([Index, BX], <<"_">>),
                erlasticsearch:create_index(ServerRef, FullIndex),
                erlasticsearch:flush(ServerRef, Index)
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

are_indices_1(ServerRef, Index) ->
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = erlasticsearch:join([Index, BX], <<"_">>),
                true = erlasticsearch:is_index(ServerRef, FullIndex)
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

are_indices_all(ServerRef, Index) ->
    FullIndexList = 
    lists:map(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                erlasticsearch:join([Index, BX], <<"_">>)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    true = erlasticsearch:is_index(ServerRef, FullIndexList).

delete_all_indices(ServerRef, Config) ->
    Index = ?config(index, Config),
    IndexWithShards = erlasticsearch:join([Index, <<"with_shards">>], <<"_">>),
    delete_all_indices(ServerRef, Index, true),
    delete_all_indices(ServerRef, IndexWithShards, true),
    erlasticsearch:flush(ServerRef).

% Optionally check to see if the indices exist before trying to delete
delete_all_indices(ServerRef, Index, CheckIndex) ->
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = erlasticsearch:join([Index, BX], <<"_">>),

                case CheckIndex of
                    true ->
                        case erlasticsearch:is_index(ServerRef, FullIndex) of
                            % Only delete if the index exists
                            true -> 
                                delete_this_index(ServerRef, FullIndex);
                            false ->
                                true
                        end;
                    false -> 
                        % Blindly Delete the indices
                        delete_this_index(ServerRef, FullIndex)
                end
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

delete_this_index(ServerRef, Index) ->
    Response = erlasticsearch:delete_index(ServerRef, Index),
    true = erlasticsearch:is_200(Response).

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
    erlasticsearch:join([BData, BIndex], <<"_">>).

random_name(Name) ->
    random:seed(erlang:now()),
    Id = list_to_binary(integer_to_list(random:uniform(999999999))),
    <<Name/binary, Id/binary>>.

setup_environment() ->
    random:seed(erlang:now()).

setup_lager() ->
    application:start(crypto),
    application:start(compiler),
    application:start(syntax_tools),
    application:start(lager),
    lager:set_loglevel(lager_console_backend, debug),
    lager:set_loglevel(lager_file_backend, "console.log", debug).

start(Config) ->
    ClientName = ?config(client_name, Config),
    PoolName = ?config(pool_name, Config),
    application:start(kernel),
    application:start(stdlib),
    application:start(crypto),
    application:start(compiler),
    application:start(syntax_tools),
    application:start(sasl),
    application:start(jsx),
    application:start(poolboy),
    application:start(erlasticsearch),
    erlasticsearch:start_client(ClientName),
    erlasticsearch:start_pool(PoolName)
    .

stop(Config) ->
    ClientName = ?config(client_name, Config),
    PoolName = ?config(pool_name, Config),
    erlasticsearch:stop_client(ClientName),
    erlasticsearch:stop_pool(PoolName),
    ok.
%    application:stop(jsx),
%    application:stop(sasl),
%    application:stop(syntax_tools),
%    application:stop(compiler),
%    application:stop(crypto),
%    application:stop(stdlib),
%    application:stop(kernel).

