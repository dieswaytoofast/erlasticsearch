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

% TODO: Only import what is needed.
-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").

% TODO: Only export what is needed.
-compile(export_all).

-define(CHECKSPEC(M,F,N), true = proper:check_spec({M,F,N})).
-define(PROPTEST(A), true = proper:quickcheck(A())).

-define(TIMEOUT, 45000).
-define(NUMTESTS, 500).
-define(DOCUMENT_DEPTH, 5).
-define(THREE_SHARDS, <<"{\"settings\":{\"number_of_shards\":3}}">>).
-define(MAPPING_KEY, <<"some_type">>).
-define(MAPPING_VALUE, <<"boolean">>).
-define(MAPPING_DOC(Type), [{Type, [{<<"properties">>, [{?MAPPING_KEY, [{<<"type">>, ?MAPPING_VALUE}]}]}]}]).
-define(ALIASES_DOC(Index, Alias), [{<<"actions">>, [[{<<"add">>, [{<<"index">>, Index}, {<<"alias">>, Alias}]}]]}]).
-define(UPDATE_KEY, <<"udpate_key">>).
-define(UPDATE_VALUE, <<"udpate_value">>).
-define(UPDATE_DOC, [{<<"doc">>, [{?UPDATE_KEY, ?UPDATE_VALUE}]}]).


suite() ->
    [{ct_hooks,[cth_surefire]}, {timetrap,{seconds,320}}].

init_per_suite(Config) ->
    setup_environment(),
    Config.

end_per_suite(_Config) ->
    ok.

pool_name(Name) ->
    binary_to_atom(Name, latin1).

connection_options(1) ->
    [];
connection_options(2) ->
    [{thrift_host, "localhost"}];
connection_options(3) ->
    [{thrift_host, "localhost"},
     {thrift_port, 9500}];
connection_options(4) ->
    [{thrift_host, "localhost"},
     {thrift_port, 9500},
     {thrift_options, [{framed, false}]}];
connection_options(_) ->
    [{thrift_host, "localhost"},
     {thrift_port, 9500},
     {thrift_options, [{framed, false}]},
     {binary_response, false}].

pool_options(1) ->
    [];
pool_options(2) ->
    [{size, 7}];
pool_options(_) ->
    [{size, 7},
     {max_overflow, 14}].

retry_options(1) ->
    [];
retry_options(2) ->
    [{retry_interval, 500}];
retry_options(_) ->
    [{retry_interval, 500},
     {retry_amount, 5}].

update_config(Config) ->
    Version = es_test_version(Config),
    Config1 = lists:foldl(fun(X, Acc) ->
                    proplists:delete(X, Acc)
            end, Config, [es_test_version,
                          index,
                          type,
                          index_with_shards,
                          connection_options,
                          pool_options,
                          pool,
                          retry_options]),
    [{es_test_version, Version + 1} | Config1].

es_test_version(Config) ->
    case proplists:get_value(es_test_version, Config) of
        undefined -> 1;
        Val -> Val
    end.


init_per_group(_GroupName, Config) ->

    Config1 =
    case ?config(saved_config, Config) of
        {_, Config0} -> Config0;
        undefined -> Config
    end,
    Version = es_test_version(Config1),
    PoolName = pool_name(random_name(<<"pool_">>)),
    PoolOptions = pool_options(Version),
    ConnectionOptions = connection_options(Version),
    RetryOptions = retry_options(Version),

    Config2 = [{pool_options, PoolOptions},
               {connection_options, ConnectionOptions ++ RetryOptions},
               {pool, PoolName} | Config1],
    start(Config2),


    Index = random_name(<<"index_">>),
    IndexWithShards = erlasticsearch:join([Index, <<"with_shards">>], <<"_">>),

    Config3 = [{index, Index}, {index_with_shards, IndexWithShards}]
                ++ Config2,
    delete_all_indices(PoolName, Config3),

    Type = random_name(<<"type_">>),

    [{type, Type}] ++ Config3.

end_per_group(_GroupName, Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    IndexWithShards = ?config(index_with_shards, Config),
    delete_all_indices(PoolName, Index),
    delete_all_indices(PoolName, IndexWithShards),
    stop(Config),
    Config1 = update_config(Config),
    {save_config, Config1}.


init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

groups() ->
    [{crud_index, [{repeat, 5}],
       [t_is_index_1,
        t_is_index_all,
        t_is_type_1,
        t_is_type_all,
        t_create_index,
        t_create_index_with_shards,
        t_open_index
      ]},

    {crud_mapping, [{repeat, 5}],
       [t_put_mapping,
        t_get_mapping,
        t_delete_mapping
      ]},

    {aliases, [{repeat, 5}],
       [t_aliases,
        t_insert_alias_1,
        t_insert_alias_2,
        t_delete_alias,
        t_is_alias,
        t_get_alias
      ]},

     {index_helpers, [{repeat, 5}],
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
        t_indices_stats,
        t_status_all,
        t_clear_cache_1,
        t_clear_cache_list,
        t_clear_cache_all

       ]},
    % These three _MUST_ be in this sequence, and by themselves
    {crud_doc, [{repeat, 5}],
      [ t_insert_doc,
       t_get_doc,
       t_update_doc,
       t_delete_doc,
       t_bulk
      ]},
    {doc_helpers, [{repeat, 5}],
       [t_is_doc,
        t_mget_index,
        t_mget_type,
        t_mget_id
      ]},
     {test, [{repeat, 5}],
      [
        t_delete_by_query_param
      ]},
     {cluster_helpers, [{repeat, 5}],
      [t_health,
       t_cluster_state,
       t_state,
       t_nodes_info,
       t_nodes_stats
      ]},

     {search, [{repeat, 5}],
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
        {group, doc_helpers},
        {group, crud_mapping},
        {group, aliases}
    ].

t_health(Config) ->
    PoolName = ?config(pool, Config),
    Response = erlasticsearch:health(PoolName),
    true = erlasticsearch_worker:is_200(Response).

t_cluster_state(Config) ->
    t_state(Config).

t_state(Config) ->
    PoolName = ?config(pool, Config),
    Response1 = erlasticsearch:state(PoolName),
    true = erlasticsearch_worker:is_200(Response1),
    Response2 = erlasticsearch:state(PoolName, [{filter_nodes, true}]),
    true = erlasticsearch_worker:is_200(Response2).

t_nodes_info(Config) ->
    PoolName = ?config(pool, Config),
    Response1 = erlasticsearch:nodes_info(PoolName),
    true = erlasticsearch_worker:is_200(Response1).

t_nodes_stats(Config) ->
    PoolName = ?config(pool, Config),
    Response1 = erlasticsearch:nodes_stats(PoolName),
    true = erlasticsearch_worker:is_200(Response1).

t_indices_stats(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    create_indices(PoolName, Index),
    check_indices_stats(PoolName, Index),
    delete_all_indices(PoolName, Index).

t_status_1(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    create_indices(PoolName, Index),
    check_status_1(PoolName, Index),
    delete_all_indices(PoolName, Index).

t_status_all(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    create_indices(PoolName, Index),
    check_status_all(PoolName, Index),
    delete_all_indices(PoolName, Index).

t_clear_cache_1(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    create_indices(PoolName, Index),
    clear_cache_1(PoolName, Index),
    delete_all_indices(PoolName, Index).

t_clear_cache_list(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    create_indices(PoolName, Index),
    clear_cache_list(PoolName, Index),
    delete_all_indices(PoolName, Index).

t_clear_cache_all(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    create_indices(PoolName, Index),
    clear_cache_all(PoolName, Index),
    delete_all_indices(PoolName, Index).

t_is_index_1(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    create_indices(PoolName, Index),
    are_indices_1(PoolName, Index),
    delete_all_indices(PoolName, Index).

t_is_index_all(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    create_indices(PoolName, Index),
    are_indices_all(PoolName, Index),
    delete_all_indices(PoolName, Index).

t_is_type_1(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    build_data(PoolName, Index, Type),
    are_types_1(PoolName, Index, Type),
    clear_data(PoolName, Index).

t_is_type_all(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    build_data(PoolName, Index, Type),
    are_types_all(PoolName, Index, Type).

check_status_1(PoolName, Index) ->
    lists:foreach(fun(X) ->
                FullIndex = enumerated(Index, X),
                Response = erlasticsearch:status(PoolName, FullIndex),
                true = erlasticsearch_worker:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

check_indices_stats(PoolName, Index) ->
    lists:foreach(fun(X) ->
                FullIndex = enumerated(Index, X),
                Response = erlasticsearch:indices_stats(PoolName, FullIndex),
                true = erlasticsearch_worker:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

check_status_all(PoolName, Index) ->
    FullIndexList =
    lists:map(fun(X) ->
                enumerated(Index, X)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    Response = erlasticsearch:status(PoolName, FullIndexList),
    true = erlasticsearch_worker:is_200(Response).

clear_cache_1(PoolName, Index) ->
    lists:foreach(fun(X) ->
                FullIndex = enumerated(Index, X),
                Response1 = erlasticsearch:clear_cache(PoolName, FullIndex),
                true = erlasticsearch_worker:is_200(Response1),
                Response2 = erlasticsearch:clear_cache(PoolName, FullIndex, [{filter, true}]),
                true = erlasticsearch_worker:is_200(Response2)
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

clear_cache_list(PoolName, Index) ->
    FullIndexList =
    lists:map(fun(X) ->
                enumerated(Index, X)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    Response1 = erlasticsearch:clear_cache(PoolName, FullIndexList),
    true = erlasticsearch_worker:is_200(Response1),
    Response2 = erlasticsearch:clear_cache(PoolName, FullIndexList, [{filter, true}]),
    true = erlasticsearch_worker:is_200(Response2).

clear_cache_all(PoolName, _Index) ->
    Response1 = erlasticsearch:clear_cache(PoolName),
    true = erlasticsearch_worker:is_200(Response1),
    Response2 = erlasticsearch:clear_cache(PoolName, [], [{filter, true}]),
    true = erlasticsearch_worker:is_200(Response2).


are_types_1(PoolName, Index, Type) ->
    lists:foreach(fun(X) ->
                FullIndex = enumerated(Index, X),
                lists:foreach(fun(Y) ->
                            FullType = enumerated(Type, Y),
                            true = true_response(erlasticsearch:is_type(PoolName, FullIndex, FullType))
                    end, lists:seq(1, ?DOCUMENT_DEPTH))
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

are_types_all(PoolName, Index, Type) ->
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
                true = true_response(erlasticsearch:is_type(PoolName, FullIndexList, FullType))
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    % List of types
    lists:foreach(fun(X) ->
                FullIndex = enumerated(Index, X),
                true = true_response(erlasticsearch:is_type(PoolName, FullIndex, FullTypeList))
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    % List of indices and types
    true = true_response(erlasticsearch:is_type(PoolName, FullIndexList, FullTypeList)).

build_data(PoolName, Index, Type) ->
    lists:foreach(fun(X) ->
                FullIndex = enumerated(Index, X),
                lists:foreach(fun(Y) ->
                            FullType = enumerated(Type, Y),
                            BX = list_to_binary(integer_to_list(X)),
                            erlasticsearch:insert_doc(PoolName, FullIndex,
                                                      FullType, BX, json_document(X)),
                            erlasticsearch:flush(PoolName)
                    end, lists:seq(1, ?DOCUMENT_DEPTH))
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

clear_data(PoolName, Index) ->
    lists:foreach(fun(X) ->
                FullIndex = enumerated(Index, X),
                erlasticsearch:delete_index(PoolName, FullIndex)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    erlasticsearch:flush(PoolName).

t_create_index(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    create_indices(PoolName, Index),
    delete_all_indices(PoolName, Index).

t_put_mapping(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    erlasticsearch:create_index(PoolName, Index),
    MappingDoc = jsx:encode(?MAPPING_DOC(Type)),
    Response = erlasticsearch:put_mapping(PoolName, Index, Type, MappingDoc),
    true = erlasticsearch_worker:is_200(Response),
    delete_this_index(PoolName, Index).

t_get_mapping(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    erlasticsearch:create_index(PoolName, Index),
    MappingDoc = jsx:encode(?MAPPING_DOC(Type)),
    Response1 = erlasticsearch:put_mapping(PoolName, Index, Type, MappingDoc),
    true = erlasticsearch_worker:is_200(Response1),
    Response2 = erlasticsearch:get_mapping(PoolName, Index, Type),
    validate_mapping(Index, Type, Response2),
    delete_this_index(PoolName, Index).

validate_mapping(Index, Type, Response) ->
    {body, Data1} = lists:keyfind(body, 1, Response),
    DataB = case is_binary(Data1) of
        true ->
            jsx:decode(Data1);
        false ->
            Data1
    end,
    {Index, DataI} = lists:keyfind(Index, 1, DataB),
    {<<"mappings">>, DataM} = lists:keyfind(<<"mappings">>, 1, DataI),
    {Type, DataT} = lists:keyfind(Type, 1, DataM),
    {<<"properties">>, DataP} = lists:keyfind(<<"properties">>, 1, DataT),
    {?MAPPING_KEY, [{<<"type">>, ?MAPPING_VALUE}]} = lists:keyfind(?MAPPING_KEY, 1, DataP).

t_delete_mapping(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    erlasticsearch:create_index(PoolName, Index),
    MappingDoc = jsx:encode(?MAPPING_DOC(Type)),
    Response1 = erlasticsearch:put_mapping(PoolName, Index, Type, MappingDoc),
    true = erlasticsearch_worker:is_200(Response1),
    Response2 = erlasticsearch:delete_mapping(PoolName, Index, Type),
    true = erlasticsearch_worker:is_200(Response2),
    delete_this_index(PoolName, Index).

t_aliases(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    Alias = random_name(Index),
    erlasticsearch:create_index(PoolName, Index),
    AliasesDoc = ?ALIASES_DOC(Index, Alias),
    Response = erlasticsearch:aliases(PoolName, AliasesDoc),
    true = erlasticsearch_worker:is_200(Response),
    delete_this_index(PoolName, Index).

t_insert_alias_1(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    Alias = random_name(Index),
    erlasticsearch:create_index(PoolName, Index),
    Response = erlasticsearch:insert_alias(PoolName, Index, Alias),
    true = erlasticsearch_worker:is_200(Response),
    delete_this_index(PoolName, Index).

t_insert_alias_2(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    Alias = random_name(Index),
    erlasticsearch:create_index(PoolName, Index),
    Params = [{<<"routing">>, <<"1">>}],
    Response = erlasticsearch:insert_alias(PoolName, Index, Alias, Params),
    true = erlasticsearch_worker:is_200(Response),
    delete_this_index(PoolName, Index).

t_delete_alias(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    Alias = random_name(Index),
    erlasticsearch:create_index(PoolName, Index),
    Response1 = erlasticsearch:insert_alias(PoolName, Index, Alias),
    true = erlasticsearch_worker:is_200(Response1),
    Response2 = erlasticsearch:delete_alias(PoolName, Index, Alias),
    true = erlasticsearch_worker:is_200(Response2),
    delete_this_index(PoolName, Index).

t_is_alias(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    Alias = random_name(Index),
    erlasticsearch:create_index(PoolName, Index),
    Response1 = erlasticsearch:insert_alias(PoolName, Index, Alias),
    true = erlasticsearch_worker:is_200(Response1),
    true = true_response(erlasticsearch:is_alias(PoolName, Index, Alias)),
    delete_this_index(PoolName, Index).

t_get_alias(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    Alias = random_name(Index),
    erlasticsearch:create_index(PoolName, Index),
    Response1 = erlasticsearch:insert_alias(PoolName, Index, Alias),
    true = erlasticsearch_worker:is_200(Response1),
    Response2 = erlasticsearch:get_alias(PoolName, Index, Alias),
    validate_alias(Index, Alias, Response2),
    delete_this_index(PoolName, Index).

validate_alias(Index, Alias, Response) ->
    {body, Data1} = lists:keyfind(body, 1, Response),
    Data2 = case is_binary(Data1) of
        true ->
            jsx:decode(Data1);
        false ->
            Data1
    end,
    {Index, Data3} = lists:keyfind(Index, 1, Data2),
    {<<"aliases">>, Data4} = lists:keyfind(<<"aliases">>, 1, Data3),
    {Alias, _} = lists:keyfind(Alias, 1, Data4).

t_create_index_with_shards(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index_with_shards, Config),
    create_indices(PoolName, Index),
    delete_all_indices(PoolName, Index).

t_flush_1(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    create_indices(PoolName, Index),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = erlasticsearch:join([Index, BX], <<"_">>),
                Response = erlasticsearch:flush(PoolName, FullIndex),
                true = erlasticsearch_worker:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    delete_all_indices(PoolName, Index).

t_flush_list(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    create_indices(PoolName, Index),
    Indexes =
    lists:map(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                erlasticsearch:join([Index, BX], <<"_">>)
            end, lists:seq(1, ?DOCUMENT_DEPTH)),
    Response = erlasticsearch:flush(PoolName, Indexes),
    true = erlasticsearch_worker:is_200(Response),
    delete_all_indices(PoolName, Index).

t_flush_all(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    create_indices(PoolName, Index),
    Response = erlasticsearch:flush(PoolName),
    true = erlasticsearch_worker:is_200(Response),
    delete_all_indices(PoolName, Index).

t_refresh_1(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    create_indices(PoolName, Index),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = erlasticsearch:join([Index, BX], <<"_">>),
                Response = erlasticsearch:refresh(PoolName, FullIndex),
                true = erlasticsearch_worker:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    delete_all_indices(PoolName, Index).

t_refresh_list(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    create_indices(PoolName, Index),
    Indexes =
    lists:map(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                erlasticsearch:join([Index, BX], <<"_">>)
            end, lists:seq(1, ?DOCUMENT_DEPTH)),
    Response = erlasticsearch:refresh(PoolName, Indexes),
    true = erlasticsearch_worker:is_200(Response),
    delete_all_indices(PoolName, Index).

t_refresh_all(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    create_indices(PoolName, Index),
    Response = erlasticsearch:refresh(PoolName),
    true = erlasticsearch_worker:is_200(Response),
    delete_all_indices(PoolName, Index).

t_optimize_1(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    create_indices(PoolName, Index),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = erlasticsearch:join([Index, BX], <<"_">>),
                Response = erlasticsearch:optimize(PoolName, FullIndex),
                true = erlasticsearch_worker:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    delete_all_indices(PoolName, Index).

t_optimize_list(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    create_indices(PoolName, Index),
    Indexes =
    lists:map(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                erlasticsearch:join([Index, BX], <<"_">>)
            end, lists:seq(1, ?DOCUMENT_DEPTH)),
    Response = erlasticsearch:optimize(PoolName, Indexes),
    true = erlasticsearch_worker:is_200(Response),
    delete_all_indices(PoolName, Index).

t_optimize_all(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    create_indices(PoolName, Index),
    Response = erlasticsearch:optimize(PoolName),
    true = erlasticsearch_worker:is_200(Response),
    delete_all_indices(PoolName, Index).

t_segments_1(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    create_indices(PoolName, Index),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = erlasticsearch:join([Index, BX], <<"_">>),
                Response = erlasticsearch:segments(PoolName, FullIndex),
                true = erlasticsearch_worker:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    delete_all_indices(PoolName, Index).

t_segments_list(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    create_indices(PoolName, Index),
    Indexes =
    lists:map(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                erlasticsearch:join([Index, BX], <<"_">>)
            end, lists:seq(1, ?DOCUMENT_DEPTH)),
    Response = erlasticsearch:segments(PoolName, Indexes),
    true = erlasticsearch_worker:is_200(Response),
    delete_all_indices(PoolName, Index).

t_segments_all(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    create_indices(PoolName, Index),
    Response = erlasticsearch:segments(PoolName),
    true = erlasticsearch_worker:is_200(Response),
    delete_all_indices(PoolName, Index).

t_open_index(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    process_t_insert_doc(PoolName, Config),
    Response = erlasticsearch:close_index(PoolName, Index),
    true = erlasticsearch_worker:is_200(Response),
    Response1 = erlasticsearch:open_index(PoolName, Index),
    true = erlasticsearch_worker:is_200(Response1),
    process_t_delete_doc(PoolName, Config).


t_mget_id(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    Query = id_query(),
    process_t_insert_doc(PoolName, Config),
    Result = erlasticsearch:mget_doc(PoolName, Index, Type, Query),
    ?DOCUMENT_DEPTH  = docs_from_result(Result),
    process_t_delete_doc(PoolName, Config).

t_mget_type(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    Query = id_query(Type),
    process_t_insert_doc(PoolName, Config),
    Result = erlasticsearch:mget_doc(PoolName, Index, Query),
    ?DOCUMENT_DEPTH  = docs_from_result(Result),
    process_t_delete_doc(PoolName, Config).

t_mget_index(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    Query = id_query(Index, Type),
    process_t_insert_doc(PoolName, Config),
    Result = erlasticsearch:mget_doc(PoolName, Query),
    ?DOCUMENT_DEPTH  = docs_from_result(Result),
    process_t_delete_doc(PoolName, Config).

t_search(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    process_t_insert_doc(PoolName, Config),
    lists:foreach(fun(X) ->
                Query = param_query(X),
                Result = erlasticsearch:search(PoolName, Index, Type, <<>>, [{q, Query}]),
                % The document is structured so that the number of top level
                % keys is as (?DOCUMENT_DEPTH + 1 - X)
                ?DOCUMENT_DEPTH  = hits_from_result(Result) + X - 1
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    process_t_delete_doc(PoolName, Config).

t_count(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    process_t_insert_doc(PoolName, Config),
    lists:foreach(fun(X) ->
                Query1 = param_query(X),
                Query2 = json_query(X),

                % query as parameter
                Result1 = erlasticsearch:count(PoolName, Index, Type, <<>>, [{q, Query1}]),
                % The document is structured so that the number of top level
                % keys is as (?DOCUMENT_DEPTH + 1 - X)
                ?DOCUMENT_DEPTH  = count_from_result(Result1) + X - 1,

                % query as doc
                Result2 = erlasticsearch:count(PoolName, Index, Type, Query2, []),
                % The document is structured so that the number of top level
                % keys is as (?DOCUMENT_DEPTH + 1 - X)
                ?DOCUMENT_DEPTH  = count_from_result(Result2) + X - 1

        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    process_t_delete_doc(PoolName, Config).

t_delete_by_query_param(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    Query1 = param_query(1),
    process_t_insert_doc(PoolName, Config),
    Result1 = erlasticsearch:count(PoolName, Index, Type, <<>>, [{q, Query1}]),
    5 = count_from_result(Result1),
    DResult1 = erlasticsearch:delete_by_query(PoolName, Index, Type, <<>>, [{q, Query1}]),
    true = erlasticsearch_worker:is_200(DResult1),
    erlasticsearch:flush(PoolName, Index),
    DResult1a = erlasticsearch:count(PoolName, Index, Type, <<>>, [{q, Query1}]),
    0  = count_from_result(DResult1a),

    process_t_insert_doc(PoolName, Config),
    ADResult1 = erlasticsearch:delete_by_query(PoolName, <<>>, [{q, Query1}]),
    true = erlasticsearch_worker:is_200(ADResult1),
    erlasticsearch:flush(PoolName, Index),
    ADResult1a = erlasticsearch:count(PoolName, <<>>, [{q, Query1}]),
    0  = count_from_result(ADResult1a).

t_delete_by_query_doc(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    Query1 = param_query(1),
    Query2 = json_query(1),
    process_t_insert_doc(PoolName, Config),
    Result1 = erlasticsearch:count(PoolName, Index, Type, <<>>, [{q, Query1}]),
    5 = count_from_result(Result1),
    DResult1 = erlasticsearch:delete_by_query(PoolName, Index, Type, Query2, []),
    true = erlasticsearch_worker:is_200(DResult1),
    erlasticsearch:flush(PoolName, Index),
    DResult1a = erlasticsearch:count(PoolName, Index, Type, <<>>, [{q, Query1}]),
    0  = count_from_result(DResult1a),

    process_t_insert_doc(PoolName, Config),
    ADResult1 = erlasticsearch:delete_by_query(PoolName, Query2),
    true = erlasticsearch_worker:is_200(ADResult1),
    erlasticsearch:flush(PoolName, Index),
    ADResult1a = erlasticsearch:count(PoolName, <<>>, [{q, Query1}]),
    0  = count_from_result(ADResult1a).


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
    jsx:encode([{query, [{term, [{Key, Value}]}]}]).

decode_body(Body) when is_binary(Body) ->
    jsx:decode(Body);
decode_body(Body) -> Body.

hits_from_result(Result) ->
    {body, Body} = lists:keyfind(body, 1, Result),
    DBody = decode_body(Body),
    case lists:keyfind(<<"hits">>, 1, DBody) of
        false -> throw(false);
        {_, Value} ->
            case lists:keyfind(<<"total">>, 1, Value) of
                false -> throw(false);
                {_, Data} -> Data
            end
    end.

docs_from_result(Result) ->
    {body, Body} = lists:keyfind(body, 1, Result),
    DBody = decode_body(Body),
    case lists:keyfind(<<"docs">>, 1, DBody) of
        false -> throw(false);
        {_, Value} ->
            length(Value)
    end.


count_from_result(Result) ->
    {body, Body} = lists:keyfind(body, 1, Result),
    DBody = decode_body(Body),
    case lists:keyfind(<<"count">>, 1, DBody) of
        false -> throw(false);
        {_, Value} -> Value
    end.

t_bulk(Config) ->
    PoolName = ?config(pool, Config),
    process_t_bulk(PoolName, Config).

t_insert_doc(Config) ->
    PoolName = ?config(pool, Config),
    process_t_insert_doc(PoolName, Config),
    process_t_delete_doc(PoolName, Config).

t_is_doc(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    process_t_insert_doc(PoolName, Config),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                true = true_response(erlasticsearch:is_doc(PoolName, Index, Type, BX))
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    process_t_delete_doc(PoolName, Config).

t_get_doc(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    process_t_insert_doc(PoolName, Config),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                Response = erlasticsearch:get_doc(PoolName, Index, Type, BX),
                true = erlasticsearch_worker:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    process_t_delete_doc(PoolName, Config).

t_update_doc(Config) ->
    PoolName = ?config(pool, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    process_t_insert_doc(PoolName, Config),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                Doc = ?UPDATE_DOC,
                Response1 = erlasticsearch:update_doc(PoolName, Index, Type, BX, Doc),
                true = erlasticsearch_worker:is_200(Response1),
                Response2 = erlasticsearch:get_doc(PoolName, Index, Type, BX),
                validate_update(Response2)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    process_t_delete_doc(PoolName, Config).

validate_update(Response) ->
    {body, Data1} = lists:keyfind(body, 1, Response),
    Data2 = case is_binary(Data1) of
        true ->
            jsx:decode(Data1);
        false ->
            Data1
    end,
    {_, Data3} = lists:keyfind(<<"_source">>, 1, Data2),
    {?UPDATE_KEY, ?UPDATE_VALUE} = lists:keyfind(?UPDATE_KEY, 1, Data3).

t_delete_doc(Config) ->
    PoolName = ?config(pool, Config),
    process_t_insert_doc(PoolName, Config),
    process_t_delete_doc(PoolName, Config).

process_t_insert_doc(PoolName, Config) ->
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                Response = erlasticsearch:insert_doc(PoolName, Index,
                                                     Type, BX, json_document(X)),
                true = erlasticsearch_worker:is_200_or_201(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    erlasticsearch:flush(PoolName, Index).

process_t_bulk(PoolName, Config) ->
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    lists:foreach(fun(X) ->
                Response1 = erlasticsearch:bulk(PoolName, <<>>,
                                                <<>>, bulk_document(<<>>, <<>>, X)),
                true = erlasticsearch_worker:is_200_or_201(Response1),
                Response2 = erlasticsearch:bulk(PoolName, Index,
                                                Type, bulk_document(Index, <<>>, X)),
                true = erlasticsearch_worker:is_200_or_201(Response2),
                Response3 = erlasticsearch:bulk(PoolName, Index,
                                               Type, bulk_document(Index, Type, X)),
                true = erlasticsearch_worker:is_200_or_201(Response3)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    erlasticsearch:flush(PoolName, Index).

process_t_delete_doc(PoolName, Config) ->
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                Response = erlasticsearch:delete_doc(PoolName, Index, Type, BX),
                true = erlasticsearch_worker:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    erlasticsearch:flush(PoolName, Index).

create_indices(PoolName, Index) ->
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = erlasticsearch:join([Index, BX], <<"_">>),
                erlasticsearch:create_index(PoolName, FullIndex),
                erlasticsearch:flush(PoolName, Index)
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

are_indices_1(PoolName, Index) ->
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = erlasticsearch:join([Index, BX], <<"_">>),
                true = true_response(erlasticsearch:is_index(PoolName, FullIndex))
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

are_indices_all(PoolName, Index) ->
    FullIndexList =
    lists:map(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                erlasticsearch:join([Index, BX], <<"_">>)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    true = true_response(erlasticsearch:is_index(PoolName, FullIndexList)).

delete_all_indices(PoolName, Config) when is_list(Config) ->
    Index = ?config(index, Config),
    IndexWithShards = erlasticsearch:join([Index, <<"with_shards">>], <<"_">>),
    delete_all_indices(PoolName, Index),
    delete_all_indices(PoolName, IndexWithShards),
    erlasticsearch:flush(PoolName);

delete_all_indices(PoolName, Index) when is_binary(Index) ->
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = erlasticsearch:join([Index, BX], <<"_">>),
                delete_this_index(PoolName, FullIndex)
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

delete_this_index(PoolName, Index) ->
    case true_response(erlasticsearch:is_index(PoolName, Index)) of
        true ->
            Response = erlasticsearch:delete_index(PoolName, Index),
            true = erlasticsearch_worker:is_200(Response);
        false -> true
    end.

json_document(N) ->
    jsx:encode(document(N)).

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

bulk_document(Index, Type, N) ->
    case N of
        1 -> bulk_line(Index, Type, 1);
        X -> erlasticsearch:join([bulk_line(Index, Type, X), bulk_document(Index, Type, X-1)], <<>>)
    end.

bulk_line(<<>>, <<>>, N) ->
    Index = index(N),
    Type = type(N),
    Id = id(N),
    Key = key(N),
    Value = value(N),
    <<"{ \"index\" : { \"_index\" : \"", Index/binary, "\", \"_type\" : \"", Type/binary, "\", \"_id\" : \"", Id/binary, "\" } }\n{ \"", Key/binary, "\" : \"", Value/binary, "\" }\n">>;
% Pre-existing Index
bulk_line(_Index, <<>>, N) ->
    Type = type(N),
    Id = id(N),
    Key = key(N),
    Value = value(N),
    <<"{ \"index\" : { \"_type\" : \"", Type/binary, "\", \"_id\" : \"", Id/binary, "\" } }\n{ \"", Key/binary, "\" : \"", Value/binary, "\" }\n">>;
% Pre-existing Index and Type
bulk_line(_Index, _Type, N) ->
    Id = id(N),
    Key = key(N),
    Value = value(N),
    <<"{ \"index\" : { \"_id\" : \"", Id/binary, "\" } }\n{ \"", Key/binary, "\" : \"", Value/binary, "\" }\n">>.

index(N) -> data_index(index, N).
type(N) -> data_index(type, N).
id(N) -> data_index(id, N).
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

true_response(Response) ->
    case lists:keyfind(result, 1, Response) of
        {result, true} -> true;
        {result, <<"true">>} -> true;
        _ -> false
    end.

setup_environment() ->
    random:seed(erlang:now()).

setup_lager() ->
    reltool_util:application_start(lager),
    lager:set_loglevel(lager_console_backend, debug),
    lager:set_loglevel(lager_file_backend, "console.log", debug).

start(Config) ->
    PoolName = ?config(pool, Config),
    ConnectionOptions = ?config(connection_options, Config),
    PoolOptions = ?config(pool_options, Config),
    reltool_util:application_start(jsx),
    reltool_util:application_start(erlasticsearch),
    % Increase timeout for local testing of elasticsearch
    application:set_env(erlasticsearch, worker_timeout, ?TIMEOUT),
    erlasticsearch:start_pool(PoolName, PoolOptions, ConnectionOptions).

stop(Config) ->
    PoolName = ?config(pool, Config),
    erlasticsearch:stop_pool(PoolName),
    reltool_util:application_stop(erlasticsearch),
    reltool_util:application_stop(jsx),
    ok.
