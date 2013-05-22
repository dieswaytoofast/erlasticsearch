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
-module(erlasticsearch_poolboy).
-author('Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>').

-include("erlasticsearch.hrl").

%% ElasticSearch
% Tests
-export([is_index/2]).
-export([is_type/3]).
-export([is_doc/4]).

% Cluster helpers
-export([health/1]).
-export([state/1, state/2]).
-export([nodes_info/1, nodes_info/2, nodes_info/3]).
-export([nodes_stats/1, nodes_stats/2, nodes_stats/3]).

% Index CRUD
-export([create_index/2, create_index/3]).
-export([delete_index/1, delete_index/2]).
-export([open_index/2]).
-export([close_index/2]).

% Doc CRUD
-export([insert_doc/5, insert_doc/6]).
-export([get_doc/4, get_doc/5]).
-export([mget_doc/2, mget_doc/3, mget_doc/4]).
-export([delete_doc/4, delete_doc/5]).
-export([search/4, search/5]).
-export([count/2, count/3, count/4, count/5]).
-export([delete_by_query/2, delete_by_query/3, delete_by_query/4, delete_by_query/5]).

%% Index helpers
-export([status/2]).
-export([refresh/1, refresh/2]).
-export([flush/1, flush/2]).
-export([optimize/1, optimize/2]).
-export([clear_cache/1, clear_cache/2, clear_cache/3]).
-export([segments/1, segments/2]).

-export([is_200/1, is_200_or_201/1]).

-define(APP, ?MODULE).

%% @doc Get the health the  ElasticSearch cluster
-spec health(atom()) -> response().
health(PoolName) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {health}, infinity)
    end).

%% @equiv state(PoolName, []).
-spec state(server_ref()) -> response().
state(PoolName) ->
    state(PoolName, []).

%% @doc Get the state of the  ElasticSearch cluster
-spec state(atom(), params()) -> response().
state(PoolName, Params) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {state, Params}, infinity)
    end).

%% @equiv nodes_info(PoolName, [], []).
-spec nodes_info(atom()) -> response().
nodes_info(PoolName) ->
    nodes_info(PoolName, [], []).

%% @equiv nodes_info(PoolName, [NodeName], []).
-spec nodes_info(atom(), node_name()) -> response().
nodes_info(PoolName, NodeName) when is_binary(NodeName) ->
    nodes_info(PoolName, [NodeName], []);
%% @equiv nodes_info(PoolName, NodeNames, []).
nodes_info(PoolName, NodeNames) when is_list(NodeNames) ->
    nodes_info(PoolName, NodeNames, []).

%% @doc Get the nodes_info of the  ElasticSearch cluster
-spec nodes_info(atom(), [node_name()], params()) -> response().
nodes_info(PoolName, NodeNames, Params) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {nodes_info, NodeNames, Params}, infinity)
    end).

%% @equiv nodes_stats(PoolName, [], []).
-spec nodes_stats(atom()) -> response().
nodes_stats(PoolName) ->
    nodes_stats(PoolName, [], []).

%% @equiv nodes_stats(PoolName, [NodeName], []).
-spec nodes_stats(atom(), node_name()) -> response().
nodes_stats(PoolName, NodeName) when is_binary(NodeName) ->
    nodes_stats(PoolName, [NodeName], []);
%% @equiv nodes_stats(PoolName, NodeNames, []).
nodes_stats(PoolName, NodeNames) when is_list(NodeNames) ->
    nodes_stats(PoolName, NodeNames, []).

%% @doc Get the nodes_stats of the  ElasticSearch cluster
-spec nodes_stats(atom(), [node_name()], params()) -> response().
nodes_stats(PoolName, NodeNames, Params) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {nodes_stats, NodeNames, Params}, infinity)
    end).

%% @doc Get the status of an index/indices in the  ElasticSearch cluster
-spec status(atom(), index() | [index()]) -> response().
status(PoolName, Index) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {status, Index}, infinity)
    end).

%% @equiv create_index(PoolName, Index, <<>>)
-spec create_index(atom(), index()) -> response().
create_index(PoolName, Index) ->
    create_index(PoolName, Index, <<>>).

%% @doc Create an index in the ElasticSearch cluster
-spec create_index(atom(), index(), doc()) -> response().
create_index(PoolName, Index, Doc) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {create_index, Index, Doc}, infinity)
    end).

%% @doc Delete all the  indices in the ElasticSearch cluster
-spec delete_index(atom()) -> response().
delete_index(PoolName) ->
    delete_index(PoolName, ?ALL).

%% @doc Delete an index in the ElasticSearch cluster
-spec delete_index(atom(), [index() | [index()]]) -> response().
delete_index(PoolName, Index) when is_binary(Index) ->
    delete_index(PoolName, [Index]);
delete_index(PoolName, Index) when is_list(Index) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {delete_index, Index}, infinity)
    end).

%% @doc Open an index in the ElasticSearch cluster
-spec open_index(atom(), index()) -> response().
open_index(PoolName, Index) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {open_index, Index}, infinity)
    end).

%% @doc Close an index in the ElasticSearch cluster
-spec close_index(atom(), index()) -> response().
close_index(PoolName, Index) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {close_index, Index}, infinity)
    end).

%% @doc Check if an index/indices exists in the ElasticSearch cluster
-spec is_index(atom(), index() | [index()]) -> boolean().
is_index(PoolName, Index) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {is_index, Index}, infinity)
    end).

%% @doc Check if a type exists in an index/indices in the ElasticSearch cluster
-spec is_type(atom(), index() | [index()], [type() | [type()]]) -> boolean().
is_type(PoolName, Index, Type) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {is_type, Index, Type}, infinity)
    end).

%% @doc Checks to see if the doc exists
-spec is_doc(atom(), index(), type(), id()) -> response().
is_doc(PoolName, Index, Type, Id) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {is_doc, Index, Type, Id}, infinity)
    end).

%% @equiv insert_doc(Index, Type, Id, Doc, []).
-spec insert_doc(atom(), index(), type(), id(), doc()) -> response().
insert_doc(PoolName, Index, Type, Id, Doc) ->
    insert_doc(PoolName, Index, Type, Id, Doc, []).

%% @doc Insert a doc into the ElasticSearch cluster
-spec insert_doc(atom(), index(), type(), id(), doc(), params()) -> response().
insert_doc(PoolName, Index, Type, Id, Doc, Params) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {insert_doc, Index, Type, Id, Doc, Params}, infinity)
    end).

%% @equiv get_doc(PoolName, Index, Type, Id, []).
-spec get_doc(atom(), index(), type(), id()) -> response().
get_doc(PoolName, Index, Type, Id) ->
    get_doc(PoolName, Index, Type, Id, []).

%% @doc Get a doc from the ElasticSearch cluster
-spec get_doc(atom(), index(), type(), id(), params()) -> response().
get_doc(PoolName, Index, Type, Id, Params) ->
    poolboy:transaction(PoolName, fun(Worker) ->
                gen_server:call(Worker, {get_doc, Index, Type, Id, Params}, infinity)
        end).

%% @equiv mget_doc(PoolName, <<>>, <<>>, Doc)
-spec mget_doc(atom(), doc()) -> response().
mget_doc(PoolName, Doc) ->
    mget_doc(PoolName, <<>>, <<>>, Doc).

%% @equiv mget_doc(PoolName, Index, <<>>, Doc)
-spec mget_doc(atom(), index(), doc()) -> response().
mget_doc(PoolName, Index, Doc) ->
    mget_doc(PoolName, Index, <<>>, Doc).

%% @doc Get a doc from the ElasticSearch cluster
-spec mget_doc(atom(), index(), type(), doc()) -> response().
mget_doc(PoolName, Index, Type, Doc) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {mget_doc, Index, Type, Doc}, infinity)
    end).

%% @equiv delete_doc(PoolName, Index, Type, Id, []).
-spec delete_doc(atom(), index(), type(), id()) -> response().
delete_doc(PoolName, Index, Type, Id) ->
    delete_doc(PoolName, Index, Type, Id, []).
%% @doc Delete a doc from the ElasticSearch cluster
-spec delete_doc(atom(), index(), type(), id(), params()) -> response().
delete_doc(PoolName, Index, Type, Id, Params) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {delete_doc, Index, Type, Id, Params}, infinity)
    end).

%% @equiv search(PoolName, Index, Type, Doc, []).
-spec search(atom(), index(), type(), doc()) -> response().
search(PoolName, Index, Type, Doc) ->
    search(PoolName, Index, Type, Doc, []).
%% @doc Search for docs in the ElasticSearch cluster
-spec search(atom(), index(), type(), doc(), params()) -> response().
search(PoolName, Index, Type, Doc, Params) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {search, Index, Type, Doc, Params}, infinity)
    end).

%% @equiv count(PoolName, ?ALL, [], Doc []).
-spec count(atom(), doc()) -> boolean().
count(PoolName, Doc) when is_binary(Doc) ->
    count(PoolName, ?ALL, [], Doc, []).

%% @equiv count(PoolName, ?ALL, [], Doc, Params).
-spec count(atom(), doc(), params()) -> boolean().
count(PoolName, Doc, Params) when is_binary(Doc), is_list(Params) ->
    count(PoolName, ?ALL, [], Doc, Params).

%% @equiv count(PoolName, Index, [], Doc, Params).
-spec count(atom(), index() | [index()], doc(), params()) -> boolean().
count(PoolName, Index, Doc, Params) when is_binary(Index), is_binary(Doc), is_list(Params) ->
    count(PoolName, [Index], [], Doc, Params);
count(PoolName, Indexes, Doc, Params) when is_list(Indexes), is_binary(Doc), is_list(Params) ->
    count(PoolName, Indexes, [], Doc, Params).

%% @doc Get the number of matches for a query
-spec count(atom(), index() | [index()], type() | [type()], doc(), params()) -> boolean().
count(PoolName, Index, Type, Doc, Params) when is_binary(Index), is_binary(Type), is_binary(Doc), is_list(Params) ->
    count(PoolName, [Index], [Type], Doc, Params);
count(PoolName, Indexes, Type, Doc, Params) when is_list(Indexes), is_binary(Type), is_binary(Doc), is_list(Params) ->
    count(PoolName, Indexes, [Type], Doc, Params);
count(PoolName, Index, Types, Doc, Params) when is_binary(Index), is_list(Types), is_binary(Doc), is_list(Params) ->
    count(PoolName, [Index], Types, Doc, Params);
count(PoolName, Indexes, Types, Doc, Params) when is_list(Indexes), is_list(Types), is_binary(Doc), is_list(Params) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {count, Indexes, Types, Doc, Params}, infinity)
    end).

%% @equiv delete_by_query(PoolName, ?ALL, [], Doc []).
-spec delete_by_query(atom(), doc()) -> boolean().
delete_by_query(PoolName, Doc) when is_binary(Doc) ->
    delete_by_query(PoolName, ?ALL, [], Doc, []).

%% @equiv delete_by_query(PoolName, ?ALL, [], Doc, Params).
-spec delete_by_query(atom(), doc(), params()) -> boolean().
delete_by_query(PoolName, Doc, Params) when is_binary(Doc), is_list(Params) ->
    delete_by_query(PoolName, ?ALL, [], Doc, Params).

%% @equiv delete_by_query(PoolName, Index, [], Doc, Params).
-spec delete_by_query(atom(), index() | [index()], doc(), params()) -> boolean().
delete_by_query(PoolName, Index, Doc, Params) when is_binary(Index), is_binary(Doc), is_list(Params) ->
    delete_by_query(PoolName, [Index], [], Doc, Params);
delete_by_query(PoolName, Indexes, Doc, Params) when is_list(Indexes), is_binary(Doc), is_list(Params) ->
    delete_by_query(PoolName, Indexes, [], Doc, Params).

%% @doc Get the number of matches for a query
-spec delete_by_query(atom(), index() | [index()], type() | [type()], doc(), params()) -> boolean().
delete_by_query(PoolName, Index, Type, Doc, Params) when is_binary(Index), is_binary(Type), is_binary(Doc), is_list(Params) ->
    delete_by_query(PoolName, [Index], [Type], Doc, Params);
delete_by_query(PoolName, Indexes, Type, Doc, Params) when is_list(Indexes), is_binary(Type), is_binary(Doc), is_list(Params) ->
    delete_by_query(PoolName, Indexes, [Type], Doc, Params);
delete_by_query(PoolName, Index, Types, Doc, Params) when is_binary(Index), is_list(Types), is_binary(Doc), is_list(Params) ->
    delete_by_query(PoolName, [Index], Types, Doc, Params);
delete_by_query(PoolName, Indexes, Types, Doc, Params) when is_list(Indexes), is_list(Types), is_binary(Doc), is_list(Params) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {delete_by_query, Indexes, Types, Doc, Params}, infinity)
    end).

%% @doc Refresh all indices
%-spec refresh(atom()) -> response().
refresh(PoolName) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {refresh}, infinity)
    end).

%% @doc Refresh one or more indices
%-spec refresh(atom(), index() | [index()]) -> response().
refresh(PoolName, Index) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {refresh, Index}, infinity)
    end).

%% @doc Flush all indices
-spec flush(atom()) -> response().
flush(PoolName) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {flush}, infinity)
    end).

%% @doc Flush one or more indices
-spec flush(atom(), index() | [index()]) -> response().
flush(PoolName, Index) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {flush, Index}, infinity)
    end).

%% @doc Optimize all indices
-spec optimize(atom()) -> response().
optimize(PoolName) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {optimize}, infinity)
    end).

%% @doc Optimize one or more indices
-spec optimize(atom(), index() | [index()]) -> response().
optimize(PoolName, Index) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {optimize, Index}, infinity)
    end).

%% @equiv segments(PoolName, ?ALL).
%% @doc Optimize all indices
-spec segments(atom()) -> response().
segments(PoolName) ->
    segments(PoolName, ?ALL).

%% @doc Optimize one or more indices
-spec segments(atom(), index() | [index()]) -> response().
segments(PoolName, Index) when is_binary(Index) ->
    segments(PoolName, [Index]);
segments(PoolName, Indexes) when is_list(Indexes) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {segments, Indexes}, infinity)
    end).

%% @equiv clear_cache(PoolName, ?ALL, []).
%% @doc Clear all the caches
-spec clear_cache(atom()) -> response().
clear_cache(PoolName) ->
    clear_cache(PoolName, ?ALL, []).

%% @equiv clear_cache(PoolName, Indexes, []).
-spec clear_cache(atom(), index() | [index()]) -> response().
clear_cache(PoolName, Index) when is_binary(Index) ->
    clear_cache(PoolName, [Index], []);
clear_cache(PoolName, Indexes) when is_list(Indexes) ->
    clear_cache(PoolName, Indexes, []).

%% @equiv clear_cache(PoolName, Indexes, []).
-spec clear_cache(atom(), index() | [index()], params()) -> response().
clear_cache(PoolName, Index, Params) when is_binary(Index), is_list(Params) ->
    clear_cache(PoolName, [Index], Params);
clear_cache(PoolName, Indexes, Params) when is_list(Indexes), is_list(Params) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {clear_cache, Indexes, Params}, infinity)
    end).

-spec is_200(response()) -> boolean().
is_200(X) ->
    erlasticsearch:is_200(X).

-spec is_200_or_201(response()) -> boolean().
is_200_or_201(X) ->
    erlasticsearch:is_200_or_201(X).
