%%%-------------------------------------------------------------------
%%% @author Juan Jose Comellas <juanjo@comellas.org>
%%% @author Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>
%%% @copyright (C) 2011-2012 Juan Jose Comellas, Mahesh Paolini-Subramanya
%%% @doc Erlastic_search type and record definitions
%%% @end
%%%
%%% This source file is subject to the New BSD License. You should have received
%%% a copy of the New BSD license with this software. If not, it can be
%%% retrieved from: http://www.opensource.org/licenses/bsd-license.php
%%%-------------------------------------------------------------------

-include("elasticsearch_types.hrl").


%% NOTE: The following two definitions, error() and exception(), are
%% contextually meaningless if there's no specific information or unifying,
%% generic operation(s) defined.
-type error()           :: {error, Reason :: any()}.
-type exception()       :: {exception, Reason :: any()}.

-type method()          :: atom().
-type rest_response()   :: #restResponse{}.
-type response()        :: [tuple()] | error().
-type rest_request()    :: #restRequest{}.
-type connection()      :: any().
-type node_name()       :: binary().
-type index()           :: binary().
-type type()            :: binary().
-type id()              :: binary() | undefined.
-type doc()             :: binary() | list().
-type params()          :: [tuple()].
-type pool_name()       :: atom().


%% Defaults
%%% PoolBoy
-define(DEFAULT_WORKER_TIMEOUT, 5000).
-define(DEFAULT_POOL_NAME, <<"default_erlasticsearch_pool">>).
-define(DEFAULT_POOL_OPTIONS, [{size, 5},
                               {max_overflow, 10}
                              ]).

%%% Thrift
-define(DEFAULT_THRIFT_HOST, "localhost").
-define(DEFAULT_THRIFT_PORT, 9500).
-define(DEFAULT_CONNECTION_REFRESH_INTERVAL, 60000).
-define(DEFAULT_CONNECTION_OPTIONS, [{thrift_host, ?DEFAULT_THRIFT_HOST},
                                     {thrift_port, ?DEFAULT_THRIFT_PORT},
                                     {binary_response, true},
                                     {connection_refresh_interval, ?DEFAULT_CONNECTION_REFRESH_INTERVAL}
                                    ]).

%% Errors
-define(NO_SUCH_SEQUENCE, no_such_sequence).

%% Methods
-define(STATE, <<"_cluster/state">>).
-define(HEALTH, <<"_cluster/health">>).
-define(NODES, <<"_nodes">>).
-define(STATUS, <<"_status">>).
-define(STATS, <<"stats">>).
-define(INDICES_STATS, <<"_stats">>).
-define(SEARCH, <<"_search">>).
-define(REFRESH, <<"_refresh">>).
-define(FLUSH, <<"_flush">>).
-define(OPEN, <<"_open">>).
-define(CLOSE, <<"_close">>).
-define(MGET, <<"_mget">>).
-define(COUNT, <<"_count">>).
-define(QUERY, <<"_query">>).
-define(OPTIMIZE, <<"_optimize">>).
-define(SEGMENTS, <<"_segments">>).
-define(CLEAR_CACHE, <<"_cache/clear">>).
-define(MAPPING, <<"_mapping">>).
-define(ALIASES, <<"_aliases">>).
-define(ALIAS, <<"_alias">>).
-define(UPDATE, <<"_update">>).
-define(BULK, <<"_bulk">>).

% Shortcuts
-define(ALL, <<"_all">>).

-define(POOL_IN_USE_METRIC, <<"erlasticsearch.pool.inuse">>).
-define(WORKER_DISCONNECTED_METRIC, <<"erlasticsearch.worker.disconnected.">>).
