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


-type error()           :: {error, Reason :: term()}.
-type method()          :: atom().
-type response()        :: {ok, #restResponse{}} | error().
-type request()         :: #restRequest{}.
-type connection()      :: any().
-type node_name()       :: binary().
-type index()           :: binary().
-type type()            :: binary().
-type id()              :: binary() | undefined.
-type doc()             :: binary().
-type params()          :: [tuple()].
-type client_name()     :: binary().
-type registered_name() :: atom().
-type server_ref()      :: atom() | pid() | client_name().
-type target()          :: atom() | pid().


%% Defaults
-define(DEFAULT_THRIFT_HOST, "localhost").
-define(DEFAULT_THRIFT_PORT, 9500).
-define(DEFAULT_THRIFT_OPTIONS, []).
-define(REGISTERED_NAME_PREFIX, "erlasticsearch_").

%% Errors
-define(NO_SUCH_SEQUENCE, no_such_sequence).

%% Methods
-define(STATE, <<"_cluster/state">>).
-define(HEALTH, <<"_cluster/health">>).
-define(NODES, <<"_cluster/nodes">>).
-define(STATS, <<"stats">>).
-define(STATUS, <<"_status">>).
-define(SEARCH, <<"_search">>).
-define(REFRESH, <<"_refresh">>).
-define(FLUSH, <<"_flush">>).
-define(OPEN, <<"_open">>).
-define(CLOSE, <<"_close">>).
-define(OPTIMIZE, <<"_optimize">>).
-define(SEGMENTS, <<"_segments">>).
-define(CLEAR_CACHE, <<"_cache/clear">>).
