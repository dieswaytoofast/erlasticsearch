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
-type index()           :: binary().
-type type()            :: binary().
-type id()              :: binary().
-type doc()             :: binary().
-type params()          :: [tuple()].

%% Defaults
-define(DEFAULT_THRIFT_HOST, "localhost").
-define(DEFAULT_THRIFT_PORT, 9500).
-define(DEFAULT_THRIFT_OPTIONS, []).

%% Errors
-define(NO_SUCH_SEQUENCE, no_such_sequence).

%% Methods
-define(HEALTH, "_cluster/health").
