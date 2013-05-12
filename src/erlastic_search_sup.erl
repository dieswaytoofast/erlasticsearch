%%%-------------------------------------------------------------------
%%% @author Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>
%%% @copyright (C) 2013 Mahesh Paolini-Subramanya
%%% @doc Root Supervisor for erlastic_search
%%% @end
%%%
%%% This source file is subject to the New BSD License. You should have received
%%% a copy of the New BSD license with this software. If not, it can be
%%% retrieved from: http://www.opensource.org/licenses/bsd-license.php
%%%-------------------------------------------------------------------
-module(erlastic_search_sup).
-author('Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>').

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%% @doc Helper macro for declaring children of supervisor
-define(WORKER(Restart, Module, Args), {Module, {Module, start_link, Args}, Restart, 5000, worker, [Module]}).
-define(SUPERVISOR(Restart, Module, Args), {Module, {Module, start_link, Args}, Restart, 5000, supervisor, [Module]}).

-type startlink_err() :: {'already_started', pid()} | 'shutdown' | term().
-type startlink_ret() :: {'ok', pid()} | 'ignore' | {'error', startlink_err()}.

-spec start_link() -> startlink_ret().
start_link() ->
    lager:debug("Starting erlastic_search root supervisor (~s)~n", [?SERVER]),
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init(Args :: term()) -> {ok, {{RestartStrategy :: supervisor:strategy(), MaxR :: non_neg_integer(), MaxT :: non_neg_integer()},
                                    [ChildSpec :: supervisor:child_spec()]}}.
init([]) ->
    RestartStrategy = one_for_one,
    MaxRestarts = 3,
    MaxSecondsBetweenRestarts = 60,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},
    {ok, {SupFlags, [?WORKER(permanent, erlastic_search, [])]}}.
