%%%-------------------------------------------------------------------
%%% @author Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>
%%% @copyright (C) 2013 Mahesh Paolini-Subramanya
%%% @doc Root Supervisor for erlasticsearch
%%% @end
%%%
%%% This source file is subject to the New BSD License. You should have received
%%% a copy of the New BSD license with this software. If not, it can be
%%% retrieved from: http://www.opensource.org/licenses/bsd-license.php
%%%-------------------------------------------------------------------
-module(erlasticsearch_sup).
-author('Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>').

-behaviour(supervisor).

-include("erlasticsearch.hrl").

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

-type startlink_err() :: {'already_started', pid()} | 'shutdown' | term().
-type startlink_ret() :: {'ok', pid()} | 'ignore' | {'error', startlink_err()}.

-spec start_link() -> startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init(Args :: term()) -> {ok, {{RestartStrategy :: supervisor:strategy(), MaxR :: non_neg_integer(), MaxT :: non_neg_integer()},
                                    [ChildSpec :: supervisor:child_spec()]}}.
init([]) ->
    {ok, Pools} = application:get_env(erlasticsearch, pools),
    PoolSpecs =
        lists:map(
          fun({Name, SizeArgs, WorkerArgs}) ->
                  PoolArgs =
                      [{name, {local, Name}},
                       {worker_module, erlasticsearch_poolboy_worker}] ++ SizeArgs,
                  poolboy:child_spec(Name, PoolArgs, WorkerArgs)
          end,
          Pools),
    {ok, {{one_for_one, 10, 10}, PoolSpecs}}.
