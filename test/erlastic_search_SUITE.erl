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
-module(erlastic_search_SUITE).
-author('Mahesh Paolini-Subramanya <mahesh@dieswaytoofast.com>').

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").

-compile(export_all).

-define(CHECKSPEC(M,F,N), true = proper:check_spec({M,F,N})).
-define(PROPTEST(A), true = proper:quickcheck(A())).

-define(NUMTESTS, 500).
-define(DOCUMENT_DEPTH, 5).

-define(TEST_INDEX, <<"test_index">>).
-define(TEST_TYPE, <<"test_type">>).

-record(restResponse, {status :: integer(),
                       headers :: dict(),
                       body :: string() | binary()
                      }).

suite() ->
    [{ct_hooks,[cth_surefire]}, {timetrap,{seconds,320}}].

init_per_suite(Config) ->
    setup_lager(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_GroupName, Config) ->
    start(),
    Config.

end_per_group(_GroupName, _Config) ->
    stop(),
    ok.


init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

groups() ->
    [{crud, [],
      [t_insert, 
       t_get, 
       t_delete
      ]}
    ].

all() ->
    [{group, crud}].

t_insert(_) ->
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                {ok, Response} = erlastic_search:insert_doc(?TEST_INDEX, ?TEST_TYPE, 
                                                            BX, json_document(X)),
                ct:pal("Insert:~p~n", [Response]),
                case Response#restResponse.status of
                    R when R =:= 200 orelse
                            R =:= 201 -> true;
                    _ -> throw(false)
                end
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

t_get(_) ->
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                {ok, Response} = erlastic_search:get_doc(?TEST_INDEX, ?TEST_TYPE, BX),
                ct:pal("Get:~p~n", [Response]),
                200 = Response#restResponse.status
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

t_delete(_) ->
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                {ok, Response} = erlastic_search:delete_doc(?TEST_INDEX, ?TEST_TYPE, BX),
                ct:pal("Delete:~p~n", [Response]),
                200 = Response#restResponse.status
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

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

-spec data_index(atom(), integer()) -> binary().
data_index(Data, Index) ->
    BData = list_to_binary(atom_to_list(Data)),
    BIndex = list_to_binary(integer_to_list(Index)),
    bstr:join([BData, <<"_">>, BIndex]).

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
    application:start(erlastic_search).

stop() ->
    ok.
%    application:stop(jsx),
%    application:stop(bstr),
%    application:stop(sasl),
%    application:stop(syntax_tools),
%    application:stop(compiler),
%    application:stop(crypto),
%    application:stop(stdlib),
%    application:stop(kernel).

