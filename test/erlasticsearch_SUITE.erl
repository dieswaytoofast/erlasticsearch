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
    setup_lager(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_GroupName, Config) ->
    start(),

    Index = random_name(<<"index_">>),
    IndexWithShards = bstr:join([Index, <<"with_shards">>], <<"_">>),

    Config1 = [{index, Index}, {index_with_shards, IndexWithShards}]
                ++ Config,
    % Clear out any existing indices w/ this name
    delete_all_indices(Index, true),
    delete_all_indices(IndexWithShards, true),

    Type = random_name(<<"type_">>),

    [{type, Type}] ++ Config1.

end_per_group(_GroupName, Config) ->
    Index = ?config(index, Config),
    IndexWithShards = ?config(index_with_shards, Config),
    delete_all_indices(Index, true),
    delete_all_indices(IndexWithShards, true),
    stop(),
    ok.


init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

groups() ->
    [{crud_index, [],
      [t_is_index,
       t_create_index, 
       t_create_index_with_shards
      ]},
    {crud_doc, [],
      [t_insert_doc, 
       t_get_doc, 
       t_delete_doc
      ]}
    ].

all() ->
    [{group, crud_index}, {group, crud_doc}].

t_is_index(Config) ->
    Index = ?config(index, Config),
    create_indices(Index),
    are_indices(Index),
    delete_all_indices(Index).

t_create_index(Config) ->
    Index = ?config(index, Config),
    create_indices(Index),
    delete_all_indices(Index).

t_create_index_with_shards(Config) ->
    Index = ?config(index_with_shards, Config),
    create_indices(Index),
    delete_all_indices(Index).

t_insert_doc(Config) ->
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                Response = erlasticsearch:insert_doc(Index, Type, 
                                                            BX, json_document(X)),
                ct:pal("Insert:~p~n", [Response]),
                true = erlasticsearch:is_200_or_201(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

t_get_doc(Config) ->
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                Response = erlasticsearch:get_doc(Index, Type, BX),
                ct:pal("Get:~p~n", [Response]),
                true = erlasticsearch:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

t_delete_doc(Config) ->
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                Response = erlasticsearch:delete_doc(Index, Type, BX),
                ct:pal("Delete:~p~n", [Response]),
                true = erlasticsearch:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

%% Test helpers
% Create a bunch-a indices
create_indices(Index) ->
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = bstr:join([Index, BX], <<"_">>),
                Response = erlasticsearch:create_index(FullIndex),
                ct:pal("Create Index:~p~n", [Response]),
                true = erlasticsearch:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

are_indices(Index) ->
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = bstr:join([Index, BX], <<"_">>),
                true = erlasticsearch:is_index(FullIndex)
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

% By default, blindly delete
delete_all_indices(Index) ->
    delete_all_indices(Index, false).

% Optionally check to see if the indices exist before trying to delete
delete_all_indices(Index, CheckIndex) ->
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = bstr:join([Index, BX], <<"_">>),

                case CheckIndex of
                    true ->
                        case erlasticsearch:is_index(FullIndex) of
                            % Only delete if the index exists
                            true -> 
                                delete_this_index(FullIndex);
                            false ->
                                true
                        end;
                    false -> 
                        % Blindly Delete the indices
                        delete_this_index(FullIndex)
                end
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

delete_this_index(Index) ->
    Response = erlasticsearch:delete_index(Index),
    ct:pal("Delete Index:~p~n", [{erlasticsearch:is_200(Response), Response}]),
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

-spec data_index(atom(), integer()) -> binary().
data_index(Data, Index) ->
    BData = list_to_binary(atom_to_list(Data)),
    BIndex = list_to_binary(integer_to_list(Index)),
    bstr:join([BData, <<"_">>, BIndex]).

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
    application:start(erlasticsearch).

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

