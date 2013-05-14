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

init_per_group(_GroupName, Config0) ->
    ClientName = random_name(<<"client_">>),
    Config1 = [{client_name, ClientName} | Config0],
    start(Config1),


    Index = random_name(<<"index_">>),
    IndexWithShards = bstr:join([Index, <<"with_shards">>], <<"_">>),

    Config2 = [{index, Index}, {index_with_shards, IndexWithShards}]
                ++ Config1,
    % Clear out any existing indices w/ this name
    delete_all_indices(Config2),

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
      [t_is_index,
       t_create_index, 
       t_create_index_with_shards
      ]},
     {index_helpers, [],
       [t_flush_1,
       t_flush_list,
       t_flush_all,
       t_refresh_1,
       t_refresh_list,
       t_refresh_all
       ]},
    {crud_doc, [],
      [t_insert_doc, 
       t_get_doc, 
       t_delete_doc
      ]},
     {search, [],
      [t_search
      ]}
    ].

all() ->
    [
        {group, crud_index}, 
        {group, crud_doc}, 
        {group, search},
        {group, index_helpers}
    ].

t_is_index(Config) ->
    ClientName = ?config(client_name, Config),
    Index = ?config(index, Config),
    create_indices(ClientName, Index),
    are_indices(ClientName, Index),
    delete_all_indices(ClientName, Index).

t_create_index(Config) ->
    ClientName = ?config(client_name, Config),
    Index = ?config(index, Config),
    create_indices(ClientName, Index),
    delete_all_indices(ClientName, Index).

t_create_index_with_shards(Config) ->
    ClientName = ?config(client_name, Config),
    Index = ?config(index_with_shards, Config),
    create_indices(ClientName, Index),
    delete_all_indices(ClientName, Index).

t_flush_1(Config) ->
    ClientName = ?config(client_name, Config),
    Index = ?config(index, Config),
    create_indices(ClientName, Index),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = bstr:join([Index, BX], <<"_">>),
                Response = erlasticsearch:flush(ClientName, FullIndex),
                ct:pal("Flush one Index:~p~n", [Response]),
                true = erlasticsearch:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    delete_all_indices(ClientName, Index).

t_flush_list(Config) ->
    ClientName = ?config(client_name, Config),
    Index = ?config(index, Config),
    create_indices(ClientName, Index),
    Indexes = 
    lists:map(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                bstr:join([Index, BX], <<"_">>)
            end, lists:seq(1, ?DOCUMENT_DEPTH)),
    Response = erlasticsearch:flush(ClientName, Indexes),
    ct:pal("Flush list Index:~p~n", [Response]),
    true = erlasticsearch:is_200(Response),
    delete_all_indices(ClientName, Index).

t_flush_all(Config) ->
    ClientName = ?config(client_name, Config),
    Index = ?config(index, Config),
    create_indices(ClientName, Index),
    Response = erlasticsearch:flush(ClientName),
    ct:pal("Flush all Index:~p~n", [Response]),
    true = erlasticsearch:is_200(Response),
    delete_all_indices(ClientName, Index).

t_refresh_1(Config) ->
    ClientName = ?config(client_name, Config),
    Index = ?config(index, Config),
    create_indices(ClientName, Index),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = bstr:join([Index, BX], <<"_">>),
                Response = erlasticsearch:refresh(ClientName, FullIndex),
                ct:pal("Flush one Index:~p~n", [Response]),
                true = erlasticsearch:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    delete_all_indices(ClientName, Index).

t_refresh_list(Config) ->
    ClientName = ?config(client_name, Config),
    Index = ?config(index, Config),
    create_indices(ClientName, Index),
    Indexes = 
    lists:map(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                bstr:join([Index, BX], <<"_">>)
            end, lists:seq(1, ?DOCUMENT_DEPTH)),
    Response = erlasticsearch:refresh(ClientName, Indexes),
    ct:pal("Flush list Index:~p~n", [Response]),
    true = erlasticsearch:is_200(Response),
    delete_all_indices(ClientName, Index).

t_refresh_all(Config) ->
    ClientName = ?config(client_name, Config),
    Index = ?config(index, Config),
    create_indices(ClientName, Index),
    Response = erlasticsearch:refresh(ClientName),
    ct:pal("Flush all Index:~p~n", [Response]),
    true = erlasticsearch:is_200(Response),
    delete_all_indices(ClientName, Index).



t_search(Config) ->
    t_insert_doc(Config),

    ClientName = ?config(client_name, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    lists:foreach(fun(X) ->
                Query = in_query(X),
                % Refresh the index otherwise it might not 'take'
                erlasticsearch:refresh(ClientName, Index),
                Result = erlasticsearch:search(ClientName, Index, Type, <<>>, [{q, Query}]),
                % The document is structured so that the number of top level
                % keys is as (?DOCUMENT_DEPTH + 1 - X)
                ?DOCUMENT_DEPTH  = hits_from_result(Result) + X - 1
        end, lists:seq(1, ?DOCUMENT_DEPTH)),
    t_delete_doc(Config).

in_query(X) ->
    Key = key(X),
    Value = value(X),
    bstr:join([Key, Value], <<":">>).

hits_from_result({ok, {_, _, _, JSON}}) ->
    case lists:keyfind(<<"hits">>, 1, jsx:decode(JSON)) of
        false -> throw(false);
        {_, Result} ->
            case lists:keyfind(<<"total">>, 1, Result) of
                false -> throw(false);
                {_, Data} -> Data
            end
    end.



t_insert_doc(Config) ->
    ClientName = ?config(client_name, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                Response = erlasticsearch:insert_doc(ClientName, Index, 
                                                     Type, BX, json_document(X)),
                ct:pal("Insert:~p~n", [Response]),
                true = erlasticsearch:is_200_or_201(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

t_get_doc(Config) ->
    ClientName = ?config(client_name, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                Response = erlasticsearch:get_doc(ClientName, Index, Type, BX),
                ct:pal("Get:~p~n", [Response]),
                true = erlasticsearch:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

t_delete_doc(Config) ->
    ClientName = ?config(client_name, Config),
    Index = ?config(index, Config),
    Type = ?config(type, Config),
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                Response = erlasticsearch:delete_doc(ClientName, Index, Type, BX),
                ct:pal("Delete:~p~n", [Response]),
                true = erlasticsearch:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

%% Test helpers
% Create a bunch-a indices
create_indices(ClientName, Index) ->
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = bstr:join([Index, BX], <<"_">>),
                Response = erlasticsearch:create_index(ClientName, FullIndex),
                ct:pal("Create Index:~p~n", [Response]),
                true = erlasticsearch:is_200(Response)
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

are_indices(ClientName, Index) ->
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = bstr:join([Index, BX], <<"_">>),
                true = erlasticsearch:is_index(ClientName, FullIndex)
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

delete_all_indices(Config) ->
    ct:pal("Config:~p~n", [Config]),
    ClientName = ?config(client_name, Config),
    Index = ?config(index, Config),
    IndexWithShards = bstr:join([Index, <<"with_shards">>], <<"_">>),
    delete_all_indices(ClientName, Index, true),
    delete_all_indices(ClientName, IndexWithShards, true).

% By default, blindly delete
delete_all_indices(ClientName, Index) ->
    delete_all_indices(ClientName, Index, false).

% Optionally check to see if the indices exist before trying to delete
delete_all_indices(ClientName, Index, CheckIndex) ->
    lists:foreach(fun(X) ->
                BX = list_to_binary(integer_to_list(X)),
                FullIndex = bstr:join([Index, BX], <<"_">>),

                case CheckIndex of
                    true ->
                        case erlasticsearch:is_index(ClientName, FullIndex) of
                            % Only delete if the index exists
                            true -> 
                                delete_this_index(ClientName, FullIndex);
                            false ->
                                true
                        end;
                    false -> 
                        % Blindly Delete the indices
                        delete_this_index(ClientName, FullIndex)
                end
        end, lists:seq(1, ?DOCUMENT_DEPTH)).

delete_this_index(ClientName, Index) ->
    Response = erlasticsearch:delete_index(ClientName, Index),
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
    bstr:join([BData, BIndex], <<"_">>).

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

start(Config) ->
    ClientName = ?config(client_name, Config),
    application:start(kernel),
    application:start(stdlib),
    application:start(crypto),
    application:start(compiler),
    application:start(syntax_tools),
    application:start(sasl),
    application:start(bstr),
    application:start(jsx),
    application:start(erlasticsearch),
    erlasticsearch:start_client(ClientName)
    .

stop(Config) ->
    ClientName = ?config(client_name, Config),
    erlasticsearch:stop_client(ClientName),
    ok.
%    application:stop(jsx),
%    application:stop(bstr),
%    application:stop(sasl),
%    application:stop(syntax_tools),
%    application:stop(compiler),
%    application:stop(crypto),
%    application:stop(stdlib),
%    application:stop(kernel).

