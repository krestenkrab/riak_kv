%% -------------------------------------------------------------------
%%
%% riak_kv_erleveldb_backend: ErLevelDB Driver for Riak
%%
%% Copyright (c) Paul J. Davis <paul.joseph.davis@gmail.com>
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(riak_kv_erleveldb_backend).
-behavior(riak_kv_backend).
-author('Paul J. Davis <paul.joseph.davis@gmail.com>').

%% KV Backend API
-export([start/2,
         stop/1,
         get/2,
         put/3,
         delete/2,
         list/1,
         list_bucket/2,
         fold/3,
         fold_keys/3,
         fold_bucket_keys/4,
         drop/1,
         is_empty/1,
         callback/3]).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


start(Partition, Config) ->

    %% Get the data root directory
    DataDir =
        case proplists:get_value(data_root, Config) of
            undefined ->
                case application:get_env(erleveldb, data_root) of
                    {ok, Dir} ->
                        Dir;
                    _ ->
                        riak:stop("erleveldb data_root unset, failing")
                end;
            Value ->
                Value
        end,

    %% Setup actual erleveldb dir for this partition
    ok = filelib:ensure_dir(DataDir ++ "/"),
    ErLevelDBRoot = filename:join([DataDir, integer_to_list(Partition)]),
    ErLevelDBOpts = [create_if_missing],
    case erleveldb:open_db(ErLevelDBRoot, ErLevelDBOpts) of
        {ok, Db} ->
            {ok, Db};
        Error ->
            Error
    end.


stop(_) ->
    ok.


get(Db, BKey) ->
    Key = term_to_binary(BKey),
    case erleveldb:get(Db, Key) of
        {ok, Value} ->
            {ok, Value};
        {error, not_found} ->
            {error, notfound};
        {error, Reason} ->
            {error, Reason}
    end.


put(Db, BKey, Val) ->
    Key = term_to_binary(BKey),
    ok = erleveldb:put(Db, Key, Val).


delete(Db, BKey) ->
    Key = term_to_binary(BKey),
    ok = erleveldb:del(Db, Key).


list(Db) ->
    Fun = fun(B, K, Acc0) -> [{B, K} | Acc0] end,
    lists:reverse(fold_keys(Db, Fun, [])).
    

list_bucket(Db, {filter, Bucket, Fun}) ->
    FiltFun = fun(B, K, Acc) ->
        case B of
            Bucket ->
                case Fun(K) of
                    true -> [K | Acc];
                    false -> Acc
                end;
            _ ->
                Acc
        end
    end,
    fold_keys(Db, FiltFun, []);
list_bucket(Db, '_') ->
    FiltFun = fun(B, _K, Acc) ->
        case lists:member(B, Acc) of
            true -> Acc;
            false -> [B | Acc]
        end
    end,
    fold_keys(Db, FiltFun, []);
list_bucket(Db, Bucket) ->
    FiltFun = fun(B, K, Acc) ->
        case B of
            Bucket -> [K | Acc];
            _ -> Acc
        end
    end,
    fold_keys(Db, FiltFun, []).


fold_bucket_keys(Db, _Bucket, Fun, Acc) ->
    fold_keys(Db, fun(Key2, Acc2) -> Fun(Key2, dummy_val, Acc2) end, Acc).


fold_keys(Db, Fun, Acc0) ->
    {ok, Iter} = erleveldb:iter(Db),
    {ok, {BK, _}} = erleveldb:seek(Iter, first),
    {B, K} = binary_to_term(BK),
    Acc1 = Fun(B, K, Acc0),
    fold_keys0(Iter, Fun, Acc1).

fold_keys0(Iter, Fun, Acc0) ->
    case erleveldb:next(Iter) of
        {ok, {BK, _}} ->
            {B, K} = binary_to_term(BK),
            Acc1 = Fun(B, K, Acc0),
            fold_keys0(Iter, Fun, Acc1);
        {error, not_found} ->
            Acc0;
        {error, Reason} ->
            {error, Reason}
    end.


fold(Db, Fun, Acc0) ->
    {ok, Iter} = erleveldb:iter(Db),
    {ok, {BK, V}} = erleveldb:seek(Iter, first),
    {B, K} = binary_to_term(BK),
    Acc1 = Fun({B, K}, V, Acc0),
    fold0(Iter, Fun, Acc1).

fold0(Iter, Fun, Acc0) ->
    case erleveldb:next(Iter) of
        {ok, {BK, V}} ->
            {B, K} = binary_to_term(BK),
            Acc1 = Fun({B, K}, V, Acc0),
            fold0(Iter, Fun, Acc1);
        {error, not_found} ->
            Acc0;
        {error, Reason} ->
            {error, Reason}
    end.


drop(Db) ->
    erleveldb:destroy_db(Db).


is_empty(Db) ->
    {ok, Iter} = erleveldb:iter(Db),
    case erleveldb:seek(Iter, first) of
        {ok, _} -> false;
        {error, not_found} -> true
    end.


%% TODO: Something like bitcask's schedule sync.
%% Ignore callbacks for other backends so multi backend works
callback(_State, _Ref, _Msg) ->
    ok.


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

dbname() ->
    "test/erleveldb-backend".

simple_test() ->
    ?assertCmd("rm -rf " ++ dbname()),
    application:set_env(erleveldb, data_root, dbname()),
    riak_kv_backend:standard_test(?MODULE, []).

custom_config_test() ->
    ?assertCmd("rm -rf " ++ dbname()),
    application:set_env(erleveldb, data_root, ""),
    riak_kv_backend:standard_test(?MODULE, [{data_root, dbname()}]).

-endif.
