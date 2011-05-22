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
-author('Kresten Krab Thorup <krab@trifork.com>').

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
	    io:format("Failed to open ~p: ~p~n", [ErLevelDBRoot, Error]),
            Error
    end.


stop(_) ->
    ok.


get(Db, BKey) ->
    Key = sext:encode(BKey),
    case erleveldb:get(Db, Key) of
        {ok, Value} ->
            {ok, Value};
        {error, not_found} ->
            {error, notfound};
        {error, Reason} ->
            {error, Reason}
    end.


put(Db, BKey, Val) ->
    Key = sext:encode(BKey),
    ok = erleveldb:put(Db, Key, Val).


delete(Db, BKey) ->
    Key = sext:encode(BKey),
    ok = erleveldb:del(Db, Key).


list(Db) ->
    Fun = fun(B, K, Acc0) -> [{B, K} | Acc0] end,
    lists:reverse(fold_keys(Db, Fun, [])).
    

list_bucket(Db, {filter, Bucket, Fun}) ->
    FiltFun = fun(_B, K, Acc) ->
		      case Fun(K) of
			  true -> [K | Acc];
			  false -> Acc
		      end
	      end,
    fold_bucket_keys(Db, Bucket, FiltFun, []);
list_bucket(Db, '_') ->
    list_buckets(Db);
list_bucket(Db, Bucket) ->
    FiltFun = fun(_B, K, Acc) -> [K | Acc] end,
    fold_bucket_keys(Db, Bucket, FiltFun, []).

list_buckets(Db) ->
    {ok, Iter} = erleveldb:iter(Db),
    list_buckets0(Iter, sext:encode({'_', '_'}), []).

list_buckets0(Iter, From, Result) ->
    case erleveldb:seek(Iter, From) of
	{ok, {Bucket,_}} ->
            NextFrom = sext:prefix({<<Bucket/binary,0>>,'_'}),
	    list_buckets0(Iter, NextFrom, [Bucket|Result]);
	{error, not_found} ->
	    Result;
	{error,_}=Error ->
	    Error
    end.

fold_bucket_keys(Db, Bucket, Fun, Acc) ->
    {ok, Iter} = erleveldb:iter(Db),
    Prefix = sext:prefix({Bucket, '_'}),
    fold_bucket_keys0(erleveldb:seek(Iter, Prefix),
		      Bucket, Iter, Fun, Acc).

fold_bucket_keys0({ok, {BK, _}}, Bucket, Iter, Fun, Acc0) ->
    case sext:decode(BK) of
	{Bucket, K} ->
	    Acc1 = Fun(Bucket, K, Acc0),
	    fold_bucket_keys0(erleveldb:next(Iter),
			      Bucket, Iter, Fun, Acc1);
	_ ->
	    Acc0
    end;
fold_bucket_keys0({error, not_found}, _,_,_,Acc) ->
    Acc;
fold_bucket_keys0({error, _}=Err, _,_,_,_) ->
    Err.
    

fold_keys(Db, Fun, Acc0) ->
    {ok, Iter} = erleveldb:iter(Db),
    fold_keys0(erleveldb:seek(Iter, first), Iter, Fun, Acc0).

fold_keys0({ok, {BK, _}}, Iter, Fun, Acc0) ->
    {B, K} = sext:decode(BK),
    Acc1 = Fun(B, K, Acc0),
    fold_keys0(erleveldb:next(Iter), Iter, Fun, Acc1);
fold_keys0({error, not_found}, _,_,Acc) ->
    Acc;
fold_keys0({error, _}=Err, _,_,_) ->
    Err.


fold(Db, Fun, Acc) ->
    {ok, Iter} = erleveldb:iter(Db),
    fold0(erleveldb:seek(Iter, first), Iter, Fun, Acc).

fold0({ok, {BK, V}}, Iter, Fun, Acc0) ->
    {B, K} = sext:decode(BK),
    Acc1 = Fun({B, K}, V, Acc0),
    fold0(erleveldb:next(Iter), Iter, Fun, Acc1);
fold0({error, not_found}, _,_,Acc) ->
    Acc;
fold0({error, _}=Err,_,_,_) ->
    Err.


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
