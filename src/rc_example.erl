-module(rc_example).

-export([ring_status/0,
        put/2,
        get/1,
        delete/1,
        names/0,
        values/0,
        clear/0,
        watch/1,
        aae/2,
        do/2,
        do_test/0,
        refresh_watch/3]).

-define(Replicas, 8).
-define(VNodeMaster, rc_example_vnode_master).
-define(NodeService, rc_example).
-define(Bucket, <<"rc_example">>).
-define(Timeout, 5000).

ring_status() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:pretty_print(Ring, [legend]).

do_test() ->
    Name = jms,
    Key = [a],
    Increment = fun(CntrName) ->
                        CrdtField = {CntrName, riak_dt_emcntr},
                        {update, CrdtField, increment}
                end,
    Update = {update, [Increment(c)]},
    KeyedUpdate = {Key, Update},
    do(Name, [KeyedUpdate]).

do(Name, KeyedOps) ->
    % Find the closest partition, perform the action there, get the crdt as a result, merge it with the others immediately
    DocIdx = hash_name(Name),
    PrefList = riak_core_apl:get_apl(DocIdx, ?Replicas, ?NodeService),
    case sync_command_first_success_wins(Name, {get_partition_for_name, Name}, PrefList, ?Timeout) of
        {ok, PartitionId} ->
            do1(PartitionId, PrefList, Name, KeyedOps);
        {error, {not_found, PartitionId}} ->
            do1(PartitionId, PrefList, Name, KeyedOps);
        Error ->
            Error
    end.

do1(PartitionId, PrefList, Name, KeyedOps) ->
    IndexNode = lists:keyfind(PartitionId, 1, PrefList),
    Losers = proplists:delete(PartitionId, PrefList),
    case riak_core_vnode_master:sync_spawn_command(IndexNode, {do, Name, KeyedOps}, ?VNodeMaster) of
        {ok, CrdtMap} ->
            riak_core_vnode_master:command(Losers, {merge, Name, CrdtMap}, ignore, ?VNodeMaster),
            ok;
        Error ->
            Error
    end.

put(Name, Value) ->
    sync_command_first_success_wins(Name, {put, Name, Value}, ?Replicas, ?Timeout).

get(Name) ->
    sync_command_first_success_wins(Name, {get, Name, map}, ?Replicas, ?Timeout).

delete(Name) ->
    sync_command_first_success_wins(Name, {delete, Name}, ?Replicas, ?Timeout).

names() ->
    coverage_to_list(coverage_command(names)).

values() ->
    coverage_to_list(coverage_command(values)).

clear() ->
    {ok, #{list := []}} = coverage_to_list(coverage_command(clear)),
    ok.

watch(Name) ->
    rc_example_watch:start_watch(Name, 10000).

refresh_watch(Name, Receiver, Ref) ->
    DocIdx = hash_name(Name),
    PrefList = riak_core_apl:get_apl(DocIdx, ?Replicas, ?NodeService),
    case sync_command_first_success_wins(Name, {get_partition_for_name, Name}, PrefList, ?Timeout) of
        {ok, PartitionId} ->
            IndexNode = lists:keyfind(PartitionId, 1, PrefList),
            Losers = proplists:delete(PartitionId, PrefList),
            % We choose to watch first, then unwatch => duplicates, but no misses
            Res = riak_core_vnode_master:sync_spawn_command(IndexNode, {watch, Name, Receiver, Ref}, ?VNodeMaster),
            riak_core_vnode_master:command(Losers, {unwatch, Name, Receiver}, ignore, ?VNodeMaster),
            Res;
        Error ->
            Error
    end.

aae(Name, Timeout) ->
    DocIdx = hash_name(Name),
    PrefList = riak_core_apl:get_apl(DocIdx, ?Replicas, ?NodeService),
    try
        case sync_command_all_successes(Name, {get, Name, raw}, PrefList, Timeout) of
            [FirstValue|Values] ->
                MergedFinal = lists:foldl(
                  fun(Value, Merged) ->
                          % Note: if Value =:= Merged, riak_dt_map:merge/2 always returns the Merged,
                          % even if the data passed in isn't a riak_dt_map at all
                          try riak_dt_map:merge(Value, Merged)
                          catch _:_ ->
                                    lager:info("failed to merge ~p into ~p", [Value, Merged]),
                                    Merged
                          end
                  end, FirstValue, Values),
                riak_core_vnode_master:command(PrefList, {merge, Name, MergedFinal}, ignore, ?VNodeMaster),
                ok;
            [] ->
                ok
        end
    catch error:timeout ->
              {error, timeout}
    end.

%% internal
%%

% Command must induce the vnode to reply with {ok, term()} or {error, term()}
sync_command_all_successes(Name, Command, N, Timeout) when is_integer(N) ->
    DocIdx = hash_name(Name),
    PrefList = riak_core_apl:get_apl(DocIdx, N, ?NodeService),
    sync_command_all_successes(Name, Command, PrefList, Timeout);
sync_command_all_successes(_Name, Command, PrefList, Timeout) ->
    ReqId = erlang:phash2(make_ref()),
    Self = self(),
    ReceiverProxy = spawn_link(
                 fun() ->
                         fun CollectResponses(0, Acc) ->
                                 Self ! {ReqId, Acc};
                             CollectResponses(N, Acc) ->
                                 receive
                                     {ReqId, {error, _}} ->
                                         CollectResponses(N-1, Acc);
                                     {ReqId, {ok, Resp}} ->
                                         CollectResponses(N-1, [Resp|Acc])
                                 end
                        end(length(PrefList), [])
                 end),
    Sender = {raw, ReqId, ReceiverProxy},
    riak_core_vnode_master:command(PrefList, Command, Sender, ?VNodeMaster),
    receive
        {ReqId, Reply} ->
            lists:reverse(Reply)
    after Timeout ->
              unlink(ReceiverProxy),
              exit(ReceiverProxy, kill),
              erlang:error(timeout)
    end.

hash_name(Name) ->
    riak_core_util:chash_key({?Bucket, term_to_binary(Name)}).

%sync_command(Name, Command)->
%    % TODO - send to all replicas
%    DocIdx = hash_name(Name),
%    PrefList = riak_core_apl:get_apl(DocIdx, 1, ?NodeService),
%    [IndexNode] = PrefList,
%    riak_core_vnode_master:sync_spawn_command(IndexNode, Command, ?VNodeMaster).

% Command must induce the vnode to reply with {ok, term()} or {error, term()}
sync_command_first_success_wins(Name, Command, N, Timeout) when is_integer(N) ->
    DocIdx = hash_name(Name),
    PrefList = riak_core_apl:get_apl(DocIdx, N, ?NodeService),
    sync_command_first_success_wins(Name, Command, PrefList, Timeout);
sync_command_first_success_wins(_Name, Command, PrefList, Timeout) when is_list(PrefList) ->
    ReqId = erlang:phash2(make_ref()),

    Self = self(),
    % using a proxy process allows extra messages to be cleanly dropped
    ReceiverProxy = spawn_link(
                 fun() ->
                         % Returns {ok, term()} for the first successful partition encountered.
                         % Otherwise, returns the first error encountered
                         fun LookForSuccess(0, FirstError) ->
                                 Self ! {ReqId, FirstError};
                             LookForSuccess(N, FirstError) ->
                                 receive
                                     {ReqId, ok} ->
                                         Self ! {ReqId, ok};
                                     {ReqId, {ok, Reply}} ->
                                         Self ! {ReqId, {ok, Reply}};
                                     {ReqId, Error={error, _Reason}} ->
                                         case FirstError of
                                             {error, no_response} ->
                                                 LookForSuccess(N-1, Error);
                                             _ ->
                                                 LookForSuccess(N-1, FirstError)
                                         end
                                 end
                        end(length(PrefList), {error, no_response})
                 end),
    Sender = {raw, ReqId, ReceiverProxy},
    riak_core_vnode_master:command(PrefList, Command, Sender, ?VNodeMaster),
    receive
        {ReqId, Reply} ->
            Reply
    after Timeout ->
              unlink(ReceiverProxy),
              exit(ReceiverProxy, kill),
              erlang:error(timeout)
    end.

coverage_command(Command) ->
    riak_core_coverage_fold:run_link(fun(Partition, Node, Data, Acc) ->
                                             [{Partition, Node, Data}|Acc]
                                     end, [], {?VNodeMaster, ?NodeService, Command}, ?Timeout).

coverage_to_list({ok, CoverageResult}) ->
    {ok, lists:foldl(
      fun({_Partition, _Node, L}, Acc=#{list := List, from := C, n := T}) when L =/= [] ->
              Acc#{list => List ++ L, from => C+1, n => T+1};
          ({_Partition, _Node, []}, Acc=#{n := T}) ->
              Acc#{n => T+1}
      end, #{list => [], from => 0, n => 0}, CoverageResult)};
coverage_to_list(Error) ->
    Error.
