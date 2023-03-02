-module(rc_example).

-export([ping/0,
        ring_status/0,
        put/2,
        get/1,
        delete/1,
        keys/0,
        values/0,
        clear/0]).

ping() ->
    sync_command(os:timestamp(), ping).

ring_status() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:pretty_print(Ring, [legend]).

put(Key, Value) ->
    sync_command(Key, {put, Key, Value}).

get(Key) ->
    sync_command(Key, {get, Key}).

delete(Key) ->
    sync_command(Key, {delete, Key}).

keys() ->
    coverage_to_list(coverage_command(keys)).

values() ->
    coverage_to_list(coverage_command(values)).

clear() ->
    {ok, #{list := []}} = coverage_to_list(coverage_command(clear)),
    ok.


%% internal

hash_key(Key) ->
    riak_core_util:chash_key({<<"rc_example">>, term_to_binary(Key)}).

sync_command(Key, Command)->
    DocIdx = hash_key(Key),
    PrefList = riak_core_apl:get_apl(DocIdx, 1, rc_example),
    [IndexNode] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, Command, rc_example_vnode_master).

coverage_command(Command) ->
    Timeout = 5000,
    ReqId = erlang:phash2(make_ref()), % must be an int
    {ok, _Process} = rc_example_coverage_fsm:start_link(ReqId, self(), Command, Timeout),
    receive
        {ReqId, Val} ->
            Val
    after Timeout ->
              exit(timeout)
    end.

coverage_to_list({ok, CoverageResult}) ->
    {ok, lists:foldl(
      fun({_Partition, _Node, L}, Acc=#{list := List, from := C, n := T}) when L =/= [] ->
              Acc#{list => List ++ L, from => C+1, n => T+1};
          ({_Partition, _Node, []}, Acc=#{n := T}) ->
              Acc#{n => T+1}
      end, #{list => [], from => 0, n => 0}, CoverageResult)};
coverage_to_list(Error) ->
    Error.
