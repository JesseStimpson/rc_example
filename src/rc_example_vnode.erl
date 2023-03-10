-module(rc_example_vnode).
-behaviour(riak_core_vnode).

-export([%start_vnode/1,
         init/1,
         handle_command/3,
         handle_handoff_command/3,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_info/2]).
-export([build_update_chain_from_key/1]).

-include_lib("riak_core/include/riak_core_vnode.hrl").

-record(state, {partition, data=#{}, watches=#{}, watch_monitors=#{}}).

init([Partition]) ->
    {ok, #state{partition = Partition}}.

handle_command({get_partition_for_name, Name}, _Sender, State = #state{partition = Partition, data=Data}) ->
    case maps:is_key(Name, Data) of
        true ->
            {reply, {ok, Partition}, State};
        false ->
            {reply, {error, {not_found, Partition}}, State}
    end;

handle_command({watch, Name, Receiver, Ref}, _Sender, State=#state{data=Data}) ->
    case {add_watch(Name, Receiver, Ref, State), maps:find(Name, Data)} of
        {{true, State2}, {ok, Value}} ->
            {reply, {ok, crdt_map_to_value(Value)}, handle_watches(Name, Value, State2)};
        {{false, State2}, {ok, Value}} ->
            {reply, {ok, crdt_map_to_value(Value)}, State2};
        {{_, State2}, error} ->
            {reply, {ok, not_found}, State2}
    end;

handle_command({unwatch, Name, Receiver}, _Sender, State) ->
    State2 = remove_watch(Name, Receiver, State),
    {reply, ok, State2};

handle_command({do, Name, KeyedOps}, _Sender, State=#state{partition=Partition, data=Data}) ->
    Update = build_update_chains_from_keyed_opts(KeyedOps),
    CrdtMap = case maps:find(Name, Data) of
                  {ok, M} -> M;
                  error -> riak_dt_map:new()
              end,
    log("do ~p ~p ~p", [Update, Partition, CrdtMap], State),
    {ok, CrdtMap2} = riak_dt_map:update(Update, Partition, CrdtMap),
    NewData = Data#{Name => CrdtMap2},
    {reply, {ok, CrdtMap2}, handle_watches(Name, CrdtMap2, State#state{data=NewData})};

handle_command({put, Name, Value}, _Sender, State = #state{data = Data}) ->
    case maps:find(Name, Data) of
        {ok, Value} ->
            {reply, ok, State};
        _ ->
            log("PUT ~p:~p", [Name, Value], State),
            NewData = Data#{Name => Value},
            {reply, ok, handle_watches(Name, Value, State#state{data = NewData})}
    end;

handle_command({merge, Name, Value}, _Sender, State = #state{data = Data}) ->
    case maps:find(Name, Data) of
        {ok, Value} ->
            {reply, ok, State};
        {ok, OtherValue} ->
            %log("MERGE ~p:~p", [Name, Value], State),
            Value2 = try riak_dt_map:merge(Value, OtherValue)
                     catch _:_ -> Value
                     end,
            NewData = Data#{Name => Value2},
            {reply, ok, handle_watches(Name, Value2, State#state{data = NewData})};
        _ ->
            %log("MERGE-PUT ~p:~p", [Name, Value], State),
            NewData = Data#{Name => Value},
            {reply, ok, handle_watches(Name, Value, State#state{data = NewData})}
    end;

handle_command({get, Name, Structure}, _Sender, State = #state{data = Data}) ->
    %log("GET ~p", [Name], State),
    case maps:get(Name, Data, not_found) of
        not_found ->
            {reply, {error, not_found}, State};
        Value ->
            case Structure of
                raw ->
                    {reply, {ok, Value}, State};
                map ->
                    {reply, {ok, crdt_map_to_value(Value)}, State}
            end
    end;

handle_command({delete, Name}, _Sender, State = #state{data = Data}) ->
    log("DELETE ~p", [Name], State),
    NewData = maps:remove(Name, Data),
    State2 = remove_all_watches(Name, State),
    {reply, {ok, maps:get(Name, Data, not_found)}, State2#state{data = NewData}};

handle_command(Message, _Sender, State) ->
    log("unhandled_command ~p", [Message], State),
    {noreply, State}.

handle_info({'DOWN', MRef, process, Pid, _Reason}, State=#state{watch_monitors=WatchMonitors}) ->
    case maps:get(MRef, WatchMonitors, undefined) of
        {Name, Pid} ->
            {ok, remove_watch(Name, Pid, State)};
        _ -> % undefined | {Name, DifferentPid}
            {ok, State}
    end;
handle_info(_Info, State) ->
    {ok, State}.

handle_handoff_command(?FOLD_REQ{foldfun=FoldFun, acc0=Acc0}, _Sender,
                       State = #state{data = Data}) ->
    [ log("Received fold request for handoff", State) || maps:size(Data) > 0 ],
    Result = maps:fold(FoldFun, Acc0, Data),
    {reply, Result, State};
handle_handoff_command({get_partition_for_name, _}, _Sender, State) ->
    {forward, State};
handle_handoff_command(Get={get, _, _}, Sender, State) ->
    %%  if the command is read (a get), we reply with our local copy of the
    %%  data (we know it's up to date because we applied all the writes locally)
    log("GET during handoff, handling locally ~p", [Name], State),
    handle_command(Get, Sender, State);
handle_handoff_command(Watch={watch, _, _, _}, _Sender, State) ->
    %% we know our copy is up to date, so we can accept the watch
    handle_command(Watch, Sender, State);
handle_handoff_command(Message, Sender, State) ->
    %% when the command is a write (a put or a delete), we change our local
    %% copy of the code and we forward it to the receiving vnode (that way,
    %% if it was already migrated, the change is applied in that copy too)
    log("~p during handoff, handling locally and forwarding", [Message], State),
    {reply, _Result, NewState} = handle_command(Message, Sender, State),
    {forward, NewState}.

handle_handoff_data(BinData, State) ->
    {Name, Value} = binary_to_term(BinData),
    log("received handoff data ~p", [{Name, Value}], State),
    handle_command({merge, Name, Value}, ignore, State).

encode_handoff_item(Name, Value) ->
    term_to_binary({Name, Value}).

handle_coverage(names, _KeySpaces, {_, ReqId, _}, State = #state{data = Data}) ->
    log("Received names coverage", State),
    Names = maps:keys(Data),
    {reply, {ReqId, Names}, State};
handle_coverage(values, _KeySpaces, {_, ReqId, _}, State = #state{data = Data}) ->
    log("Received values coverage", State),
    Values = maps:values(Data),
    {reply, {ReqId, Values}, State};
handle_coverage(clear, _KeySpaces, {_, ReqId, _}, State) ->
    log("Received clear coverage", State),
    NewState = State#{data => #{}},
    {reply, {ReqId, []}, NewState}.

%% internal
handle_watches(Name, Value, State=#state{watches=Watches}) ->
    NamedWatches = maps:get(Name, Watches, []),
    [ Pid ! {Ref, {Name, crdt_map_to_value(Value)}} || {Pid, {Ref, _MRef}} <- NamedWatches ],
    State.

%% same as logger:info but prepends the partition
log(String, State) ->
  log(String, [], State).

log(String, Args, #state{partition = Partition}) ->
  String2 = "[~.36B] " ++ String,
  Args2 = [Partition | Args],
  lager:info(String2, Args2),
  ok.

add_watch(Name, Receiver, Ref, State=#state{watches=Watches, watch_monitors=WatchMonitors}) ->
    NamedWatches = maps:get(Name, Watches, []),
    case proplists:get_value(Receiver, NamedWatches) of
        {Ref, _MRef} ->
            {false, State};
        {_, MRef} ->
            NamedWatches2 = lists:keystore(Receiver, 1, NamedWatches, {Receiver, {Ref, MRef}}),
            {true, State#state{watches=Watches#{Name => NamedWatches2}}};
        undefined ->
            MRef = erlang:monitor(process, Receiver),
            log("watch ~p -> ~p ~p", [Name, Receiver, MRef], State),
            NamedWatches2 = [{Receiver, {Ref, MRef}}|NamedWatches],
            WatchMonitors2 = WatchMonitors#{MRef => {Name, Receiver}},
            {true, State#state{watches = Watches#{Name => NamedWatches2}, watch_monitors = WatchMonitors2}}
    end.

remove_watch(Name, Receiver, State=#state{watches=Watches, watch_monitors=WatchMonitors}) ->
    NamedWatches = maps:get(Name, Watches, []),
    case proplists:get_value(Receiver, NamedWatches) of
        {_Ref, MRef} ->
            log("unwatch ~p ~p", [Name, Receiver], State),
            erlang:demonitor(MRef),
            WatchMonitors2 = maps:without([MRef], WatchMonitors),
            case proplists:delete(Receiver, NamedWatches) of
                [] ->
                    State#state{watches=maps:without([Name], Watches), watch_monitors=WatchMonitors2};
                NamedWatches2 ->
                    State#state{watches=Watches#{Name => NamedWatches2}, watch_monitors=WatchMonitors2}
            end;
        undefined ->
            State
    end.

remove_all_watches(Name, State=#state{watches=Watches, watch_monitors=WatchMonitors}) ->
    NamedWatches = maps:get(Name, Watches, []),
    OldMRefs = lists:map(
      fun({_Receiver, {_Ref, MRef}}) ->
              erlang:demonitor(MRef),
              MRef
      end, NamedWatches),
    State#state{watches=maps:without([Name], Watches), watch_monitors=maps:without(OldMRefs, WatchMonitors)}.

%% @doc
%% Key is a list of atoms that name nested maps
build_update_chain_from_key(Key) ->
    % __accessed__ is a special key in every leaf map that counts the number of accesses. The riak
    % API cannot make an empty leaf map, so we just throw a counter in there.
    Final = {update, [{update, {'__accessed__', riak_dt_emcntr}, increment}]},
    build_update_chain_from_key(Key, Final).

build_update_chains_from_keyed_opts(KeyedOps) ->
    Chains = [ build_update_chain_from_key(Key, Op) || {Key, Op} <- KeyedOps ],
    TopLevelOps = lists:foldl(fun({update, TLO}, Acc) -> Acc ++ TLO end, [], Chains),
    {update, TopLevelOps}.

%% @doc
%% Final is the list of operations to execute on the final map in the Key
build_update_chain_from_key([], Final) ->
    Final;
build_update_chain_from_key([H|T], Final) ->
    {update, [{update, {H, riak_dt_map}, build_update_chain_from_key(T, Final)}]}.

crdt_map_to_value(CrdtMap) ->
    FieldList = riak_dt_map:value(CrdtMap),
    crdt_fields_to_value(riak_dt_map, FieldList).

crdt_fields_to_value(riak_dt_map, FieldList) when is_list(FieldList) ->
    maps:from_list([ {FieldName2, crdt_fields_to_value(FieldType, FieldValue2)} || {{FieldName2, FieldType}, FieldValue2} <- FieldList ]);
crdt_fields_to_value(riak_dt_emcntr, I) when is_integer(I) ->
    I.

