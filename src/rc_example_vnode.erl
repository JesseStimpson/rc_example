-module(rc_example_vnode).
-behaviour(riak_core_vnode).

-export([%start_vnode/1,
         init/1,
         handle_command/3,
         handle_handoff_command/3,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4]).

-include_lib("riak_core/include/riak_core_vnode.hrl").

init([Partition]) ->
    {ok, #{partition => Partition, data => #{}}}.

handle_command({put, Key, Value}, _Sender, State = #{data := Data}) ->
    log("PUT ~p:~p", [Key, Value], State),
    NewData = Data#{Key => Value},
    {reply, ok, State#{data => NewData}};

handle_command({get, Key}, _Sender, State = #{data := Data}) ->
    log("GET ~p", [Key], State),
    {reply, maps:get(Key, Data, not_found), State};

handle_command({delete, Key}, _Sender, State = #{data := Data}) ->
    log("DELETE ~p", [Key], State),
    NewData = maps:remove(Key, Data),
    {reply, maps:get(Key, Data, not_found), State#{data => NewData}};

handle_command(ping, _Sender, State = #{partition := Partition}) ->
    log("Received ping command ~p", [Partition], State),
    {reply, {pong, Partition}, State};

handle_command(Message, _Sender, State) ->
    log("unhandled_command ~p", [Message], State),
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=FoldFun, acc0=Acc0}, _Sender,
                       State = #{data := Data}) ->
    log("Received fold request for handoff", State),
    Result = maps:fold(FoldFun, Acc0, Data),
    {reply, Result, State};
handle_handoff_command({get, Key}, Sender, State) ->
    %%  if the command is read (a get), we reply with our local copy of the
    %%  data (we know it's up to date because we applied all the writes locally)
    log("GET during handoff, handling locally ~p", [Key], State),
    handle_command({get, Key}, Sender, State);
handle_handoff_command(Message, Sender, State) ->
    %% when the command is a write (a put or a delete), we change our local
    %% copy of the code and we forward it to the receiving vnode (that way,
    %% if it was already migrated, the change is applied in that copy too)
    log("~p during handoff, handling locally and forwarding", [Message], State),
    {reply, _Result, NewState} = handle_command(Message, Sender, State),
    {forward, NewState}.

handle_handoff_data(BinData, State=#{data := Data}) ->
    {Key, Value} = binary_to_term(BinData),
    log("received handoff data ~p", [{Key, Value}], State),
    NewData = Data#{Key => Value},
    {reply, ok, State#{data => NewData}}.

encode_handoff_item(Key, Value) ->
    term_to_binary({Key, Value}).

handle_coverage(keys, _KeySpaces, {_, ReqId, _}, State = #{data := Data}) ->
    log("Received keys coverage", State),
    Keys = maps:keys(Data),
    {reply, {ReqId, Keys}, State};
handle_coverage(values, _KeySpaces, {_, ReqId, _}, State = #{data := Data}) ->
    log("Received values coverage", State),
    Values = maps:values(Data),
    {reply, {ReqId, Values}, State};
handle_coverage(clear, _KeySpaces, {_, ReqId, _}, State) ->
    log("Received clear coverage", State),
    NewState = State#{data => #{}},
    {reply, {ReqId, []}, NewState}.

%% internal

%% same as logger:info but prepends the partition
log(String, State) ->
  log(String, [], State).

log(String, Args, #{partition := Partition}) ->
  String2 = "[~.36B] " ++ String,
  Args2 = [Partition | Args],
  lager:info(String2, Args2),
  ok.
