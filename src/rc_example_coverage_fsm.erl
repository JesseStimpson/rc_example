-module(rc_example_coverage_fsm).

-behaviour(riak_core_coverage_fsm).

-export([start_link/4,
         init/2,
         process_results/2,
         finish/2]).

start_link(ReqId, ClientPid, Request, Timeout) ->
    riak_core_coverage_fsm:start_link(?MODULE, {pid, ReqId, ClientPid},
                                      [Request, Timeout]).

init({pid, ReqId, ClientPid}, [Request, Timeout]) ->
    lager:info("Starting coverage request ~p ~p", [ReqId, Request]),

    State = #{req_id => ReqId,
              from => ClientPid,
              request => Request,
              accum => []},

    {Request,                   %% An opaque data structure representing the command to be handled by the vnodes (in handle_coverage)
     allup,                     %% An atom that specifies whether we want to run the command in all vnodes (all) or only those reachable (allup)
     1,                         %% ReplicationFactor, used to accurately create a minimal covering set of vnodes
     1,                         %% PrimaryVNodeCoverage, the number of primary vnodes from the preflist to use in creating the coverage plan, typically 1
     rc_example,                %% NodeCheckService, same as the atom passed to the node_watcher in application startup
     rc_example_vnode_master,   %% VNodeMaster, the atom to use to reach the vnode master module
     Timeout,                   %% Timeout for the coverage request
     State}.                    %% Initial state for this fsm

process_results({{_ReqId, {Partition, Node}}, Data}, State = #{accum := Accum}) ->
    NewAccum = [{Partition, Node, Data} | Accum],
    %% return 'done' if the vnode has given all its data. Otherwise, returning 'ok' will allow future data to be delivered,
    %% or 'error' will abort the coverage command (with a call to finish)
    {done, State#{accum => NewAccum}}.

finish(clean, State = #{req_id := ReqId, from := From, accum := Accum}) ->
    lager:info("Finished coverage request ~p", [ReqId]),

    %% send the result back to the caller
    From ! {ReqId, {ok, Accum}},
    {stop, normal, State};

finish({error, Reason}, State = #{req_id := ReqId, from := From, accum := Accum}) ->
    lager:warning("Coverage query failed! Reason: ~p", [Reason]),
    From ! {ReqId, {partial, Reason, Accum}},
    {stop, normal, State}.
