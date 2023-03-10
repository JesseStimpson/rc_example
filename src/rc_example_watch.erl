-module(rc_example_watch).
-export([start_watch/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {name, pid, ref, tick}).

-spec start_watch(term(), integer()) -> {ok, {any(), {term(), term()}}} | {error, term()}.
start_watch(Name, Tick) ->
    Pid = self(),
    {ok, Watch} = gen_server:start_link(?MODULE, #state{name=Name, pid=Pid, tick=Tick}, []),
    case gen_server:call(Watch, start, 5000) of
        {ok, Result} ->
            {ok, {Watch, {Name, Result}}};
        Error ->
            Error
    end.

init(State) ->
    {ok, State#state{ref=self()}}.

handle_call(start, _From, State) ->
    case do_watch(State) of
        Error={error, _Reason} ->
            {stop, normal, Error, State};
        {ok, Result, State2} ->
            {reply, {ok, Result}, State2}
    end.

handle_cast(_Cast, _State) ->
    erlang:error(function_clause).

handle_info(watch, State) ->
    % TODO - do something else if the name is deleted
    {ok, _Result, State2} = do_watch(State),
    {noreply, State2};
handle_info(_Info, State) ->
    {noreply, State}.

do_watch(State=#state{name=Name, pid=Pid, ref=Ref, tick=Tick}) ->
    _ = rc_example:aae(Name, Tick),
    case rc_example:refresh_watch(Name, Pid, Ref) of
        Error={error, _Reason} ->
            Error;
        {ok, Result} ->
            erlang:send_after(Tick, self(), watch),
            {ok, Result, State}
    end.
