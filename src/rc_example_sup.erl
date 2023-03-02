%%%-------------------------------------------------------------------
%% @doc rc_example top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(rc_example_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init([]) ->
    %% it starts all the worker vnodes, receives all the requests on that particular physical node and routes each of them to the vnode that should handle it.
    VMaster = {rc_example_vnode_master,
               {riak_core_vnode_master, start_link, [rc_example_vnode]},
               permanent, 5000, worker, [riak_core_vnode_master]},

    {ok, {{one_for_one, 5, 10}, [VMaster]}}.

%% internal functions
