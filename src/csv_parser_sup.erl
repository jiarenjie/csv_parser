%%%-------------------------------------------------------------------
%%% @author jiarj
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 04. 九月 2017 17:54
%%%-------------------------------------------------------------------
-module(csv_parser_sup).
-author("jiarj").
-behaviour(supervisor).

%% API
-export([init/1]).
-export([start_link/0]).

-define(SERVER, ?MODULE).


start_link() ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, []).


init([]) ->
  RestartStrategy = {one_for_one, 4, 60},
  Children = [{csv_table_deal,
    {csv_table_deal, start_link, []},
    permanent, 2000, supervisor, [csv_table_deal]}],
  {ok, {RestartStrategy, Children}}.