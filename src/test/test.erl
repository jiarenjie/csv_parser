%%%-------------------------------------------------------------------
%%% @author jiarj
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. 六月 2017 14:07
%%%-------------------------------------------------------------------
-module(test).
-compile(export_all).
-author("jiarj").

%% API
-export([]).

config() ->
  #{field_map => #{
    <<"SettleDate">> => {<<"清算日期"/utf8>>, [<<" ">>]}
    , <<"TxnTimeWithColon">> => {<<"交易时间"/utf8>>, [<<" ">>]}
    , <<"UpMchtNo">> => {<<"结算商户编号"/utf8>>, [<<" ">>]}
    , <<"TermId">> => {<<"交易终端号"/utf8>>, [<<" ">>]}
    , <<"TxnTypeChinese">> => {<<"交易类型"/utf8>>, [<<" ">>]}
    , <<"BankCardNo">> => {<<"卡号"/utf8>>, [<<" ">>]}
    , <<"TxnAmtWithDigits">> => <<"交易金额"/utf8>>
    , <<"FeeWithDigits">> => <<"商户手续费"/utf8>>
    , <<"RefId">> => {<<"参考号bit37"/utf8>>, [<<" ">>]}
    , <<"_TermSeq">> =>{<<"终端流水号bit11"/utf8>>, [<<" ">>]}
  }

    , delimit_field => [<<",">>]
    , delimit_line => [<<$\n>>]
    , skipTopLines => 1
    , skipEndLines => 0
    , headLine =>1

  }.

config3() ->
  #{
    field_map => #{
      <<"settleDate">> => <<"清算日期"/utf8>>
      , <<"txnDate">> => <<"交易日期"/utf8>>
      , <<"txnTime">> => <<"交易时间"/utf8>>
      , <<"txnAmt">> => <<"交易金额"/utf8>>
      , <<"req">> => <<"流水号"/utf8>>
    }
    , delimit_line => [<<$\r, $\n>>]
    , delimit_field => [<<"\t">>]
    , skipTopLines => 4
    , skipEndLines => 2
    , headLine =>4

  }.

config4() ->
  #{
    field_map => #{
      <<"settleDate">> => <<"清算日期"/utf8>>
      , <<"txnDate">> => <<"交易日期"/utf8>>
      , <<"txnTime">> => <<"交易时间"/utf8>>
      , <<"txnAmt">> => <<"交易金额"/utf8>>
      , <<"req">> => <<"流水号"/utf8>>
    }
    , delimit_line => [<<$\r, $\n>>]
    , delimit_field => [<<"\t">>]
    , skipTopLines => 4
    , skipEndLines => 2
    , headLine =>4
  }.

config5() ->
  #{field_map => #{
    <<"shanghuhao">> => {<<"       商户号       "/utf8>>, [<<" ">>]}
    , <<"txnDate">> => {<<"   交易日期   "/utf8>>, [<<" ">>]}
    , <<"txnTime">> => {<<"   交易时间   "/utf8>>, [<<" ">>]}
    , <<"txnAmt">> => {<<"    交易金额    "/utf8>>, [<<" ">>, <<"\,">>]}
    , <<"req">> => {<<"    流水号    "/utf8>>, [<<" ">>]}
  }
    , delimit_line => [<<"\n">>]
    , delimit_field => [<<226, 148, 130>>]
    , skipTopLines => 6
    , skipEndLines => 4
    , separation_line => 6
    , headLine =>5
  }.

config6() ->
  #{field_map => #{
    <<"traceNo">> => {28, 6},
    <<"txnTime">> => {35, 10},
    <<"cardNo">> => {46, 19},
    <<"txnAmt">> => {66, 12},
    <<"queryId">> => {87, 21},
    <<"orderId">> => {112, 32},
    <<"origTraceNo">> => {148, 6},
    <<"origTxnTime">> => {155, 10},
    <<"settleAmt">> => {180, 13},
    <<"txnType">> => {215, 2},
    <<"origQueryId">> => {271, 21},
    <<"merId">> => {293, 15},
    <<"TermId">> => {388, 8},
    <<"MerReserved">> => {397, 32},
    <<"origOrderId">> => {475, 32}
  }
    , delimit_line => [<<$\r, $\n>>]
    , skipTopLines => 0
    , skipEndLines => 0
  }.


read_line_test() ->
  F = fun(Binary) ->
    Lists = binary:split(Binary, [<<$\n>>], [global, trim]),
    lager:debug("length:~p", [length(Lists)])
      end,
  csv_parser:read_line_fold(F,"/mnt/d/test.txt", 10).



netbank_test() ->
  Config = config3(),
  {ok, Bin} = file:read_file("/mnt/d/csv/20170710.txt.netbank"),
  L = csv_parser:parse(Config, Bin),
  lager:debug("length(L):~p", [length(L)]),
  L.

wap_test() ->
  Config = config4(),
  {ok, Bin} = file:read_file("/mnt/d/csv/finance.20170705.txt.wap"),
  %{ok,Bin} = file:read_file("/mnt/d/csv/20170625.txt.wap"),
  L = csv_parser:parse(Config, Bin),
  lager:debug("length(L):~p", [length(L)]),
  L.

csv_test3() ->
  Config = config(),
  {ok, Bin} = file:read_file("/mnt/d/csv/1234.csv"),
  L = csv_parser:parse(Config, Bin),
  lager:debug("length(L):~p", [length(L)]),
  L.



p_jif1_test() ->
  Config = config5(),
  {ok, Bin} = file:read_file("/mnt/d/csv/p-jif1.txt"),
  L = csv_parser:parse(Config, Bin),
  lager:debug("length(L):~p", [length(L)]),
  L.

inn_test() ->
  Config = config6(),
  {ok, Bin} = file:read_file("/mnt/d/csv/INN17071888ZM_898319849000019"),
  L = csv_parser:parse(Config, Bin),
  lager:debug("length(L):~p", [length(L)]),
  L.