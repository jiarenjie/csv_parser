%%%-------------------------------------------------------------------
%%% @author jiarj
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. 六月 2017 14:07
%%%-------------------------------------------------------------------
-module(t_cav_parse).
-compile(export_all).
-include_lib("stdlib/include/qlc.hrl").
-author("jiarj").

%% API
-export([backup/1,restore/1]).




to_mode(X, Fields, Config, Operate) ->
  F = fun out_2_model_one_field/2,
  {VL, _, _, _} = lists:foldl(F, {[], Config, X, Operate}, Fields),
  VL
.
out_2_model_one_field(Field, {Acc, Model2OutMap, PL, Operate}) when is_atom(Field), is_list(Acc), is_map(Model2OutMap) ->
  Config = maps:get(Field, Model2OutMap),

%%  lager:debug("Config=~p,Field=~p", [Config, Field]),

  Value = do_out_2_model_one_field({maps:get(Field, PL), Config}, Operate),
  %% omit undefined key/value , which means not appear in PL

  AccNew = [{Field, Value} | Acc],
  {AccNew, Model2OutMap, PL, Operate}.

do_out_2_model_one_field({undefined, _}, write) ->
  <<"undefined">>;
do_out_2_model_one_field({Value, binary}, write) ->
  Value;
do_out_2_model_one_field({<<"undefined">>, _}, save) ->
  undefined;
do_out_2_model_one_field({Value, binary}, save) ->
  Value;

do_out_2_model_one_field({Value, integer}, write) ->
  integer_to_binary(Value);
do_out_2_model_one_field({Value, integer}, save) ->
  binary_to_integer(Value);

do_out_2_model_one_field({Value, atom}, write) ->
  atom_to_binary(Value, utf8);
do_out_2_model_one_field({Value, atom}, save) ->
  binary_to_atom(Value, utf8);

do_out_2_model_one_field({Value, F}, write) when is_function(F) ->
  F(Value, write);
do_out_2_model_one_field({Value, F}, save) when is_function(F) ->
  F(Value, save).
%%================================================================================================

table_deal_config(repo_mchants_pt) ->
  #{id => integer,
    mcht_full_name => binary,
    mcht_short_name => binary,
    payment_method =>
    fun(Value, O) ->
      case O of
        write ->
          [Payment_method] = Value,
          t_cav_parse:do_out_2_model_one_field({Payment_method, atom}, O);
        save ->
          Payment_method = t_cav_parse:do_out_2_model_one_field({Value, atom}, O),
          [Payment_method]
      end
    end
    ,
    quota =>
    fun(Value, O) ->
      case O of
        write ->
          [{txn, Txn}, {daily, Daily}, {monthly, Monthly}] = Value,
          Txn_binary = t_cav_parse:do_out_2_model_one_field({Txn, integer}, O),
          Daily_binary = t_cav_parse:do_out_2_model_one_field({Daily, integer}, O),
          Monthly_binary = t_cav_parse:do_out_2_model_one_field({Monthly, integer}, O),
          <<Txn_binary/binary, "\,", Daily_binary/binary, "\,", Monthly_binary/binary>>;
        save ->
          [Txn, Daily, Monthly] = binary:split(Value, [<<"\,">>], [global]),
          Txn_integer = t_cav_parse:do_out_2_model_one_field({Txn, integer}, O),
          Daily_integer = t_cav_parse:do_out_2_model_one_field({Daily, integer}, O),
          Monthly_integer = t_cav_parse:do_out_2_model_one_field({Monthly, integer}, O),
          [{txn, Txn_integer}, {daily, Daily_integer}, {monthly, Monthly_integer}]
      end
    end,
    status => atom,
    up_mcht_id => binary,
    up_term_no => binary,
    update_ts =>
    fun(Value, O) ->
      case O of
        write ->
          {Time1, Time2, Time3} = Value,
          Time1_binary = t_cav_parse:do_out_2_model_one_field({Time1, integer}, O),
          Time2_binary = t_cav_parse:do_out_2_model_one_field({Time2, integer}, O),
          Tim3_binary = t_cav_parse:do_out_2_model_one_field({Time3, integer}, O),
          <<Time1_binary/binary, "\,", Time2_binary/binary, "\,", Tim3_binary/binary>>;
        save ->
          [Time1, Time2, Time3] = binary:split(Value, [<<"\,">>], [global]),
          Time1_integer = t_cav_parse:do_out_2_model_one_field({Time1, integer}, O),
          Time2_integer = t_cav_parse:do_out_2_model_one_field({Time2, integer}, O),
          Tim3_integer = t_cav_parse:do_out_2_model_one_field({Time3, integer}, O),
          {Time1_integer, Time2_integer, Tim3_integer}
      end
    end
  };

table_deal_config(repo_mcht_txn_log_pt) ->
  #{mcht_index_key =>
  fun(Value, O) ->
    case O of
      write ->
        {Mcht_id, Mcht_txn_date, Mcht_txn_seq} = Value,
        B_Mcht_id = t_cav_parse:do_out_2_model_one_field({Mcht_id, binary}, O),
        B_Mcht_txn_date = t_cav_parse:do_out_2_model_one_field({Mcht_txn_date, binary}, O),
        B_Mcht_txn_seq = t_cav_parse:do_out_2_model_one_field({Mcht_txn_seq, binary}, O),
        <<B_Mcht_id/binary, "\,", B_Mcht_txn_date/binary, "\,", B_Mcht_txn_seq/binary>>;
      save ->
        [Mcht_id, Mcht_txn_date, Mcht_txn_seq] = binary:split(Value, [<<"\,">>], [global]),
        B_Mcht_id = t_cav_parse:do_out_2_model_one_field({Mcht_id, binary}, O),
        B_Mcht_txn_date = t_cav_parse:do_out_2_model_one_field({Mcht_txn_date, binary}, O),
        B_Mcht_txn_seq = t_cav_parse:do_out_2_model_one_field({Mcht_txn_seq, binary}, O),
        {B_Mcht_id, B_Mcht_txn_date, B_Mcht_txn_seq}
    end
  end,
    txn_type => atom,
    mcht_id => binary,
    mcht_txn_date => binary,
    mcht_txn_time => binary,
    mcht_txn_seq => binary,
    mcht_txn_amt => integer,
    mcht_order_desc => binary,
    gateway_id => binary,
    bank_id => binary,
    prod_id => binary,
    prod_bank_acct_id => binary,
    prod_bank_acct_corp_name => binary,
    prod_bank_name => binary,
    mcht_back_url => binary,
    mcht_front_url => binary,
    prod_memo => binary,

    query_id => binary,
    settle_date => binary,
    quota => integer,
    resp_code => binary,
    resp_msg => binary,

    orig_mcht_txn_date => binary,
    orig_mcht_txn_seq => binary,
    orig_query_id => binary,

    txn_status => atom,
    bank_card_no => binary
  };
table_deal_config(repo_up_txn_log_pt) ->
  #{mcht_index_key =>
  fun(Value, O) ->
    case O of
      write ->
        {Mcht_id, Mcht_txn_date, Mcht_txn_seq} = Value,
        B_Mcht_id = t_cav_parse:do_out_2_model_one_field({Mcht_id, binary}, O),
        B_Mcht_txn_date = t_cav_parse:do_out_2_model_one_field({Mcht_txn_date, binary}, O),
        B_Mcht_txn_seq = t_cav_parse:do_out_2_model_one_field({Mcht_txn_seq, binary}, O),
        <<B_Mcht_id/binary, "\,", B_Mcht_txn_date/binary, "\,", B_Mcht_txn_seq/binary>>;
      save ->
        [Mcht_id, Mcht_txn_date, Mcht_txn_seq] = binary:split(Value, [<<"\,">>], [global]),
        B_Mcht_id = t_cav_parse:do_out_2_model_one_field({Mcht_id, binary}, O),
        B_Mcht_txn_date = t_cav_parse:do_out_2_model_one_field({Mcht_txn_date, binary}, O),
        B_Mcht_txn_seq = t_cav_parse:do_out_2_model_one_field({Mcht_txn_seq, binary}, O),
        {B_Mcht_id, B_Mcht_txn_date, B_Mcht_txn_seq}
    end
  end,
    txn_type => atom,

    up_merId => binary,
    up_txnTime => binary,
    up_orderId => binary,
    up_txnAmt => integer,
    up_reqReserved => binary,
    up_orderDesc => binary,
    up_issInsCode => binary,
    up_index_key =>
    fun(Value, O) ->
      case O of
        write ->
          {Mcht_id, Mcht_txn_date, Mcht_txn_seq} = Value,
          B_Mcht_id = t_cav_parse:do_out_2_model_one_field({Mcht_id, binary}, O),
          B_Mcht_txn_date = t_cav_parse:do_out_2_model_one_field({Mcht_txn_date, binary}, O),
          B_Mcht_txn_seq = t_cav_parse:do_out_2_model_one_field({Mcht_txn_seq, binary}, O),
          <<B_Mcht_id/binary, "\,", B_Mcht_txn_date/binary, "\,", B_Mcht_txn_seq/binary>>;
        save ->
          [Mcht_id, Mcht_txn_date, Mcht_txn_seq] = binary:split(Value, [<<"\,">>], [global]),
          B_Mcht_id = t_cav_parse:do_out_2_model_one_field({Mcht_id, binary}, O),
          B_Mcht_txn_date = t_cav_parse:do_out_2_model_one_field({Mcht_txn_date, binary}, O),
          B_Mcht_txn_seq = t_cav_parse:do_out_2_model_one_field({Mcht_txn_seq, binary}, O),
          {B_Mcht_id, B_Mcht_txn_date, B_Mcht_txn_seq}
      end
    end,
    up_queryId => binary,
    up_respCode => binary,
    up_respMsg => binary,
    up_settleAmt => integer,
    up_settleDate => binary,
    up_traceNo => binary,
    up_traceTime => binary,

    up_query_index_key =>
    fun(Value, O) ->
      case O of
        write ->
          {Mcht_id, Mcht_txn_date, Mcht_txn_seq} = Value,
          B_Mcht_id = t_cav_parse:do_out_2_model_one_field({Mcht_id, binary}, O),
          B_Mcht_txn_date = t_cav_parse:do_out_2_model_one_field({Mcht_txn_date, binary}, O),
          B_Mcht_txn_seq = t_cav_parse:do_out_2_model_one_field({Mcht_txn_seq, binary}, O),
          <<B_Mcht_id/binary, "\,", B_Mcht_txn_date/binary, "\,", B_Mcht_txn_seq/binary>>;
        save ->
          [Mcht_id, Mcht_txn_date, Mcht_txn_seq] = binary:split(Value, [<<"\,">>], [global]),
          B_Mcht_id = t_cav_parse:do_out_2_model_one_field({Mcht_id, binary}, O),
          B_Mcht_txn_date = t_cav_parse:do_out_2_model_one_field({Mcht_txn_date, binary}, O),
          B_Mcht_txn_seq = t_cav_parse:do_out_2_model_one_field({Mcht_txn_seq, binary}, O),
          {B_Mcht_id, B_Mcht_txn_date, B_Mcht_txn_seq}
      end
    end,
    txn_status => atom,
    up_accNo => binary
  }.
table_read_config(repo_mchants_pt)->
    #{
      field_map => #{
        id => <<"column1">>
        , mcht_full_name => <<"column2">>
        , mcht_short_name => <<"column3">>
        , status => <<"column4">>
        , payment_method => <<"column5">>
        , up_mcht_id => <<"column6">>
        , quota => <<"column7">>
        , up_term_no => <<"column8">>
        , update_ts => <<"column9">>
      }
      , delimit_field => [<<$^,$^>>]
      , delimit_line => [<<$$,$\n>>]
    };
table_read_config(repo_mcht_txn_log_pt)->
  #{
    field_map => #{

      mcht_index_key => <<"column1">>
      , txn_type => <<"column2">>
      , mcht_id => <<"column3">>
      , mcht_txn_date => <<"column4">>
      , mcht_txn_time => <<"column5">>
      , mcht_txn_seq => <<"column6">>
      , mcht_txn_amt => <<"column7">>
      , mcht_order_desc => <<"column8">>
      , gateway_id => <<"column9">>
      , bank_id => <<"column10">>
      , prod_id => <<"column11">>
      , prod_bank_acct_id => <<"column12">>
      , prod_bank_acct_corp_name => <<"column13">>
      , prod_bank_name => <<"column14">>
      , mcht_back_url => <<"column15">>
      , mcht_front_url => <<"column16">>
      , prod_memo => <<"column17">>

      , query_id => <<"column18">>
      , settle_date => <<"column19">>
      , quota => <<"column20">>
      , resp_code => <<"column21">>
      , resp_msg => <<"column22">>

      , orig_mcht_txn_date => <<"column23">>
      , orig_mcht_txn_seq => <<"column24">>
      , orig_query_id => <<"column25">>

      , txn_status => <<"column26">>
      , bank_card_no => <<"column27">>
    }
    , delimit_field => [<<$^,$^>>]
    , delimit_line => [<<$$,$\n>>]
  };
table_read_config(repo_up_txn_log_pt)->
  #{
    field_map => #{

      mcht_index_key => <<"column1">>
      , txn_type => <<"column2">>

      , up_merId => <<"column3">>
      , up_txnTime => <<"column4">>
      , up_orderId => <<"column5">>
      , up_txnAmt => <<"column6">>
      , up_reqReserved => <<"column7">>
      , up_orderDesc => <<"column8">>
      , up_issInsCode => <<"column9">>
      , up_index_key => <<"column10">>

      , up_queryId => <<"column11">>
      , up_respCode => <<"column12">>
      , up_respMsg => <<"column13">>
      , up_settleAmt => <<"column14">>
      , up_settleDate => <<"column15">>
      , up_traceNo => <<"column16">>
      , up_traceTime => <<"column17">>

      , up_query_index_key => <<"column18">>

      , txn_status => <<"column19">>
      , up_accNo => <<"column20">>
    }
    , delimit_field => [<<$^,$^>>]
    , delimit_line => [<<$$,$\n>>]
  }.


backup(M)->
%%  Lists = behaviour_repo:get_all(M),
  QH = behaviour_repo:qh(M, [{filter, []}]),
  Fields = utils_recop:fields(M),
  Config = table_deal_config(M),
  Table_read_config = table_read_config(M),
  Delimit_field  = maps:get(delimit_field,Table_read_config),
  Delimit_line  = maps:get(delimit_line,Table_read_config),
  FileName = "/mnt/d/csv/"++atom_to_list(M)++".txt",
  file:write_file(FileName, [], [write]),

  LinesGap = 500,
  F = fun
        (Repo, {N, Acc, Total}) when N >= LinesGap ->
          %% reach write threshold
          %% dump this to file
          lager:info("Write ~p lines to file:~ts", [Total, FileName]),
          delimited_reconcile_file:write_to_file(FileName, Acc, Fields,Delimit_field,Delimit_line, [append]),
          %% initial new empty acc
          {1, [repo_to_mode(Repo, M, Fields, Config, write)], Total + N};
        (Repo, {N, Acc, Total}) ->
          {N + 1, [repo_to_mode(Repo, M, Fields, Config, write) | Acc], Total}
      end,

  F1 = fun()->
    qlc:fold(F, {0, [], 0}, QH)
       end,
  {atomic, {N, Rest, SubTotal}} = mnesia:transaction(F1),
  lager:info("Write ~p lines to file:~ts", [SubTotal + N, FileName]),
  delimited_reconcile_file:write_to_file(FileName, Rest, Fields,Delimit_field,Delimit_line, [append]).

repo_to_mode(Repo, M, Fields, Config, Write) ->
  Model = utils_recop:to_model(M, Repo),
  to_mode(Model, Fields, Config, Write).

restore(M)->
  Config = table_read_config(M),
  Fields = utils_recop:fields(M),
  F = fun(Bin) ->
    Lists = delimited_reconcile_file:parse(Config, Bin),
    Config2 = t_cav_parse:table_deal_config(M),
    F2 = fun(Repo, Acc) ->
      Mode = to_mode(Repo, Fields, Config2, save),
%%      lager:debug("Mode:~p",[Mode]),
      behaviour_repo:save(M, maps:from_list(Mode), [dirty])
        end,
    lists:foldl(F2, [], Lists)
      end,

  FileName = "/mnt/d/csv/"++atom_to_list(M)++".txt",
  delimited_reconcile_file:read_line_fold(F,FileName, 500).




