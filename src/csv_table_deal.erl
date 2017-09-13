%%%-------------------------------------------------------------------
%%% @author jiarj
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. 六月 2017 14:07
%%%-------------------------------------------------------------------
-module(csv_table_deal).
-compile(export_all).
-include_lib("stdlib/include/qlc.hrl").
-author("jiarj").
-behaviour(gen_server).

%% API
-export([backup/2, restore/2, start_link/0]).

-define(SERVER, ?MODULE).
-record(state, {f_qh, f_fields, f_model, f_save}).


start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


init([]) ->
  {ok, F_qh} = application:get_env(f_qh),
  {ok, F_fields} = application:get_env(f_fields),
  {ok, F_save} = application:get_env(f_save),
  {ok, F_model} = application:get_env(f_model),

  {ok, #state{f_qh = F_qh, f_fields = F_fields, f_save = F_save, f_model = F_model}}.


restore(M, FileName) ->
  gen_server:cast(?SERVER, {restore, M, FileName}).

backup(M, FileName) ->
  gen_server:cast(?SERVER, {backup, M, FileName}).

handle_call(_Request, _From, State) ->
  {noreply, State}.

handle_cast({backup, M, FileName}, #state{f_qh = {M_qh, F_qh}, f_fields = {M_fields, F_fields}, f_model = {M_model, F_model}} = State) ->
  QH = apply(M_qh, F_qh, [M, [{filter, []}]]),
  Fields = apply(M_fields, F_fields, [M]),
  Config = table_deal_config(M),
  Table_read_config = table_read_config(M),
  Delimit_field = maps:get(delimit_field, Table_read_config),
  Delimit_line = maps:get(delimit_line, Table_read_config),
%%  FileName = "/mnt/d/csv/"++atom_to_list(M)++".txt",
  file:write_file(FileName, [], [write]),
  LinesGap = 500,
  lager:info("backup table: ~p from file : ~ts start", [M , FileName]),
  F_repo_to_mode =
    fun(Repo, M0, Fields, Config, Write) ->
      Model = apply(M_model, F_model, [M0, Repo]),
%%    Model = utils_recop:to_model(M, Repo),
      to_mode(Model, Fields, Config, Write)
    end,

  F = fun
        (Repo, {N, Acc, Total}) when N >= LinesGap ->
          %% reach write threshold
          %% dump this to file
          lager:debug("Write ~p lines to file:~ts", [Total, FileName]),
          csv_parser:write_to_file(FileName, Acc, Fields, Delimit_field, Delimit_line, [append]),
          %% initial new empty acc
          {1, [apply(F_repo_to_mode, [Repo, M, Fields, Config, write])], Total + N};
        (Repo, {N, Acc, Total}) ->
          {N + 1, [apply(F_repo_to_mode, [Repo, M, Fields, Config, write]) | Acc], Total}
      end,

  F1 = fun() ->
    qlc:fold(F, {0, [], 0}, QH)
       end,
  {atomic, {N, Rest, SubTotal}} = mnesia:transaction(F1),
  lager:debug("Write ~p lines to file:~ts", [SubTotal + N, FileName]),
  csv_parser:write_to_file(FileName, Rest, Fields, Delimit_field, Delimit_line, [append]),
  lager:info("Write table: ~p to file : ~ts success,total: ~p", [M , FileName,SubTotal + N]),
  {noreply, State};
handle_cast({restore, M, FileName}, #state{f_fields = {M_Field, F_Field}, f_save = {M_save, F_save}} = State) ->
  Config = table_read_config(M),
  Fields = apply(M_Field, F_Field, [M]),
  lager:info("restore table: ~p to file :~ts start",[M , FileName ]),
  F = fun(Bin) ->
    Lists = csv_parser:parse(Config, Bin),
    Config2 = csv_table_deal:table_deal_config(M),
    F2 = fun(Repo, Acc) ->
      Mode = to_mode(Repo, Fields, Config2, save),
%%      lager:debug("Mode:~p",[Mode]),
      apply(M_save, F_save, [M, maps:from_list(Mode), [dirty]])
%%      behaviour_repo:save(M, maps:from_list(Mode), [dirty])
         end,
    lists:foldl(F2, [], Lists)
      end,
%%  FileName = "/mnt/d/csv/"++atom_to_list(M)++".txt",
  Total = csv_parser:read_line_fold(F, FileName, 500),
  lager:info("restore table: ~p to file : ~ts success,total: ~p",[M , FileName,Total]),
  {noreply, State};
handle_cast(_Request, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(OldVsn, State, _Extra) ->
  {ok, State}.




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
do_out_2_model_one_field({<<"undefined">>, _}, save) ->
  undefined;
do_out_2_model_one_field({Value, binary}, write) ->
  Value;
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

do_out_2_model_one_field({Value, ts}, write) ->
  {Time1, Time2, Time3} = Value,
  Txn_binary = csv_table_deal:do_out_2_model_one_field({Time1, integer}, write),
  Daily_binary = csv_table_deal:do_out_2_model_one_field({Time2, integer}, write),
  Monthly_binary = csv_table_deal:do_out_2_model_one_field({Time3, integer}, write),
  <<Txn_binary/binary, "\,", Daily_binary/binary, "\,", Monthly_binary/binary>>;
do_out_2_model_one_field({Value, ts}, save) ->
  [Time1, Time2, Time3] = binary:split(Value, [<<"\,">>], [global]),
  Time1_integer = csv_table_deal:do_out_2_model_one_field({Time1, integer}, save),
  Time2_integer = csv_table_deal:do_out_2_model_one_field({Time2, integer}, save),
  Tim3_integer = csv_table_deal:do_out_2_model_one_field({Time3, integer}, save),
  {Time1_integer, Time2_integer, Tim3_integer};

do_out_2_model_one_field({Value, F}, write) when is_function(F) ->
  F(Value, write);
do_out_2_model_one_field({Value, F}, save) when is_function(F) ->
  F(Value, save).
%%================================================================================================


table_deal_config(repo_history_mcht_txn_log_pt) ->
  table_deal_config(repo_mcht_txn_log_pt);
table_deal_config(repo_history_up_txn_log_pt) ->
  table_deal_config(repo_up_txn_log_pt);
table_deal_config(repo_history_ums_reconcile_result_pt)->
  table_deal_config(repo_ums_reconcile_result_pt);
table_deal_config(repo_ums_reconcile_result_pt) ->
  #{
    id =>
    fun(Value, O) ->
      case O of
        write ->
          {Time1, Time2, Time3} = Value,
          Time1_binary = csv_table_deal:do_out_2_model_one_field({Time1, binary}, O),
          Time2_binary = csv_table_deal:do_out_2_model_one_field({Time2, binary}, O),
          Tim3_binary = csv_table_deal:do_out_2_model_one_field({Time3, binary}, O),
          <<Time1_binary/binary, "\,", Time2_binary/binary, "\,", Tim3_binary/binary>>;
        save ->
          [Txn_date, Txn_time, Tys_trace_no] = binary:split(Value, [<<"\,">>], [global]),
          Txn_date_r = csv_table_deal:do_out_2_model_one_field({Txn_date, binary}, O),
          Txn_time_r = csv_table_deal:do_out_2_model_one_field({Txn_time, binary}, O),
          Tys_trace_no_r = csv_table_deal:do_out_2_model_one_field({Tys_trace_no, binary}, O),
          {Txn_date_r, Txn_time_r, Tys_trace_no_r}
      end
    end
    , settlement_date => binary
    , txn_date => binary
    , txn_time => binary
    , ums_mcht_id => integer
    , term_id => binary
    , bank_card_no => binary
    , txn_amt => integer
    , txn_type =>
        fun
          (Value, O) when is_atom(Value) ->
          csv_table_deal:do_out_2_model_one_field({Value,atom},O);
          (Value, O) when is_binary(Value)->
            case O of
              write ->
                case Value of
                  <<"E74">> ->
                    <<"refund">>;
                  _ ->
                    <<"pay">>
                end;
              save ->
                binary_to_atom(Value, utf8)
            end

        end
    , txn_fee => integer
    , term_batch_no => binary
    , term_seq => binary
    , sys_trace_no => binary
    , ref_id => binary
    , auth_resp_code => binary
    , cooperate_fee => binary
    , cooperate_mcht_id => binary
    , up_txn_seq => binary
    , ums_order_id => binary
    , memo => binary
  };
table_deal_config(repo_mcht_txn_acc_pt) ->
  #{
    acc_index =>
    fun(Value, O) ->
      case O of
        write ->
          {Mcht_id, Txn_type, Month_date} = Value,
          Mcht_id_w = csv_table_deal:do_out_2_model_one_field({Mcht_id, integer}, O),
          Txn_type_w = csv_table_deal:do_out_2_model_one_field({Txn_type, atom}, O),
          Month_date_w = csv_table_deal:do_out_2_model_one_field({Month_date, binary}, O),

          <<Mcht_id_w/binary, "\,", Txn_type_w/binary, "\,", Month_date_w/binary>>;
        save ->
          [Mcht_id, Txn_type, Month_date] = binary:split(Value, [<<"\,">>], [global]),
          Mcht_id_r = csv_table_deal:do_out_2_model_one_field({Mcht_id, integer}, O),
          Txn_type_r = csv_table_deal:do_out_2_model_one_field({Txn_type, atom}, O),
          Month_date_r = csv_table_deal:do_out_2_model_one_field({Month_date, binary}, O),
          {Mcht_id_r, Txn_type_r, Month_date_r}
      end
    end,
    mcht_id => integer,
    txn_type => atom,
    month_date => binary,
    acc => integer
  };
table_deal_config(repo_backend_users_pt) ->
  #{
    id => integer,
    name => binary,
    email => binary,
    password => binary,
    role => atom,
    status => atom,
    last_update_ts => ts,
    last_login_ts => ts
  };
table_deal_config(repo_mchants_pt) ->
  #{id => integer,
    mcht_full_name => binary,
    mcht_short_name => binary,
    payment_method =>
    fun(Value, O) ->
      case O of
        write ->
          [Payment_method] = Value,
          csv_table_deal:do_out_2_model_one_field({Payment_method, atom}, O);
        save ->
          Payment_method = csv_table_deal:do_out_2_model_one_field({Value, atom}, O),
          [Payment_method]
      end
    end
    ,
    quota =>
    fun(Value, O) ->
      case O of
        write ->
          [{txn, Txn}, {daily, Daily}, {monthly, Monthly}] = Value,
          Txn_binary = csv_table_deal:do_out_2_model_one_field({Txn, integer}, O),
          Daily_binary = csv_table_deal:do_out_2_model_one_field({Daily, integer}, O),
          Monthly_binary = csv_table_deal:do_out_2_model_one_field({Monthly, integer}, O),
          <<Txn_binary/binary, "\,", Daily_binary/binary, "\,", Monthly_binary/binary>>;
        save ->
          [Txn, Daily, Monthly] = binary:split(Value, [<<"\,">>], [global]),
          Txn_integer = csv_table_deal:do_out_2_model_one_field({Txn, integer}, O),
          Daily_integer = csv_table_deal:do_out_2_model_one_field({Daily, integer}, O),
          Monthly_integer = csv_table_deal:do_out_2_model_one_field({Monthly, integer}, O),
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
          Time1_binary = csv_table_deal:do_out_2_model_one_field({Time1, integer}, O),
          Time2_binary = csv_table_deal:do_out_2_model_one_field({Time2, integer}, O),
          Tim3_binary = csv_table_deal:do_out_2_model_one_field({Time3, integer}, O),
          <<Time1_binary/binary, "\,", Time2_binary/binary, "\,", Tim3_binary/binary>>;
        save ->
          [Time1, Time2, Time3] = binary:split(Value, [<<"\,">>], [global]),
          Time1_integer = csv_table_deal:do_out_2_model_one_field({Time1, integer}, O),
          Time2_integer = csv_table_deal:do_out_2_model_one_field({Time2, integer}, O),
          Tim3_integer = csv_table_deal:do_out_2_model_one_field({Time3, integer}, O),
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
        B_Mcht_id = csv_table_deal:do_out_2_model_one_field({Mcht_id, binary}, O),
        B_Mcht_txn_date = csv_table_deal:do_out_2_model_one_field({Mcht_txn_date, binary}, O),
        B_Mcht_txn_seq = csv_table_deal:do_out_2_model_one_field({Mcht_txn_seq, binary}, O),
        <<B_Mcht_id/binary, "\,", B_Mcht_txn_date/binary, "\,", B_Mcht_txn_seq/binary>>;
      save ->
        [Mcht_id, Mcht_txn_date, Mcht_txn_seq] = binary:split(Value, [<<"\,">>], [global]),
        B_Mcht_id = csv_table_deal:do_out_2_model_one_field({Mcht_id, binary}, O),
        B_Mcht_txn_date = csv_table_deal:do_out_2_model_one_field({Mcht_txn_date, binary}, O),
        B_Mcht_txn_seq = csv_table_deal:do_out_2_model_one_field({Mcht_txn_seq, binary}, O),
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
        B_Mcht_id = csv_table_deal:do_out_2_model_one_field({Mcht_id, binary}, O),
        B_Mcht_txn_date = csv_table_deal:do_out_2_model_one_field({Mcht_txn_date, binary}, O),
        B_Mcht_txn_seq = csv_table_deal:do_out_2_model_one_field({Mcht_txn_seq, binary}, O),
        <<B_Mcht_id/binary, "\,", B_Mcht_txn_date/binary, "\,", B_Mcht_txn_seq/binary>>;
      save ->
        [Mcht_id, Mcht_txn_date, Mcht_txn_seq] = binary:split(Value, [<<"\,">>], [global]),
        B_Mcht_id = csv_table_deal:do_out_2_model_one_field({Mcht_id, binary}, O),
        B_Mcht_txn_date = csv_table_deal:do_out_2_model_one_field({Mcht_txn_date, binary}, O),
        B_Mcht_txn_seq = csv_table_deal:do_out_2_model_one_field({Mcht_txn_seq, binary}, O),
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
          B_Mcht_id = csv_table_deal:do_out_2_model_one_field({Mcht_id, binary}, O),
          B_Mcht_txn_date = csv_table_deal:do_out_2_model_one_field({Mcht_txn_date, binary}, O),
          B_Mcht_txn_seq = csv_table_deal:do_out_2_model_one_field({Mcht_txn_seq, binary}, O),
          <<B_Mcht_id/binary, "\,", B_Mcht_txn_date/binary, "\,", B_Mcht_txn_seq/binary>>;
        save ->
          [Mcht_id, Mcht_txn_date, Mcht_txn_seq] = binary:split(Value, [<<"\,">>], [global]),
          B_Mcht_id = csv_table_deal:do_out_2_model_one_field({Mcht_id, binary}, O),
          B_Mcht_txn_date = csv_table_deal:do_out_2_model_one_field({Mcht_txn_date, binary}, O),
          B_Mcht_txn_seq = csv_table_deal:do_out_2_model_one_field({Mcht_txn_seq, binary}, O),
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
          B_Mcht_id = csv_table_deal:do_out_2_model_one_field({Mcht_id, binary}, O),
          B_Mcht_txn_date = csv_table_deal:do_out_2_model_one_field({Mcht_txn_date, binary}, O),
          B_Mcht_txn_seq = csv_table_deal:do_out_2_model_one_field({Mcht_txn_seq, binary}, O),
          <<B_Mcht_id/binary, "\,", B_Mcht_txn_date/binary, "\,", B_Mcht_txn_seq/binary>>;
        save ->
          [Mcht_id, Mcht_txn_date, Mcht_txn_seq] = binary:split(Value, [<<"\,">>], [global]),
          B_Mcht_id = csv_table_deal:do_out_2_model_one_field({Mcht_id, binary}, O),
          B_Mcht_txn_date = csv_table_deal:do_out_2_model_one_field({Mcht_txn_date, binary}, O),
          B_Mcht_txn_seq = csv_table_deal:do_out_2_model_one_field({Mcht_txn_seq, binary}, O),
          {B_Mcht_id, B_Mcht_txn_date, B_Mcht_txn_seq}
      end
    end,
    txn_status => atom,
    up_accNo => binary
  }.

table_read_config(repo_history_mcht_txn_log_pt) ->
  table_read_config(repo_mcht_txn_log_pt);
table_read_config(repo_history_up_txn_log_pt) ->
  table_read_config(repo_up_txn_log_pt);
table_read_config(repo_history_ums_reconcile_result_pt) ->
  table_read_config(repo_ums_reconcile_result_pt);
table_read_config(repo_ums_reconcile_result_pt) ->
%%  [id,settlement_date,txn_date,txn_time,ums_mcht_id,term_id,
%%    bank_card_no,txn_amt,txn_type,txn_fee,term_batch_no,
%%    term_seq,sys_trace_no,ref_id,auth_resp_code,cooperate_fee,
%%    cooperate_mcht_id,up_txn_seq,ums_order_id,memo]
  #{
    field_map => #{
      id => <<"column1">>
      , settlement_date => <<"column2">>
      , txn_date => <<"column3">>
      , txn_time => <<"column4">>
      , ums_mcht_id => <<"column5">>
      , term_id => <<"column6">>

      , bank_card_no => <<"column7">>
      , txn_amt => <<"column8">>
      , txn_type => <<"column9">>
      , txn_fee => <<"column10">>
      , term_batch_no => <<"column11">>

      , term_seq => <<"column12">>
      , sys_trace_no => <<"column13">>
      , ref_id => <<"column14">>
      , auth_resp_code => <<"column15">>
      , cooperate_fee => <<"column16">>

      , cooperate_mcht_id => <<"column17">>
      , up_txn_seq => <<"column18">>
      , ums_order_id => <<"column19">>
      , memo => <<"column20">>
    }
    , delimit_field => [<<$^, $^>>]
    , delimit_line => [<<$$, $\n>>]
  };
table_read_config(repo_mcht_txn_acc_pt) ->
%%  [acc_index,mcht_id,txn_type,month_date,acc]
  #{
    field_map => #{
      acc_index => <<"column1">>
      , mcht_id => <<"column2">>
      , txn_type => <<"column3">>
      , month_date => <<"column4">>
      , acc => <<"column5">>
    }
    , delimit_field => [<<$^, $^>>]
    , delimit_line => [<<$$, $\n>>]
  };
table_read_config(repo_backend_users_pt) ->
%%  [id,name,email,password,role,status,last_update_ts, last_login_ts]
  #{
    field_map => #{
      id => <<"column1">>
      , name => <<"column2">>
      , email => <<"column3">>
      , password => <<"column4">>
      , role => <<"column5">>
      , status => <<"column6">>
      , last_update_ts => <<"column7">>
      , last_login_ts => <<"column8">>
    }
    , delimit_field => [<<$^, $^>>]
    , delimit_line => [<<$$, $\n>>]
  };
table_read_config(repo_mchants_pt) ->
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
    , delimit_field => [<<$^, $^>>]
    , delimit_line => [<<$$, $\n>>]
  };
table_read_config(repo_mcht_txn_log_pt) ->
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
    , delimit_field => [<<$^, $^>>]
    , delimit_line => [<<$$, $\n>>]
  };
table_read_config(repo_up_txn_log_pt) ->
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
    , delimit_field => [<<$^, $^>>]
    , delimit_line => [<<$$, $\n>>]
  }.









