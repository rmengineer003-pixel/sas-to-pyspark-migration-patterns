/* Running total and sequence within account using RETAIN and FIRST./LAST. */
proc sort data=work.txn; by account_id txn_date txn_id; run;
data work.txn_running;
  set work.txn;
  by account_id;
  retain running_amt txn_seq;
  if first.account_id then do; running_amt = 0; txn_seq = 0; end;
  running_amt + amount;
  txn_seq + 1;
  is_last_txn = last.account_id;
run;
