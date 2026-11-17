/* Days since previous transaction for the same account */
proc sort data=work.txn; by account_id txn_date txn_id; run;
data work.txn_gap;
  set work.txn;
  by account_id;
  prev_date = lag(txn_date);
  if first.account_id then prev_date = .;
  days_since_prev = txn_date - prev_date;
run;
