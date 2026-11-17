/* Match-merge with IN= flags to classify matches */
proc sort data=work.accounts; by account_id; run;
proc sort data=work.txn_totals; by account_id; run;
data work.acct_match;
  merge work.accounts(in=a) work.txn_totals(in=t);
  by account_id;
  length match_status $12;
  if a and t then match_status = 'BOTH';
  else if a then match_status = 'ACCOUNT_ONLY';
  else match_status = 'TXN_ONLY';
run;
