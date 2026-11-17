/* Keep the first record per key after sorting */
proc sort data=work.txn out=work.txn_first nodupkey;
  by account_id;
run;
/* Note: NODUPKEY keeps the first row in the incoming order for each key.
   Sort by account_id txn_date txn_id first to make it deterministic. */
