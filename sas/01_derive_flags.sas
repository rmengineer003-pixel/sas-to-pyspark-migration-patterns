/* Derived columns with IF/THEN/ELSE */
data work.txn_flagged;
  set work.txn;
  length amount_band $6;
  if amount >= 1000 then amount_band = 'HIGH';
  else if amount >= 100 then amount_band = 'MEDIUM';
  else amount_band = 'LOW';
  is_weekend = (weekday(txn_date) in (1, 7));
run;
