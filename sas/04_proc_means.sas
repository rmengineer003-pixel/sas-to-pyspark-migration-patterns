proc means data=work.txn noprint nway;
  class region;
  var amount;
  output out=work.region_stats n=n mean=mean_amt std=std_amt min=min_amt max=max_amt;
run;
