proc freq data=work.txn noprint;
  tables channel / out=work.channel_freq;   /* COUNT and PERCENT */
run;
