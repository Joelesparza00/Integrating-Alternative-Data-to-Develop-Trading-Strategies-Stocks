libname analysis "/home/u63744942/sasuser.v94/finalproject";

/* Import CSV file */
proc import datafile='/home/u63744942/sasuser.v94/finalproject/NasDaq100 Daily Returns.csv'
            out=russell2000
            dbms=csv replace;
run;

proc means data=russell2000;
run;

proc contents data=russell2000;
run;

proc print data = russell2000 (obs = 20);
run;

/* changing date */ 
data russell;
    set russell2000;
    date = input(date, yyyymmdd10.);
    format date date9.;
    drop HSICIG enterdate
run;
 
proc print data=russell (obs=20);
run;


/* Grouping and Aggregating by Week */
data russell;
    set russell;
    start_week = intnx('week', date, 0, 'B');
    format start_week date9.;
run;


proc sql;
    create table russell_weekly as
    select
        start_week,
        TICKER,
        SICCD,
        COMNAM,
        mean(market_cap) as mean_market_cap,
        mean(SHROUT) as mean_SHROUT,
        mean(VOL) as mean_VOL,
        mean(PRC) as mean_PRC,
        max(ASKHI) as max_ASKHI,
        min(BIDLO) as min_BIDLO,
        sum(total) as sum_total,
        sum(negative) as sum_negative,
        sum(positive) as sum_positive,
        sum(sprtrn) as sum_sprtrn,
        sum(RET) as sum_RET
    from
        russell
    group by
        TICKER, start_week;
quit;

proc sort data=russell_weekly nodupkey out=russell_week;
    by TICKER start_week;
run;

proc print data=russell_week (obs=20);
run;



/* Adding lagged Columns */ 
proc sort data=WORK.RUSSELL_WEEK;
  by TICKER start_week;
run;

data WORK.RUSSELL_WEEK_lagged;
  set WORK.RUSSELL_WEEK;
  by TICKER;
  if first.TICKER then lagged_return = .;
  lagged_return = lag(sum_RET);
  if first.TICKER then lagged_return = .;
  output;
run;

data russell_week;
	set russell_week_lagged;
	where lagged_return is not missing;
run;

proc print data=russell_week (obs=15);
run;



/* Weekly Aggregation */
proc sql;
    create table aggregated_data as
    select start_week,
           sum_sprtrn,
           sum(sum_positive) as sum_positive,
           sum(sum_negative) as sum_negative,
           sum(sum_total) as sum_total
    from russell_week
    group by start_week
    order by start_week;
quit;


proc sort data=aggregated_data nodupkey out=weekly_summary;
    by start_week;
run;

proc print data=weekly_summary;
run;


*******************************************************************;
/* This code will show us the amount of positive, negative and total news feed */
/* that is for each cluster of weekly returns based off weekly market performance*/
*******************************************************************;

/* Calculate percentiles of sprtrn when grouped by day*/
proc rank data=weekly_summary out=ranked_sprtrn groups=10 ties=mean;
  var sum_sprtrn;
  ranks sprtrn_percentile;
run;

data ranked_sprtrn;
  set ranked_sprtrn;
  length percentile_range $20;
  if sprtrn_percentile = 0 then percentile_range = 'Bottom 10%';
  else if sprtrn_percentile = 1 then percentile_range = '10-20%';
  else if sprtrn_percentile = 2 then percentile_range = '20-30%';
  else if sprtrn_percentile = 3 then percentile_range = '30-40%';
  else if sprtrn_percentile = 4 then percentile_range = '40-50%';
  else if sprtrn_percentile = 5 then percentile_range = '50-60%';
  else if sprtrn_percentile = 6 then percentile_range = '60-70%';
  else if sprtrn_percentile = 7 then percentile_range = '70-80%';
  else if sprtrn_percentile = 8 then percentile_range = '80-90%';
  else percentile_range = 'Top 10%';
run;

proc sql;
  create table percentile_summary as
  select 
      percentile_range,
      sum(sum_sprtrn) as sum_sprtn,
      sum(sum_positive) as total_positive,
      sum(sum_negative) as total_negative,
      sum(sum_total) as total
  from ranked_sprtrn
  group by percentile_range
  order by sum_sprtn desc;
quit;

proc print data=percentile_summary;
run;




*******************************************************************;
/* This code will show us the amount of positive, negative and total news feed */
/* that is for each cluster of daily returns based off individual stock performance*/
*******************************************************************;

/* Calculate percentiles of sprtrn when grouped by week*/
proc rank data=russell_week out=ranked_rtrn groups=10 ties=mean;
  var sum_ret;
  ranks rtrn_percentile;
run;

data ranked_rtrn;
  set ranked_rtrn;
  length percentile_range $20;
  if rtrn_percentile = 0 then percentile_range = 'Bottom 10%';
  else if rtrn_percentile = 1 then percentile_range = '10-20%';
  else if rtrn_percentile = 2 then percentile_range = '20-30%';
  else if rtrn_percentile = 3 then percentile_range = '30-40%';
  else if rtrn_percentile = 4 then percentile_range = '40-50%';
  else if rtrn_percentile = 5 then percentile_range = '50-60%';
  else if rtrn_percentile = 6 then percentile_range = '60-70%';
  else if rtrn_percentile = 7 then percentile_range = '70-80%';
  else if rtrn_percentile = 8 then percentile_range = '80-90%';
else percentile_range = 'Top 10%';
run;

proc sql;
  create table percentile_summary2 as
  select 
      percentile_range,
      avg(sum_ret) as avg_ret,
      avg(lagged_return) as avg_lag_ret,
      sum(sum_positive) as total_positive,
      sum(sum_negative) as total_negative,
      sum(sum_total) as total,
      sum(sum_positive) / sum(sum_total) as positive_perc,
      sum(sum_negative) / sum(sum_total) as negative_perc
  from ranked_rtrn
  group by percentile_range
  order by avg_ret desc;
quit;

proc print data=percentile_summary2;
run;

/* same thing but only for companies that recieved sentiment */
proc sql;
  create table percentile_summary3 as
  select 
      percentile_range,
      avg(sum_ret) as avg_ret,
      avg(lagged_return) as avg_lag_ret,
      sum(sum_positive) as total_positive,
      sum(sum_negative) as total_negative,
      sum(sum_total) as total,
      sum(sum_positive) / sum(sum_total) as positive_perc,
      sum(sum_negative) / sum(sum_total) as negative_perc
  from ranked_rtrn
  where sum_total > 0
  group by percentile_range
  order by avg_ret desc;
quit;

proc print data=percentile_summary3;
run;

*******************************************************************;
/* This code will show us the amount of positive, negative and total news feed */
/* that is for each cluster of daily returns based off industry*/
*******************************************************************;
proc sql;
  create table industry_summary as
  select 
      int(SICCD / 10) as industry,
      count(distinct TICKER) as num_firms,
      sum(sum_ret) as cum_ret,
 	  sum(sum_positive) as total_positive,
      sum(sum_negative) as total_negative,
      sum(sum_total) as total,
      sum(sum_positive) / sum(sum_total) as positive_perc,
      sum(sum_negative) / sum(sum_total) as negative_perc
  from russell_week
  group by industry
  order by cum_ret desc;
quit;

proc print data=industry_summary;
run;

/* same thing but only for companies that recieved sentiment */
proc sql;
  create table industry_summary2 as
  select 
      int(SICCD / 10) as industry,
      count(distinct TICKER) as num_firms,
      sum(sum_ret) as cum_ret,
 	  sum(sum_positive) as total_positive,
      sum(sum_negative) as total_negative,
      sum(sum_total) as total,
      sum(sum_positive) / sum(sum_total) as positive_perc,
      sum(sum_negative) / sum(sum_total) as negative_perc
  from russell_week
  where sum_total > 0
  group by industry
  order by cum_ret desc;
quit;

proc sql;
	create table industry_summary2 as
	select industry, num_firms, cum_ret, total_positive, total_negative, total, positive_perc, negative_perc,
		total/num_firms as tot_per_comp,
		total_positive / num_firms as pos_per_comp,
		total_negative / num_firms as neg_per_comp
	from industry_summary2;
quit;	
	
proc print data=industry_summary2;
run;

*******************************************************************;
/* This code will show us the amount of positive, negative and total news feed */
/* that is for each cluster of daily returns based off company size*/
*******************************************************************;
proc rank data=russell_week out=stock_size_ranks groups=5 ties=mean;
  var mean_market_cap;
  ranks comp_size;
run;

data stock_size;
  set stock_size_ranks;
  length percentile $20;
  if comp_size = 0 then percentile = '0-20%';
  else if comp_size = 1 then percentile = '20-40%';
  else if comp_size = 2 then percentile = '40-60%';
  else if comp_size = 3 then percentile = '60-80%';
  else percentile = '80-100%';
run;

proc sql;
  create table company_size_summary as
  select 
      percentile,
      sum(sum_ret) as sum_ret,
      sum(sum_positive) as total_positive,
      sum(sum_negative) as total_negative,
      sum(sum_total) as total,
      sum(sum_positive) / sum(sum_total) as positive_perc,
      sum(sum_negative) / sum(sum_total) as negative_perc
  from stock_size
  group by percentile
  order by sum_ret desc;
quit;

proc print data=company_size_summary;
run;

/* same thing but only for companies that recieved sentiment */
proc sql;
  create table company_size_summary as
  select 
      percentile,
      sum(sum_ret) as sum_ret,
      sum(sum_positive) as total_positive,
      sum(sum_negative) as total_negative,
      sum(sum_total) as total,
      sum(sum_positive) / sum(sum_total) as positive_perc,
      sum(sum_negative) / sum(sum_total) as negative_perc
  from stock_size
  where sum_total > 0
  group by percentile
  order by sum_ret desc;
quit;

proc print data=company_size_summary;
run;

*******************************************************************;
/* This code will show us the average amount of returns for based off each type */
/* of news article variable: negative, positive, neutral*/
*******************************************************************;

/* Total */
proc sql;
  create table total_group_summary as
  select 
      sum_total,
      count(*) as num_obs,
      mean(sum_ret) as avg_return,
      mean(lagged_return) as avg_lagged_return,
      mean(sum_sprtrn) as avg_sprtrn,
      std(sum_ret) as volatility,
      std(lagged_return) as lag_volatility,
      std(sum_sprtrn) as sp_vol
  from russell_week
  group by sum_total
  having sum_total < 21
  order by sum_total;
quit;

proc print data=total_group_summary;
run;


/* Positive*/
proc sql;
  create table positive_group_summary as
  select 
      sum_positive,
      count(*) as num_obs,
      mean(sum_ret) as avg_return,
      mean(lagged_return) as avg_lagged_return,
      mean(sum_sprtrn) as avg_sprtrn,
      std(sum_ret) as volatility,
      std(lagged_return) as lag_volatility,
      std(sum_sprtrn) as sp_vol
  from russell_week
  group by sum_positive
  order by sum_positive;
quit;

proc print data=positive_group_summary;
run;

/* Negative */
proc sql;
  create table negative_group_summary as
  select 
      sum_negative,
      count(*) as num_obs,
      mean(sum_ret) as avg_return,
      mean(lagged_return) as avg_lagged_return,
      mean(sum_sprtrn) as avg_sprtrn,
      std(sum_ret) as volatility,
      std(lagged_return) as lag_volatility,
      std(sum_sprtrn) as sp_vol
  from russell_week
  group by sum_negative
  order by sum_negative;
quit;

proc print data=negative_group_summary;
run;
