libname analysis "/home/u63744942/sasuser.v94/finalproject";

/* Import CSV file */
proc import datafile='/home/u63744942/sasuser.v94/finalproject/DowJones Daily Returns.csv'
            out=russell2000
            dbms=csv replace;
run;

proc means data=russell2000;
run;

proc contents data=russell2000;
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


/* Adding lagged Columns */ 

proc sort data=WORK.RUSSELL;
  by TICKER date;
run;

data WORK.RUSSELL_lagged;
  set WORK.RUSSELL;
  by TICKER;
  if first.TICKER then lagged_return = .;
  lagged_return = lag(RET);
  if first.TICKER then lagged_return = .;
  output;
run;

data russell;
	set russell_lagged;
	where lagged_return is not missing;
run;

proc print data=russell (obs=15);
run;


/* Daily Aggregation */
proc sql;
  create table daily_summary as
  select date,
         sprtrn,
         sum(positive) as sum_positive,
         sum(negative) as sum_negative,
         sum(total) as sum_total
  from WORK.RUSSELL
  group by date, sprtrn
  order by date;
quit;

proc print data=daily_summary;
run;


/* same thing but only for companies that recieved sentiment */

proc sql;
  create table daily_summary2 as
  select date,
         sprtrn,
         sum(positive) as sum_positive,
         sum(negative) as sum_negative,
         sum(total) as sum_total
  from WORK.RUSSELL
  where total > 0
  group by date, sprtrn
  order by date;
quit;

proc print data=daily_summary2;
run;







*******************************************************************;
/* This code will show us the amount of positive, negative and total news feed */
/* that is for each cluster of daily returns based off daily market performance*/
*******************************************************************;

/* Calculate percentiles of sprtrn when grouped by day*/
proc rank data=daily_summary2 out=ranked_sprtrn groups=10 ties=mean;
  var sprtrn;
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
      avg(sprtrn) as avg_sprtn,
      sum(sum_positive) as total_positive,
      sum(sum_negative) as total_negative,
      sum(sum_total) as total
  from ranked_sprtrn
  group by percentile_range
  order by avg_sprtn desc;
quit;

proc print data=percentile_summary;
run;

/* same thing but only for companies that recieved sentiment */
proc sql;
  create table percentile_summary1 as
  select 
      percentile_range,
      avg(sprtrn) as avg_sprtn,
      sum(sum_positive) as total_positive,
      sum(sum_negative) as total_negative,
      sum(sum_total) as total
  from ranked_sprtrn
  group by percentile_range
  order by avg_sprtn desc;
quit;

proc print data=percentile_summary1;
run;













*******************************************************************;
/* This code will show us the amount of positive, negative and total news feed */
/* that is for each cluster of daily returns based off individual stock performance*/
*******************************************************************;

/* Calculate percentiles of sprtrn when grouped by day*/
proc rank data=russell out=ranked_rtrn groups=10 ties=mean;
  var ret;
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
      avg(ret) as avg_ret,
      avg(lagged_return) as avg_lag_ret,
      sum(positive) as total_positive,
      sum(negative) as total_negative,
      sum(total) as total,
      sum(positive) / sum(total) as positive_perc,
      sum(negative) / sum(total) as negative_perc
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
      avg(ret) as avg_ret,
      avg(lagged_return) as avg_lag_ret,
      sum(positive) as total_positive,
      sum(negative) as total_negative,
      sum(total) as total,
      sum(positive) / sum(total) as positive_perc,
      sum(negative) / sum(total) as negative_perc
  from ranked_rtrn
  where total > 0
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
      sum(ret) as cum_ret,
 	  sum(positive) as total_positive,
      sum(negative) as total_negative,
      sum(total) as total,
      sum(positive) / sum(total) as positive_perc,
      sum(negative) / sum(total) as negative_perc
  from russell
  group by industry
  having num_firms > 10
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
      sum(ret) as cum_ret,
 	  sum(positive) as total_positive,
      sum(negative) as total_negative,
      sum(total) as total,
      sum(positive) / sum(total) as positive_perc,
      sum(negative) / sum(total) as negative_perc
  from russell
  where total > 0
  group by industry
  having num_firms > 10
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
proc rank data=russell_week out=stock_size_ranks groups=30 ties=mean;
  var mean_market_cap;
  ranks comp_size;
run;

data stock_size;
  set stock_size_ranks;
  length group $10;
  if comp_size > 20 then group = 'Top 10';
  else if comp_size < 21 then group = '11-30';
run;

proc sql;
  create table company_size_summary as
  select 
      group,
      sum(sum_ret) as sum_ret,
      sum(sum_positive) as total_positive,
      sum(sum_negative) as total_negative,
      sum(sum_total) as total,
      sum(sum_positive) / sum(sum_total) as positive_perc,
      sum(sum_negative) / sum(sum_total) as negative_perc
  from stock_size
  group by group
  order by sum_ret desc;
quit;

proc print data=company_size_summary;
run;


/* same thing but only for companies that recieved sentiment */


proc sql;
  create table company_size_summary as
  select 
      group,
      sum(ret) as sum_ret,
      sum(positive) as total_positive,
      sum(negative) as total_negative,
      sum(total) as total,
      sum(positive) / sum(total) as positive_perc,
      sum(negative) / sum(total) as negative_perc
  from stock_size
  where total > 0
  group by group
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
      total,
      count(*) as num_obs,
      mean(ret) as avg_return,
      mean(lagged_return) as avg_lagged_return,
      std(ret) as volatility,
      std(lagged_return) as lag_volatility,
      avg(sprtrn) as sprtrn,
      std(sprtrn) as sp_vol
  from russell
  group by total
  order by total;
quit;

proc print data=total_group_summary;
run;



/* Positive*/
proc sql;
  create table positive_group_summary as
  select 
      positive,
      count(*) as num_obs,
      mean(ret) as avg_return,
      mean(lagged_return) as avg_lagged_return,
      std(ret) as volatility,
      std(lagged_return) as lag_volatility,
      avg(sprtrn) as sprtrn,
      std(sprtrn) as sp_vol
  from russell
  group by positive
  order by positive;
quit;

proc print data=positive_group_summary;
run;


/* Negative */
proc sql;
  create table negative_group_summary as
  select 
      negative,
      count(*) as num_obs,
      mean(ret) as avg_return,
      mean(lagged_return) as avg_lagged_return,
      std(ret) as volatility,
      std(lagged_return) as lag_volatility,
      avg(sprtrn) as sprtrn,
      std(sprtrn) as sp_vol
  from russell
  group by negative
  order by negative;
quit;

proc print data=negative_group_summary;
run;

