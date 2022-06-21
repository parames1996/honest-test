CREATE TABLE rejected (
   "Amount Requested" float,
   "Application Date" date,
   "Loan Title" VARCHAR(4000),
   "Risk_Score" float,
   "Debt-To-Income Ratio" float,
   "Zip Code" VARCHAR(5),
   "State" VARCHAR(2),
   "Employment Length" VARCHAR(9),
   "Policy Code" INTEGER,
   "MA50" float
);

ALTER TABLE rejected 
ADD COLUMN MA100 float;

update postgres.public.rejected 
set "MA100" = df_EMA."MA100"
FROM
(select *
,AVG(df_date.v)
       OVER(ORDER BY df_date.date_ ROWS BETWEEN 100 PRECEDING AND CURRENT ROW)
       AS "MA100"
from (select  
    r."Application Date" as date_
    , avg(r."Risk_Score") as v
from postgres.public.rejected r
group by r."Application Date"
order by date_) df_date) as df_EMA
where postgres.public.rejected."Application Date" = df_EMA.date_
;

create or replace function ema_func(state numeric, inval numeric, alpha numeric)
  returns numeric
  language plpgsql as $$
begin
  return case
         when state is null then inval
         else alpha * inval + (1-alpha) * state
         end;
end
$$;

create aggregate ema(numeric, numeric) (sfunc = ema_func, stype = numeric);

update postgres.public.rejected 
set "MA50" = df_EMA."EMA50"
FROM
(select *
,AVG(df_date.v)
       OVER(ORDER BY df_date.date_ ROWS BETWEEN 50 PRECEDING AND CURRENT ROW)
       AS "MA50"
,round(ema(cast(df_date.v as INTEGER), 0.5 ) over (order by df_date.date_ asc),2) as "EMA50"
from (select  
    r."Application Date" as date_
    , avg(r."Risk_Score") as v
from postgres.public.rejected r
group by r."Application Date"
order by date_) df_date) as df_EMA
where postgres.public.rejected."Application Date" = df_EMA.date_;