set hive.execution.engine=mr;
set hive.vectorized.execution.enabled=false;
set hive.exec.dynamic.partition.mode=nonstrict;

with sql1 as (
SELECT 
t1.storeid,
t1.ordermonth,
SUM(t1.tradeamount) as tradeamount
FROM riso_dw.riso_order_day t1
group by
t1.ordermonth ,
t1.storeid
),
sql2 as(
SELECT 
t1.storeid,
t1.recordmonth as recordmonth,
sum(t1.tradecost) as tradecost
FROM riso_dw.riso_cost_day t1
GROUP BY
t1.storeid,
t1.recordmonth
)

SELECT
t1.ordermonth,
t1.storeid,
t1.tradeamount,
t2.tradecost,
(t1.tradeamount-t2.tradecost) as fee
FROM sql1 t1
left join sql2 t2
on t2.storeid=t1.storeid and t2.recordmonth = t1.ordermonth;