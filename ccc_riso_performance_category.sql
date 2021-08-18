set hive.execution.engine=mr;
set hive.vectorized.execution.enabled=false;
set hive.exec.dynamic.partition.mode=nonstrict;

with sql1 as (
SELECT 
t1.storeid,
t1.orderdate,
t2.lev1name,
t2.lev2name,
SUM(t1.tradeamount) as tradeamount,
sum(t1.quantity) as quantity
FROM riso_dw.riso_order_day t1
left join riso_dw.riso_category t2
on t1.categoryid = t2.lev4id 
group by
t1.storeid,
t1.orderdate,
t2.lev1name,
t2.lev2name 
),
sql2 as(
SELECT 
t1.storeid,
t1.recorddate ,
t2.lev1name,
t2.lev2name,
SUM(t1.tradecost) as tradecost
FROM riso_dw.riso_cost_day t1
left join riso_dw.riso_category t2
on t1.categoryid = t2.lev4id 
group by
t1.storeid,
t1.recorddate,
t2.lev1name,
t2.lev2name 
),
sql3 as (
SELECT
t1.storeid,
t1.orderdate,
SUM(t1.tradeamount) as tradeamount_sum
FROM riso_dw.riso_order_day t1
GROUP BY 
t1.storeid, 
t1.orderdate
),
sql4 as(
SELECT 
t1.storeid,
t1.recorddate as recorddate,
sum(t1.tradecost) as tradecost_sum
FROM riso_dw.riso_cost_day t1
GROUP BY
t1.storeid,
t1.recorddate
)

SELECT
t1.storeid,
t1.orderdate,
t1.lev1name,
t1.lev2name,
t1.tradeamount,
t1.quantity,
t2.tradecost,
t3.tradeamount_sum,
t4.tradecost_sum
FROM sql1 t1
left join sql2 t2
on t2.storeid=t1.storeid and t2.recorddate = t1.orderdate AND t2.lev1name = t1.lev1name AND t2.lev2name = t1.lev2name
left join sql3 t3
on t3.storeid = t1.storeid AND t1.orderdate = t3.orderdate
left join sql4 t4
on t4.storeid=t1.storeid and t4.recorddate = t1.orderdate