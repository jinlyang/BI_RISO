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
)

SELECT
t1.storeid,
t1.orderdate,
t1.lev1name,
t1.lev2name,
t1.tradeamount,
t1.quantity,
t2.tradecost
FROM sql1 t1
left join sql2 t2
on t2.storeid=t1.storeid and t2.recorddate = t1.orderdate AND t2.lev1name = t1.lev1name AND t2.lev2name = t1.lev2name ;