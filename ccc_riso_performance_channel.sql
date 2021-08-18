set hive.execution.engine=mr;
set hive.vectorized.execution.enabled=false;
set hive.exec.dynamic.partition.mode=nonstrict;

with sql1 as (
SELECT
t1.storeid,
t1.orderdate,
t3.channeltype,
SUM(t1.tradeamount) as  tradeamount,
SUM(t1.tradecount) as  tradecount
FROM riso_order_day t1
left join riso_dwd.riso_orderchannel t3
on t1.orderchannel  = t3.channelcode 
group BY 
t1.storeid,
t1.orderdate,
t3.channeltype
),
sql2 as (
SELECT 
t1.storeid,
t1.recorddate,
t2.channeltype ,
SUM(t1.tradecost) as tradecost
FROM riso_dw.riso_cost_day t1
left join riso_dwd.riso_orderchannel t2
on t1.orderchannel =t2.channelcode 
GROUP BY 
t1.storeid,
t1.recorddate,
t2.channeltype 
),
sql3 as (
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
t1.channeltype,
t1.tradeamount,
t1.tradecount,
t2.tradecost,
t3.tradecost_sum
FROM sql1 t1
left join sql2 t2
on t1.storeid = t2.storeid AND t1.orderdate = t2.recorddate AND t1.channeltype = t2.channeltype
left join sql3 t3
on t1.storeid = t3.storeid AND t1.orderdate = t3.recorddate