set hive.execution.engine=mr;
set hive.vectorized.execution.enabled=false;
set hive.exec.dynamic.partition.mode=nonstrict;
--set spark.sql.broadcastTimeout=1800;
--set spark.app.name=riso_dwd_to_dw;

--INSERT overwrite table riso_dm.riso_performance_day
--会员销售
with sql1 as(
SELECT
t1.storeid,
t1.orderdate,
SUM(t1.tradeamount) as vipmount 
FROM riso_order_day t1
WHERE t1.membertype = '会员'
GROUP BY 
t1.storeid,
t1.orderdate
),
--实际销售，交易笔数
sql2 as (
SELECT
t1.storeid,
t1.orderdate,
SUM(t1.tradeamount) as tradeamount,
SUM(t1.tradecount) as tradecount
FROM riso_order_day t1
GROUP BY 
t1.storeid, 
t1.orderdate
),
--销售成本
sql3 as(
SELECT 
t1.storeid,
t1.recorddate as recorddate,
sum(t1.tradecost) as tradecost
FROM riso_cost_day t1
GROUP BY
t1.storeid,
t1.recorddate
),
--客流
sql4 as(
SELECT 
t1.storeid ,
DATE_FORMAT(t1.flowtime,'yyyyMMdd') as flowdate,
SUM(t1.flownum) as flownum
FROM riso_dwd.riso_customerflow t1
group by
t1.storeid ,
DATE_FORMAT(t1.flowtime,'yyyyMMdd')
)

SELECT 
t2.orderdate,
t2.storeid,
t2.tradeamount,
t2.tradecount,
t3.tradecost,
t1.vipmount,
t4.flownum
FROM sql2 t2
left join sql3 t3
on t3.recorddate = t2.orderdate and t3.storeid = t2.storeid
left join sql1 t1
on t1.storeid = t2.storeid and t2.orderdate = t1.orderdate
left join sql4 t4
on substr(t4.storeid ,3,6) = t2.storeid and t4.flowdate = t2.orderdate;

