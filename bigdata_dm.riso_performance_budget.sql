set hive.execution.engine=spark;
set hive.vectorized.execution.enabled=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set spark.sql.broadcastTimeout=1800;
set spark.app.name=bigdata_dm.riso_performance_budget;
with sql1 as (
SELECT 
distinct
t1.orderyear ,
t1.ordermonth ,
t1.storeid ,
sum(t1.tradeamount) over (partition by t1.storeid,t1.orderyear order by t1.ordermonth asc) as tradeamount 
from bigdata_dw.riso_order_day t1

)
,sql2 as (
SELECT 
DISTINCT 
t3.recordyear, 
t3.recordmonth ,
t3.storeid ,
sum(t3.tradecost) over (partition by t3.storeid,t3.recordyear order by t3.recordmonth asc) as tradecost
from bigdata_dw.riso_cost_day t3
)
,sql3 as (
SELECT 
t2.storeid ,
t2.`year` as year_,
sum(t2.profitbudget) as profitbudget,
sum(t2.salesbudget) as salesbudget
from bigdata_dwd.riso_budget t2
group by t2.storeid ,
t2.`year`
)

insert overwrite table bigdata_dm.riso_performance_budget partition (dt )
select 
t1.storeid,
st.storename,
t1.ordermonth,
t1.orderyear,
t1.tradeamount,
t2.tradecost,
t3.profitbudget,
t3.salesbudget,
DATE_FORMAT(CURRENT_TIMESTAMP() ,'yyyy-MM-dd HH:mm:ss')  as insertdate,
DATE_FORMAT(t1.ordermonth,'yyyy') as dt 
from sql1 t1 left join sql2 t2 
on t1.ordermonth = t2.recordmonth 
and t1.storeid = t2.storeid
left join sql3 t3 on t1.orderyear = t3.year_
left join bigdata_dwd.riso_store st on t1.storeid = st.storeid; 


--CREATE table  bigdata_dm.riso_performance_budget(
--storeid string,
--storename string, 
--ordermonth string,
--orderyear string,
--tradeamount decimal(30,8),
--tradecost decimal(30,8),
--profitbudget decimal(30,8),
--salesbudget decimal(30,8),
--insertdate string
--)partitioned by (dt string)
--stored as orc;
