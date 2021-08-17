set hive.execution.engine=spark;
set hive.vectorized.execution.enabled=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set spark.sql.broadcastTimeout=1800;
set spark.app.name=bigdata_dm.riso_performance_day;
with sql1 as (
SELECT 
t1.orderdate ,
t1.storeid ,
sum(t1.tradeamount) as tradeamount ,
sum(t1.tradecount) as tradecount 
from bigdata_dw.riso_order_day t1
group by t1.orderdate ,t1.storeid 
),
sql2 as (
SELECT 
t1.orderdate ,
t1.storeid ,
sum(t1.tradeamount) as tradeamount 
from bigdata_dw.riso_order_day t1
WHERE t1.membertype = '会员'
group by t1.orderdate ,t1.storeid 
),
sql3 as (
SELECT 
t2.storeid ,
DATE_FORMAT(t2.flowtime,'yyyyMMdd') as flowdate, 
sum(t2.flownum) as flownum
from bigdata_dwd.riso_customerflow t2
group by t2.storeid ,DATE_FORMAT(t2.flowtime,'yyyyMMdd') 
),
sql4 as (
SELECT 
t3.recordtime ,
t3.storeid ,
sum(t3.tradecost) as tradecost
from bigdata_dw.riso_cost_day t3
group by t3.recordtime ,t3.storeid 
),
total1 as (
SELECT 
t1.orderdate ,
t1.storeid,
st.storename ,
t1.tradeamount ,
t1.tradecount ,
t2.tradeamount as member_trade,
t3.flownum,
t4.tradecost,
t1.tradeamount - t4.tradecost as tardefee
from sql1 t1 
left join sql2 t2 
on t1.orderdate = t2.orderdate 
and t1.storeid = t2.storeid
left join sql3 t3 
on t1.orderdate = t3.flowdate
and t1.storeid = t3.storeid
left join sql4 t4 
on t1.orderdate = t4.recordtime 
and t1.storeid = t4.storeid
left join bigdata_dwd.riso_store st on t1.storeid = st.storeid
)

INSERT overwrite TABLE bigdata_dm.riso_performance_day partition (dt)
SELECT 
orderdate,
storeid,
storename,
tradeamount,
tradecount,
member_trade,
flownum,
tradecost,
tardefee,
DATE_FORMAT(CURRENT_TIMESTAMP() ,'yyyy-MM-dd HH:mm:ss')  as insertdate,
DATE_FORMAT(orderdate,'yyyyMM') as dt 
from total1;


--create table bigdata_dm.riso_performance_day(
--orderdate string,
--storeid string,
--storename string,
--tradeamount decimal(30,8),
--tradecount bigint,
--member_trade decimal(30,8),
--flownum decimal(30,8),
--tradecost decimal(30,8),
--tardefee decimal(30,8),
--insertdate string
--)partitioned by (dt string)
--stored as orc;