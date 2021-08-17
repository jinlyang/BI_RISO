set hive.execution.engine=spark;
set hive.vectorized.execution.enabled=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set spark.sql.broadcastTimeout=1800;
set spark.app.name=bigdata_dm.riso_performance_month;
with sql1 as (
SELECT 
t1.ordermonth ,
t1.storeid ,
sum(t1.tradeamount) as tradeamount 
from bigdata_dw.riso_order_day t1
group by t1.ordermonth ,t1.storeid 
)
,sql2 as (
SELECT 
t3.recordmonth ,
t3.storeid ,
sum(t3.tradecost) as tradecost
from bigdata_dw.riso_cost_day t3
group by t3.recordmonth ,t3.storeid 
)

--INSERT overwrite table bigdata_dm.riso_performance_month partition (dt)
SELECT 
t1.ordermonth ,
t1.storeid ,
st.storename ,
t1.tradeamount ,
t2.tradecost,
t1.tradeamount - t2.tradecost as tradefee,
DATE_FORMAT(CURRENT_TIMESTAMP() ,'yyyy-MM-dd HH:mm:ss')  as insertdate,
DATE_FORMAT(ordermonth,'yyyy') as dt 
from sql1 t1 left join sql2 t2 on t1.ordermonth = t2.recordmonth and t1.storeid = t2.storeid
left JOIN  bigdata_dwd.riso_store st on t1.storeid = st.storeid ;


--CREATE TABLE bigdata_dm.riso_performance_month(
--ordermonth string,
--storeid string,
--storename string,
--tradeamount decimal(30,8),
--tradecost  decimal(30,8),
--tradefee  decimal(30,8),
--insertdate string
--)partitioned by (dt string)
--stored as orc;