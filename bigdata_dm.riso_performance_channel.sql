set hive.execution.engine=spark;
set hive.vectorized.execution.enabled=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set spark.sql.broadcastTimeout=1800;
set spark.app.name=bigdata_dm.riso_performance_channel;
with sql1 as (
SELECT 
t1.orderdate ,
t1.storeid ,
oc.channeltype ,
sum(t1.tradeamount) as tradeamount ,
sum(t1.tradecount) as tradecount 
from bigdata_dw.riso_order_day t1 left join bigdata_dwd.riso_orderchannel oc on t1.orderchannel = oc.channelcode 
group by t1.orderdate ,t1.storeid ,oc.channeltype 
)
,sql2 as (
SELECT 
t3.recordtime ,
t3.storeid ,
oc.channeltype ,
sum(t3.tradecost) as tradecost
from bigdata_dw.riso_cost_day t3 left join bigdata_dwd.riso_orderchannel oc on t3.orderchannel = oc.channelcode 
group by t3.recordtime ,t3.storeid  ,oc.channeltype 
),
total1 as (
SELECT 
t1.orderdate ,
t1.storeid ,
st.storename ,
t1.channeltype,
t1.tradeamount ,
t1.tradecount ,
t3.tradecost ,
t1.tradeamount - t3.tradecost  as tradefee,
sum(t1.tradeamount) over (partition by t1.orderdate,t1.storeid) as total_tradeamount,
sum(t3.tradecost) over (partition by t3.recordtime,t3.storeid) as total_tradecost
from sql1 t1 
left join sql2 t3 
on t1.orderdate = t3.recordtime 
and t1.storeid = t3.storeid 
and t1.channeltype = t3.channeltype
left join bigdata_dwd.riso_store st on t1.storeid = st.storeid 
)

INSERT overwrite table bigdata_dm.riso_performance_channel partition (dt)
SELECT 
t1.orderdate ,
t1.storeid ,
t1.storename ,
t1.channeltype,
t1.tradeamount ,
t1.tradecount ,
t1.tradecost ,
t1.tradefee,
t1.total_tradeamount,
t1.total_tradecost,
t1.total_tradeamount - t1.total_tradecost as total_tradefee,
DATE_FORMAT(CURRENT_TIMESTAMP() ,'yyyy-MM-dd HH:mm:ss')  as insertdate,
DATE_FORMAT(orderdate,'yyyyMM') as dt 
from total1 t1;

--CREATE table bigdata_dm.riso_performance_channel(
--orderdate string,
--storeid string,
--storename string,
--channeltype string,
--tradeamount decimal(30,8),
--tradecount	decimal(30,8),
--tradecost decimal(30,8),
--tradefee decimal(30,8),
--total_tradeamount decimal(30,8),
--total_tradecost decimal(30,8),
--total_tradefee decimal(30,8),
--insertdate string
--)partitioned by (dt string)
--stored as orc;


