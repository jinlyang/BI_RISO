set hive.execution.engine=spark;
set hive.vectorized.execution.enabled=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set spark.sql.broadcastTimeout=1800;
set spark.app.name=bigdata_dm.riso_performance_category;

with sql1 as (
SELECT 
t1.orderdate ,
t1.storeid ,
ct.categoryname_nd ,
ct.categoryname_st ,
sum(t1.tradeamount) as tradeamount ,
sum(t1.quantity) as quantity 
from bigdata_dw.riso_order_day t1 left join bigdata_dw.riso_category ct on t1.categoryid  = ct.categoryid 
group by t1.orderdate ,t1.storeid ,ct.categoryname_nd ,ct.categoryname_st 
),
sql2 as (
SELECT 
t3.recordtime ,
t3.storeid ,
ct.categoryname_nd ,
ct.categoryname_st ,
sum(t3.tradecost) as tradecost
from bigdata_dw.riso_cost_day t3 left join bigdata_dw.riso_category ct on t3.categoryid  = ct.categoryid 
group by t3.recordtime ,t3.storeid ,ct.categoryname_nd ,ct.categoryname_st 
),
total1 as (
SELECT 
t1.storeid ,
st.storename ,
t1.orderdate ,
t1.categoryname_nd,
t1.categoryname_st,
t1.tradeamount ,
t1.quantity ,
t1.tradeamount - t2.tradecost as tradefee,
sum(t1.tradeamount) over(partition by t1.storeid,t1.orderdate) as total_tradeamount,
sum(t2.tradecost) over(partition by t2.storeid,t2.recordtime) as total_tradecost
from sql1 t1 
left join sql2  t2 
on t1.orderdate = t2.recordtime 
and t1.storeid = t2.storeid 
and t1.categoryname_nd = t2.categoryname_nd
left join bigdata_dwd.riso_store st on t1.storeid = st.storeid
)

insert overwrite table bigdata_dm.riso_performance_category partition (dt)
SELECT 
t1.storeid,
t1.storename,
t1.orderdate,
t1.categoryname_nd,
t1.categoryname_st,
t1.tradeamount ,
t1.quantity ,
t1.tradefee,
t1.total_tradeamount,
t1.total_tradecost,
t1.total_tradeamount - t1.total_tradecost as total_tradefee,
DATE_FORMAT(CURRENT_TIMESTAMP() ,'yyyy-MM-dd HH:mm:ss')  as insertdate,
DATE_FORMAT(orderdate,'yyyyMM') as dt 
from total1 t1;

--create table bigdata_dm.riso_performance_category (
--storeid string,
--storename string,
--orderdate string,
--categoryname_nd string,
--categoryname_st string,
--tradeamount decimal(30,8),
--quantity decimal(30,8),
--tradefee decimal(30,8),
--total_tradeamount decimal(30,8),
--total_tradecost decimal(30,8),
--total_tradefee decimal(30,8),
--insertdate string
--)partitioned by (dt string) 
--stored as orc;

