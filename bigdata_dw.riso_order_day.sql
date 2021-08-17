set hive.execution.engine=spark;
set hive.vectorized.execution.enabled=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set spark.sql.broadcastTimeout=1800;
set spark.app.name=bigdata_dw.riso_order_day;

--INSERT overwrite table bigdata_dw.riso_order_day partition (dt)
SELECT 
DATE_FORMAT(t1.orderdate,'yyyy') as orderyear,
DATE_FORMAT(t1.orderdate,'yyyyMM') as ordermonth,
DATE_FORMAT(t1.orderdate,'yyyyMMdd') as orderdate, 
t1.storeid,
t1.categoryid ,
t2.membertype ,
t2.orderchannel,
t1.vendorid ,
t1.brandid ,
sum(t1.tradeamount) as tradeamount,
sum(t1.quantity) as quantity ,
COUNT(DISTINCT t1.orderid) as tradecount,
from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as insertdate, 
DATE_FORMAT(t1.orderdate,'yyyyMM') as dt
from bigdata_dwd.riso_orderdetail t1
left join (select orderid,orderChannel,memberType from bigdata_dwd.riso_order t2) t2 
on t1.orderid = t2.orderid 
GROUP by DATE_FORMAT(t1.orderdate,'yyyyMMdd'),
DATE_FORMAT(t1.orderdate,'yyyy'),
t1.storeid,
t1.categoryid ,
t2.membertype ,
t2.orderchannel,
t1.vendorid ,
t1.brandid ,
from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') , 
DATE_FORMAT(t1.orderdate,'yyyyMM');



--drop table bigdata_dw.riso_order_day;
--CREATE table bigdata_dw.riso_order_day(
--orderyear string,
--ordermonth string,
--orderdate string,
--storeid string,
--categoryid string,
--membertype string,
--orderChannel string,
--vendorid string,
--brandid string,
--tradeamount  decimal(30,8),
--quantity decimal(30,8),
--tradecount bigint,
--insertdate string
--)
--partitioned by (dt string)
--stored as orc;
