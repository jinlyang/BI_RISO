set hive.execution.engine=spark;
set hive.vectorized.execution.enabled=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set spark.sql.broadcastTimeout=1800;
set spark.app.name=bigdata_dw.riso_order_day;

INSERT overwrite table bigdata_dw.riso_cost_day partition ( dt )
SELECT
DATE_FORMAT( t1.recordtime,'yyyy') as recordyear ,
DATE_FORMAT( t1.recordtime,'yyyyMM') as recordmonth ,
DATE_FORMAT( t1.recordtime,'yyyyMMdd') as recordtime ,
t1.storeid ,
t1.categoryid ,
t1.orderchannel ,
t1.vendorid ,
t1.brandid ,
sum(t1.tradecost) as tradecost ,
from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as insertdate, 
DATE_FORMAT(t1.recordtime,'yyyyMM') as dt
from bigdata_dwd.riso_cost t1 
group by 
DATE_FORMAT( t1.recordtime,'yyyy'),
DATE_FORMAT( t1.recordtime,'yyyyMM'),
DATE_FORMAT( t1.recordtime,'yyyyMMdd'),
t1.storeid ,
t1.categoryid ,
t1.orderchannel ,
t1.vendorid ,
t1.brandid ,
from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss');

--CREATE table bigdata_dw.riso_cost_day(
--recordyear string,
--recordmonth string,
--recordtime string,
--storeid string,
--categoryid string,
--orderChannel string,
--vendorid string,
--brandid string,
--tradecost  decimal(30,8),
--insertdate string
--)
--partitioned by (dt string)
--stored as orc;