set hive.execution.engine=mr;
set hive.vectorized.execution.enabled=false;
set hive.exec.dynamic.partition.mode=nonstrict;
--set spark.sql.broadcastTimeout=1800;
--set spark.app.name=riso_dwd_to_dw;

INSERT overwrite table riso_dw.riso_order_day partition (dt)
SELECT

DATE_FORMAT(t1.orderdate,'yyyy') as orderyear,
DATE_FORMAT(t1.orderdate,'yyyyMM') as ordermonth,
DATE_FORMAT(t1.orderdate,'yyyyMMdd') as orderdate, 
t1.storeid,--门店id
t1.vendorid,--供应商编码
t1.brandid,--品牌编码
t1.categoryid,--商品品类编码
t2.ordertype,--订单订单类型
t2.orderchannel,--订单渠道
t2.membertype,--订单会员类型
sum(t2.tradeamountnotax) as notaxmount,
sum(t1.quantity) as quantity,--销售数量
sum(t1.tradeamount) as tradeamount,--销售金额
COUNT(DISTINCT t1.orderid) as tradecount,
from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as insertdate, 
DATE_FORMAT(t1.orderdate,'yyyyMMdd') as dt

FROM riso_dwd.riso_orderdetail t1
left join riso_dwd.riso_order t2 
on t1.orderid = t2.orderid

GROUP BY 
DATE_FORMAT(t1.orderdate,'yyyy'),
DATE_FORMAT(t1.orderdate,'yyyyMM'),
DATE_FORMAT(t1.orderdate,'yyyyMMdd'),
from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'),
t1.storeid,
t1.categoryid,
t2.membertype,
t2.orderchannel,
t2.ordertype,
t1.vendorid,
t1.brandid;
