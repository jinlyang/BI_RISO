set hive.execution.engine=mr;
set hive.vectorized.execution.enabled=false;
set hive.exec.dynamic.partition.mode=nonstrict;
--set spark.sql.broadcastTimeout=1800;
--set spark.app.name=riso_temp_to_dwd;

--客流 增量
INSERT overwrite table riso_dwd.riso_customerflow partition (dt)
SELECT  * , DATE_FORMAT(current_date(),'yyyyMMdd') as dt 
from riso_tmp.riso_customerflow;


--订单汇总 增量
INSERT overwrite table riso_dwd.riso_order partition (dt)
select  * , DATE_FORMAT(current_date(),'yyyyMMdd') as dt 
from riso_tmp.riso_order ;



--订单明细 增量
INSERT overwrite table riso_dwd.riso_orderdetail partition (dt)
select  * , DATE_FORMAT(current_date(),'yyyyMMdd') as dt 
from riso_tmp.riso_orderdetail ;


--商品成本 增量
INSERT overwrite table riso_dwd.riso_cost partition (dt)
select  * , DATE_FORMAT(current_date(),'yyyyMMdd') as dt 
from riso_tmp.riso_cost ;

--品牌 全量
INSERT overwrite table riso_dwd.riso_brand 
select  *  from riso_tmp.riso_brand;

--预算 全量
--INSERT overwrite table riso_dwd.riso_budget 
--select  * , DATE_FORMAT(current_date(),'yyyyMMdd') as insertdate;
--from riso_tmp.riso_budget;

--分类 全量
INSERT overwrite table riso_dwd.riso_category 
select  *  from riso_tmp.riso_category;

--订单渠道 全量
INSERT overwrite table riso_dwd.riso_orderchannel 
select  *  from riso_tmp.riso_orderchannel;

--门店 全量
INSERT overwrite table riso_dwd.riso_store
select  *  from riso_tmp.riso_store;

--供应商 全量
INSERT overwrite table riso_dwd.riso_vendor 
select  *  from riso_tmp.riso_vendor ;
