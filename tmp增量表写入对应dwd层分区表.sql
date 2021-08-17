set hive.execution.engine=mr;
set hive.vectorized.execution.enabled=false;
set hive.exec.dynamic.partition.mode=nonstrict;
--set spark.sql.broadcastTimeout=1800;
--set spark.app.name=riso_temp_to_dwd;

--���� ����
INSERT overwrite table riso_dwd.riso_customerflow partition (dt)
SELECT  * , DATE_FORMAT(current_date(),'yyyyMMdd') as dt 
from riso_tmp.riso_customerflow;


--�������� ����
INSERT overwrite table riso_dwd.riso_order partition (dt)
select  * , DATE_FORMAT(current_date(),'yyyyMMdd') as dt 
from riso_tmp.riso_order ;



--������ϸ ����
INSERT overwrite table riso_dwd.riso_orderdetail partition (dt)
select  * , DATE_FORMAT(current_date(),'yyyyMMdd') as dt 
from riso_tmp.riso_orderdetail ;


--��Ʒ�ɱ� ����
INSERT overwrite table riso_dwd.riso_cost partition (dt)
select  * , DATE_FORMAT(current_date(),'yyyyMMdd') as dt 
from riso_tmp.riso_cost ;

--Ʒ�� ȫ��
INSERT overwrite table riso_dwd.riso_brand 
select  *  from riso_tmp.riso_brand;

--Ԥ�� ȫ��
--INSERT overwrite table riso_dwd.riso_budget 
--select  * , DATE_FORMAT(current_date(),'yyyyMMdd') as insertdate;
--from riso_tmp.riso_budget;

--���� ȫ��
INSERT overwrite table riso_dwd.riso_category 
select  *  from riso_tmp.riso_category;

--�������� ȫ��
INSERT overwrite table riso_dwd.riso_orderchannel 
select  *  from riso_tmp.riso_orderchannel;

--�ŵ� ȫ��
INSERT overwrite table riso_dwd.riso_store
select  *  from riso_tmp.riso_store;

--��Ӧ�� ȫ��
INSERT overwrite table riso_dwd.riso_vendor 
select  *  from riso_tmp.riso_vendor ;
