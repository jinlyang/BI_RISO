set hive.execution.engine=mr;
set hive.vectorized.execution.enabled=false;
set hive.exec.dynamic.partition.mode=nonstrict;
--set spark.sql.broadcastTimeout=1800;
--set spark.app.name=riso_dwd_to_dw;

INSERT overwrite table riso_dw.riso_cost_day partition (dt)
SELECT

DATE_FORMAT(t1.recordtime,'yyyy') as recordyear,
DATE_FORMAT(t1.recordtime,'yyyyMM') as recordmonth,
DATE_FORMAT(t1.recordtime,'yyyyMMdd') as recorddate, 

t1.storeid,--�ŵ�id
t1.vendorid,--��Ӧ�̱���
t1.brandid,--Ʒ�Ʊ���
t1.categoryid,--��ƷƷ�����
t1.orderchannel,--��������

sum(t1.tradecost) as tradecost,
sum(t1.tradecostnotax) as tradecosenotax,
sum(t1.tradeamount) as tradeamount,--���۽��

from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as insertdate, 
DATE_FORMAT(t1.recordtime,'yyyyMMdd') as dt

FROM riso_dwd.riso_cost t1


GROUP BY 
DATE_FORMAT(t1.recordtime,'yyyy'),
DATE_FORMAT(t1.recordtime,'yyyyMM'),
DATE_FORMAT(t1.recordtime,'yyyyMMdd'), 
from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'), 
t1.storeid,--�ŵ�id
t1.vendorid,--��Ӧ�̱���
t1.brandid,--Ʒ�Ʊ���
t1.categoryid,--��ƷƷ�����
t1.orderchannel;--��������