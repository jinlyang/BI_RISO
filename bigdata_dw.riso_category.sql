with sql1 as (
SELECT 
*
from bigdata_dwd.riso_category t1 
where 
t1.level = 4
),
sql2 as (
SELECT 
*
from bigdata_dwd.riso_category t1 
where 
t1.level = 3
),
sql3 as (
SELECT 
*
from bigdata_dwd.riso_category t1 
where 
t1.level = 2
),
sql4 as (
SELECT 
*
from bigdata_dwd.riso_category t1 
where 
t1.level = 1
)
INSERT overwrite table bigdata_dw.riso_category
SELECT 
t1.categoryid ,
t1.categoryname ,
t2.categoryname as categoryname_rd,
t3.categoryname as categoryname_nd,
t4.categoryname as categoryname_st
from sql1 t1 left join sql2 t2 on SUBSTR(t1.categoryid,1,6) = t2.categoryid 
left join sql3 t3 on SUBSTR(t1.categoryid,1,4) = t3.categoryid 
left join sql4 t4 on SUBSTR(t1.categoryid,1,2) = t4.categoryid ;


--CREATE table bigdata_dw.riso_category (
--categoryid string,
--categoryname string,
--categoryname_rd string,
--categoryname_nd string,
--categoryname_st string
--)stored as orc;
 