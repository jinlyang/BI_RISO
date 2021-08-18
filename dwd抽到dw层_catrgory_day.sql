set hive.execution.engine=mr;
set hive.vectorized.execution.enabled=false;
set hive.exec.dynamic.partition.mode=nonstrict;
--set spark.sql.broadcastTimeout=1800;
--set spark.app.name=riso_dwd_to_dw;

with level4 as (
select t1.categoryid ,t1.categoryname ,t1.parentid 
FROM riso_dwd.riso_category  t1
WHERE t1.level = '4'
),
level3 as (
select t1.categoryid ,t1.categoryname ,t1.parentid 
FROM riso_dwd.riso_category  t1
WHERE t1.level = '3'
),
level2 as (
select t1.categoryid ,t1.categoryname
FROM riso_dwd.riso_category  t1
WHERE t1.level = '2'
)
33
INSERT overwrite table riso_dw.riso_category
SELECT 
t4.categoryid as lev4id,
t4.categoryname as lev4name,
t3.categoryid as lev3id,
t3.categoryname as lev3name,
t2.categoryid as lev2id,
t2.categoryname as lev2name
FROM level4 t4
left join level3 t3 on t4.parentid = t3.categoryid
left join level2 t2 on t3.parentid = t2.categoryid;