-- riso_dw.riso_category definition

CREATE TABLE `riso_dw.riso_category`(
  `lev4id` string, 
  `lev4name` string, 
  `lev3id` string, 
  `lev3name` string, 
  `lev2id` string, 
  `lev2name` string,
  `lev1id` string, 
  `lev1name` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://dev-blgfmdm-01:8020/user/hive/warehouse/riso_dw.db/riso_category'
TBLPROPERTIES (
  'transient_lastDdlTime'='1629200571');