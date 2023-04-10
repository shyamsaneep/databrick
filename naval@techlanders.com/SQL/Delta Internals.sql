-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.help()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/FileStore/tables/constructors.json")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/FileStore/tables/delta",True)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Log Files
-- MAGIC - JSON Transaction log file
-- MAGIC - Parquet checkpoint file
-- MAGIC - CRC- cyclic reduntant check

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE IF NOT EXISTS employee_demo (
-- MAGIC   emp_id INT,
-- MAGIC   emp_name STRING,
-- MAGIC   gender STRING,
-- MAGIC   salary INT,
-- MAGIC   department STRING
-- MAGIC )using delta

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC head dbfs:/user/hive/warehouse/employee_demo/_delta_log/00000000000000000000.json

-- COMMAND ----------

INSERT INTO employee_demo values(100, "a", "M", 10000, "DE")

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC head dbfs:/user/hive/warehouse/employee_demo/_delta_log/00000000000000000001.json

-- COMMAND ----------

INSERT INTO employee_demo values(200, "b", "F", 20000, "DS");
INSERT INTO employee_demo values(300, "c", "F", 22000, "DS")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.text("dbfs:/user/hive/warehouse/employee_demo/_delta_log/00000000000000000000.crc").display()

-- COMMAND ----------

{"tableSizeBytes":0,"numFiles":0,"numMetadata":1,"numProtocol":1,"protocol":{"minReaderVersion":1,"minWriterVersion":2},"metadata":{"id":"46aeffee-ec8f-4c82-8280-69b445b0b068","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"emp_id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"emp_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"gender\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"salary\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"department\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"description":"First Delta Table "},"createdTime":1679894556559},"histogramOpt":{"sortedBinBoundaries":[0,8192,16384,32768,65536,131072,262144,524288,1048576,2097152,4194304,8388608,12582912,16777216,20971520,25165824,29360128,33554432,37748736,41943040,50331648,58720256,67108864,75497472,83886080,92274688,100663296,109051904,117440512,125829120,130023424,134217728,138412032,142606336,146800640,150994944,167772160,184549376,201326592,218103808,234881024,251658240,268435456,285212672,301989888,318767104,335544320,352321536,369098752,385875968,402653184,419430400,436207616,452984832,469762048,486539264,503316480,520093696,536870912,553648128,570425344,587202560,603979776,671088640,738197504,805306368,872415232,939524096,1006632960,1073741824,1140850688,1207959552,1275068416,1342177280,1409286144,1476395008,1610612736,1744830464,1879048192,2013265920,2147483648,2415919104,2684354560,2952790016,3221225472,3489660928,3758096384,4026531840,4294967296,8589934592,17179869184,34359738368,68719476736,137438953472,274877906944],"fileCounts":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"totalBytes":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]},"txnId":"0a667b91-4692-4515-b463-3eeeebbd98d7","allFiles":[]}

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.text("dbfs:/user/hive/warehouse/employee_demo/_delta_log/00000000000000000001.crc").display()

-- COMMAND ----------

{"tableSizeBytes":1497,"numFiles":1,"numMetadata":1,"numProtocol":1,"protocol":{"minReaderVersion":1,"minWriterVersion":2},"metadata":{"id":"46aeffee-ec8f-4c82-8280-69b445b0b068","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"emp_id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"emp_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"gender\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"salary\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"department\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"description":"First Delta Table "},"createdTime":1679894556559},"histogramOpt":{"sortedBinBoundaries":[0,8192,16384,32768,65536,131072,262144,524288,1048576,2097152,4194304,8388608,12582912,16777216,20971520,25165824,29360128,33554432,37748736,41943040,50331648,58720256,67108864,75497472,83886080,92274688,100663296,109051904,117440512,125829120,130023424,134217728,138412032,142606336,146800640,150994944,167772160,184549376,201326592,218103808,234881024,251658240,268435456,285212672,301989888,318767104,335544320,352321536,369098752,385875968,402653184,419430400,436207616,452984832,469762048,486539264,503316480,520093696,536870912,553648128,570425344,587202560,603979776,671088640,738197504,805306368,872415232,939524096,1006632960,1073741824,1140850688,1207959552,1275068416,1342177280,1409286144,1476395008,1610612736,1744830464,1879048192,2013265920,2147483648,2415919104,2684354560,2952790016,3221225472,3489660928,3758096384,4026531840,4294967296,8589934592,17179869184,34359738368,68719476736,137438953472,274877906944],"fileCounts":[1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"totalBytes":[1497,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]},"txnId":"d33ecdbe-6d8f-441d-8b8f-58eca13204c5","allFiles":[{"path":"part-00000-876e3a98-e2ff-45f7-ba85-840719972dd5-c000.snappy.parquet","partitionValues":{},"size":1497,"modificationTime":1679895718000,"dataChange":false,"stats":"{\"numRecords\":1,\"minValues\":{\"emp_id\":100,\"emp_name\":\"a\",\"gender\":\"M\",\"salary\":10000,\"department\":\"DE\"},\"maxValues\":{\"emp_id\":100,\"emp_name\":\"a\",\"gender\":\"M\",\"salary\":10000,\"department\":\"DE\"},\"nullCount\":{\"emp_id\":0,\"emp_name\":0,\"gender\":0,\"salary\":0,\"department\":0}}","tags":{"INSERTION_TIME":"1679895718000000","MIN_INSERTION_TIME":"1679895718000000","MAX_INSERTION_TIME":"1679895718000000","OPTIMIZE_TARGET_SIZE":"268435456"}}]}

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.text("dbfs:/user/hive/warehouse/employee_demo/_delta_log/00000000000000000002.crc").display()

-- COMMAND ----------

{"tableSizeBytes":2994,"numFiles":2,"numMetadata":1,"numProtocol":1,"protocol":{"minReaderVersion":1,"minWriterVersion":2},"metadata":{"id":"46aeffee-ec8f-4c82-8280-69b445b0b068","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"emp_id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"emp_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"gender\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"salary\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"department\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"description":"First Delta Table "},"createdTime":1679894556559},"histogramOpt":{"sortedBinBoundaries":[0,8192,16384,32768,65536,131072,262144,524288,1048576,2097152,4194304,8388608,12582912,16777216,20971520,25165824,29360128,33554432,37748736,41943040,50331648,58720256,67108864,75497472,83886080,92274688,100663296,109051904,117440512,125829120,130023424,134217728,138412032,142606336,146800640,150994944,167772160,184549376,201326592,218103808,234881024,251658240,268435456,285212672,301989888,318767104,335544320,352321536,369098752,385875968,402653184,419430400,436207616,452984832,469762048,486539264,503316480,520093696,536870912,553648128,570425344,587202560,603979776,671088640,738197504,805306368,872415232,939524096,1006632960,1073741824,1140850688,1207959552,1275068416,1342177280,1409286144,1476395008,1610612736,1744830464,1879048192,2013265920,2147483648,2415919104,2684354560,2952790016,3221225472,3489660928,3758096384,4026531840,4294967296,8589934592,17179869184,34359738368,68719476736,137438953472,274877906944],"fileCounts":[2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"totalBytes":[2994,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]},"txnId":"b04dd399-ec41-4292-a0d9-ae804c41c751","allFiles":[{"path":"part-00000-876e3a98-e2ff-45f7-ba85-840719972dd5-c000.snappy.parquet","partitionValues":{},"size":1497,"modificationTime":1679895718000,"dataChange":false,"stats":"{\"numRecords\":1,\"minValues\":{\"emp_id\":100,\"emp_name\":\"a\",\"gender\":\"M\",\"salary\":10000,\"department\":\"DE\"},\"maxValues\":{\"emp_id\":100,\"emp_name\":\"a\",\"gender\":\"M\",\"salary\":10000,\"department\":\"DE\"},\"nullCount\":{\"emp_id\":0,\"emp_name\":0,\"gender\":0,\"salary\":0,\"department\":0}}","tags":{"INSERTION_TIME":"1679895718000000","MIN_INSERTION_TIME":"1679895718000000","MAX_INSERTION_TIME":"1679895718000000","OPTIMIZE_TARGET_SIZE":"268435456"}},{"path":"part-00000-0ba58726-398d-4713-966a-47e52a072377-c000.snappy.parquet","partitionValues":{},"size":1497,"modificationTime":1679895882000,"dataChange":false,"stats":"{\"numRecords\":1,\"minValues\":{\"emp_id\":200,\"emp_name\":\"b\",\"gender\":\"F\",\"salary\":20000,\"department\":\"DS\"},\"maxValues\":{\"emp_id\":200,\"emp_name\":\"b\",\"gender\":\"F\",\"salary\":20000,\"department\":\"DS\"},\"nullCount\":{\"emp_id\":0,\"emp_name\":0,\"gender\":0,\"salary\":0,\"department\":0}}","tags":{"INSERTION_TIME":"1679895882000000","MIN_INSERTION_TIME":"1679895882000000","MAX_INSERTION_TIME":"1679895882000000","OPTIMIZE_TARGET_SIZE":"268435456"}}]}

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.text("dbfs:/user/hive/warehouse/employee_demo/_delta_log/00000000000000000003.crc").display()

-- COMMAND ----------

{"tableSizeBytes":4491,"numFiles":3,"numMetadata":1,"numProtocol":1,"protocol":{"minReaderVersion":1,"minWriterVersion":2},"metadata":{"id":"46aeffee-ec8f-4c82-8280-69b445b0b068","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"emp_id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"emp_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"gender\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"salary\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"department\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"description":"First Delta Table "},"createdTime":1679894556559},"histogramOpt":{"sortedBinBoundaries":[0,8192,16384,32768,65536,131072,262144,524288,1048576,2097152,4194304,8388608,12582912,16777216,20971520,25165824,29360128,33554432,37748736,41943040,50331648,58720256,67108864,75497472,83886080,92274688,100663296,109051904,117440512,125829120,130023424,134217728,138412032,142606336,146800640,150994944,167772160,184549376,201326592,218103808,234881024,251658240,268435456,285212672,301989888,318767104,335544320,352321536,369098752,385875968,402653184,419430400,436207616,452984832,469762048,486539264,503316480,520093696,536870912,553648128,570425344,587202560,603979776,671088640,738197504,805306368,872415232,939524096,1006632960,1073741824,1140850688,1207959552,1275068416,1342177280,1409286144,1476395008,1610612736,1744830464,1879048192,2013265920,2147483648,2415919104,2684354560,2952790016,3221225472,3489660928,3758096384,4026531840,4294967296,8589934592,17179869184,34359738368,68719476736,137438953472,274877906944],"fileCounts":[3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"totalBytes":[4491,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]},"txnId":"20e8512f-0440-4739-86f6-037dde95f3dd","allFiles":[{"path":"part-00000-876e3a98-e2ff-45f7-ba85-840719972dd5-c000.snappy.parquet","partitionValues":{},"size":1497,"modificationTime":1679895718000,"dataChange":false,"stats":"{\"numRecords\":1,\"minValues\":{\"emp_id\":100,\"emp_name\":\"a\",\"gender\":\"M\",\"salary\":10000,\"department\":\"DE\"},\"maxValues\":{\"emp_id\":100,\"emp_name\":\"a\",\"gender\":\"M\",\"salary\":10000,\"department\":\"DE\"},\"nullCount\":{\"emp_id\":0,\"emp_name\":0,\"gender\":0,\"salary\":0,\"department\":0}}","tags":{"INSERTION_TIME":"1679895718000000","MIN_INSERTION_TIME":"1679895718000000","MAX_INSERTION_TIME":"1679895718000000","OPTIMIZE_TARGET_SIZE":"268435456"}},{"path":"part-00000-ebd336b1-ec75-4bb8-979a-09991e347f30-c000.snappy.parquet","partitionValues":{},"size":1497,"modificationTime":1679895884000,"dataChange":false,"stats":"{\"numRecords\":1,\"minValues\":{\"emp_id\":300,\"emp_name\":\"c\",\"gender\":\"F\",\"salary\":22000,\"department\":\"DS\"},\"maxValues\":{\"emp_id\":300,\"emp_name\":\"c\",\"gender\":\"F\",\"salary\":22000,\"department\":\"DS\"},\"nullCount\":{\"emp_id\":0,\"emp_name\":0,\"gender\":0,\"salary\":0,\"department\":0}}","tags":{"INSERTION_TIME":"1679895884000000","MIN_INSERTION_TIME":"1679895884000000","MAX_INSERTION_TIME":"1679895884000000","OPTIMIZE_TARGET_SIZE":"268435456"}},{"path":"part-00000-0ba58726-398d-4713-966a-47e52a072377-c000.snappy.parquet","partitionValues":{},"size":1497,"modificationTime":1679895882000,"dataChange":false,"stats":"{\"numRecords\":1,\"minValues\":{\"emp_id\":200,\"emp_name\":\"b\",\"gender\":\"F\",\"salary\":20000,\"department\":\"DS\"},\"maxValues\":{\"emp_id\":200,\"emp_name\":\"b\",\"gender\":\"F\",\"salary\":20000,\"department\":\"DS\"},\"nullCount\":{\"emp_id\":0,\"emp_name\":0,\"gender\":0,\"salary\":0,\"department\":0}}","tags":{"INSERTION_TIME":"1679895882000000","MIN_INSERTION_TIME":"1679895882000000","MAX_INSERTION_TIME":"1679895882000000","OPTIMIZE_TARGET_SIZE":"268435456"}}]}

-- COMMAND ----------

select * from employee_demo

-- COMMAND ----------

INSERT INTO employee_demo values(400, "d", "F", 20000, "DS");
INSERT INTO employee_demo values(500, "e", "M", 22000, "DE");
INSERT INTO employee_demo values(600, "f", "M", 25000, "DA");
INSERT INTO employee_demo values(700, "g", "M", 22000, "DE");
INSERT INTO employee_demo values(800, "h", "M", 25000, "DA")

-- COMMAND ----------

select * from employee_demo

-- COMMAND ----------

drop table employee_demo

-- COMMAND ----------

INSERT INTO employee_demo values(400, "d", "F", 20000, "DS");

-- COMMAND ----------

INSERT INTO employee_demo values(400, "d", "F", 20000, "DS"),
(500, "e", "M", 22000, "DE"),
(600, "f", "M", 25000, "DA"),
(700, "g", "M", 22000, "DE"),
(800, "h", "M", 25000, "DA")

-- COMMAND ----------

INSERT INTO employee_demo values(400, "d", "F", 20000, "DS");
INSERT INTO employee_demo values(500, "e", "M", 22000, "DE");
INSERT INTO employee_demo values(600, "f", "M", 25000, "DA");
INSERT INTO employee_demo values(700, "g", "M", 22000, "DE");
INSERT INTO employee_demo values(800, "h", "M", 25000, "DA")

-- COMMAND ----------

select * from employee_demo

-- COMMAND ----------

delete from employee_demo where emp_id =400

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC head dbfs:/user/hive/warehouse/employee_demo/_delta_log/00000000000000000010.json

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.parquet("dbfs:/user/hive/warehouse/employee_demo/_delta_log/00000000000000000010.checkpoint.parquet").display()

-- COMMAND ----------

UPDATE employee_demo
set salary = salary+1111
where emp_id=100

-- COMMAND ----------

select * from employee_demo

-- COMMAND ----------

-- MAGIC %fs 
-- MAGIC head dbfs:/user/hive/warehouse/employee_demo/_delta_log/00000000000000000011.json

-- COMMAND ----------

UPDATE employee_demo
set salary = salary+2222
where emp_id=500

-- COMMAND ----------

INSERT INTO employee_demo values(900, "d", "F", 20000, "DS");
INSERT INTO employee_demo values(1000, "e", "M", 22000, "DE");
INSERT INTO employee_demo values(901, "f", "M", 25000, "DA");
INSERT INTO employee_demo values(701, "g", "M", 22000, "DE");
INSERT INTO employee_demo values(801, "h", "M", 25000, "DA");
INSERT INTO employee_demo values(501, "h", "M", 25000, "DA");
INSERT INTO employee_demo values(401, "h", "M", 25000, "DA");

-- COMMAND ----------

INSERT INTO employee_demo values(5701, "h", "M", 25000, "DA");
INSERT INTO employee_demo values(4051, "h", "M", 25000, "DA");

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.parquet("dbfs:/user/hive/warehouse/employee_demo/_delta_log/00000000000000000020.checkpoint.parquet").display()

-- COMMAND ----------

part-00000-da656041-9ad4-4047-87d5-a92c1808cafa-c000.snappy.parquet
part-00001-aac0eb0e-d3b3-4e2b-87ff-4226967bd7a4-c000.snappy.parquet

-- COMMAND ----------

select * from employee_demo

-- COMMAND ----------

delete from employee_demo where emp_id =500

-- COMMAND ----------

select * from employee_demo

-- COMMAND ----------

drop table employee_demo

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE IF NOT EXISTS employee_demo (
-- MAGIC   emp_id INT,
-- MAGIC   emp_name STRING,
-- MAGIC   gender STRING,
-- MAGIC   salary INT,
-- MAGIC   department STRING
-- MAGIC )using delta

-- COMMAND ----------

INSERT INTO employee_demo values(5701, "h", "M", 25000, "DA");
INSERT INTO employee_demo values(4051, "h", "M", 25000, "DA");

-- COMMAND ----------

select * from employee_demo

-- COMMAND ----------

delete from employee_demo where emp_id =5701

-- COMMAND ----------

select * from employee_demo

-- COMMAND ----------

drop table employee_demo

-- COMMAND ----------

