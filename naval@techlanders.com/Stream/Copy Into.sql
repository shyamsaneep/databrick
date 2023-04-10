-- Databricks notebook source
-- Synatax
COPY INTO my_table
FROM '/path/to/files’
FILEFORMAT = <format>
FORMAT_OPTIONS (<format options>)
COPY_OPTIONS (<copy options>);


-- COMMAND ----------

drop table my_table;
Create table my_table(Id String ,Name STRING ,Gender STRING,Salary STRING,Country STRING,Date STRING)

-- COMMAND ----------

COPY INTO my_table
FROM 'dbfs:/mnt/shelladlstech/raw/streaminput/'
FILEFORMAT = CSV
FORMAT_OPTIONS('header'='true')
COPY_OPTIONS('mergeSchema'='true');    

-- COMMAND ----------

select * from my_table

-- COMMAND ----------

COPY INTO my_table
FROM 'dbfs:/mnt/shelladlstech/raw/streaminput/'
FILEFORMAT = CSV
COPY_OPTIONS('mergeSchema'='true');                

-- COMMAND ----------

select * from my_table

-- COMMAND ----------

drop table my_table

-- COMMAND ----------

Create table my_table

-- COMMAND ----------

drop table my_table_shell18

-- COMMAND ----------

create table my_table_shell18

-- COMMAND ----------

COPY INTO my_table_shell18
FROM 'dbfs:/mnt/shelladlstech/raw/streaminput/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header'= 'true',
                'inferSchema'='true')
COPY_OPTIONS ('mergeSchema'='true');

-- COMMAND ----------

select * from my_table_shell18

-- COMMAND ----------

describe extended my_table_shell18

-- COMMAND ----------

