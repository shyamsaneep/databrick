-- Databricks notebook source
-- MAGIC %python
-- MAGIC employees = [(1, "Scott", "Tiger", 1000.0, 
-- MAGIC                       "united states"
-- MAGIC                      )]
-- MAGIC df = spark. \
-- MAGIC     createDataFrame(employees,
-- MAGIC                     schema="""employee_id INT, first_name STRING, 
-- MAGIC                     last_name STRING, salary FLOAT, nationality STRING
-- MAGIC                     """
-- MAGIC                    )
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.createOrReplaceTempView("df_vw")

-- COMMAND ----------

select * from df_vw

-- COMMAND ----------

CREATE TABLE employee_test(
employee_id INT, 
first_name STRING, 
last_name STRING,
salary INT, 
nationality STRING
)

-- COMMAND ----------

select * from employee_test

-- COMMAND ----------

MERGE INTO employee_test AS Target
USING df_vw AS source
ON Target.employee_id = source.employee_id
WHEN MATCHED THEN
  UPDATE SET
    Target.first_name = source.first_name,
    Target.last_name = source.last_name,
    Target.salary = source.salary,
    Target.nationality = source.nationality 
WHEN NOT MATCHED
  THEN INSERT (
    employee_id,
    first_name,
    last_name,
    salary,
    nationality
  )
  VALUES (
   employee_id,
    first_name,
    last_name,
    salary,
    nationality
  )

-- COMMAND ----------

select * from employee_test

-- COMMAND ----------

-- MAGIC %python
-- MAGIC employees = [(1, "Scott", "Tiger", 1000.0, 
-- MAGIC                       "India"
-- MAGIC                      ), 
-- MAGIC             (2, "John", "MIll", 2000.0, "UK")]
-- MAGIC df = spark. \
-- MAGIC     createDataFrame(employees,
-- MAGIC                     schema="""employee_id INT, first_name STRING, 
-- MAGIC                     last_name STRING, salary FLOAT, nationality STRING
-- MAGIC                     """
-- MAGIC                    )
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC df.createOrReplaceTempView("df_vw")

-- COMMAND ----------

select * from df_vw

-- COMMAND ----------

select * from employee_test

-- COMMAND ----------

describe history employee_test

-- COMMAND ----------

