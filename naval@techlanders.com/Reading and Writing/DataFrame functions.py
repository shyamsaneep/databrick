# Databricks notebook source
# MAGIC %run "/Users/naval@techlanders.com/Data Frame Sample"

# COMMAND ----------

df.withColumnRenamed("id","emp_id").show()

# COMMAND ----------

help(df.toDF)

# COMMAND ----------

required_column=["id","name","last_name","email","Mobile","courses","is_customer","DOB"]

# COMMAND ----------

Target_column=["emp_id","first_name","last_name","email","Mobile","courses","is_customer","DOB"]

# COMMAND ----------

df.select(required_column).toDF(Target_column).show()

# COMMAND ----------

df1= df.select(required_column).toDF(*Target_column)

# COMMAND ----------

df.columns

# COMMAND ----------

display(df1)
df1.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

help(explode)

# COMMAND ----------

df.withColumn("phone_no", explode("mobile")).show()

# COMMAND ----------

df.withColumn("mobile", explode("mobile")).show()

# COMMAND ----------

help(df.filter)

# COMMAND ----------

df1.filter("id=1").show()

# COMMAND ----------

df1.where("id=1").show()

# COMMAND ----------

df1.where("id==1").show()

# COMMAND ----------

df1.where(col("id")==1).show()

# COMMAND ----------

df1.filter(df1['emp_id']==1).show()

# COMMAND ----------

df1.filter(df1['is_customer']==True).show()

# COMMAND ----------

help(df.sort)

# COMMAND ----------

df1.sort("first_name").show()

# COMMAND ----------

df1.orderBy(col("first_name")).show()

# COMMAND ----------

df1.sort("first_name",ascending=False).show()

# COMMAND ----------

df1.sort("first_name".desc()).show()

# COMMAND ----------

df1.sort(col("first_name").desc()).show()

# COMMAND ----------

df1.sort("first_name","last_name").show()

# COMMAND ----------

courses = [{'course_id': 1,
  'course_name': '2020 Complete Python Bootcamp: From Zero to Hero in Python',
  'suitable_for': 'Beginner',
  'enrollment': 1100093,
  'stars': 4.6,
  'number_of_ratings': 318066},
 {'course_id': 4,
  'course_name': 'Angular - The Complete Guide (2020 Edition)',
  'suitable_for': 'Intermediate',
  'enrollment': 422557,
  'stars': 4.6,
  'number_of_ratings': 129984},
 {'course_id': 12,
  'course_name': 'Automate the Boring Stuff with Python Programming',
  'suitable_for': 'Advanced',
  'enrollment': 692617,
  'stars': 4.6,
  'number_of_ratings': 70508},
 {'course_id': 10,
  'course_name': 'Complete C# Unity Game Developer 2D',
  'suitable_for': 'Advanced',
  'enrollment': 364934,
  'stars': 4.6,
  'number_of_ratings': 78989},
 {'course_id': 5,
  'course_name': 'Java Programming Masterclass for Software Developers',
  'suitable_for': 'Advanced',
  'enrollment': 502572,
  'stars': 4.6,
  'number_of_ratings': 123798},
 {'course_id': 15,
  'course_name': 'Learn Python Programming Masterclass',
  'suitable_for': 'Advanced',
  'enrollment': 240790,
  'stars': 4.5,
  'number_of_ratings': 58677},
 {'course_id': 3,
  'course_name': 'Machine Learning A-Z™: Hands-On Python & R In Data Science',
  'suitable_for': 'Intermediate',
  'enrollment': 692812,
  'stars': 4.5,
  'number_of_ratings': 132228},
 {'course_id': 14,
  'course_name': 'Modern React with Redux [2020 Update]',
  'suitable_for': 'Intermediate',
  'enrollment': 203214,
  'stars': 4.7,
  'number_of_ratings': 60835},
 {'course_id': 8,
  'course_name': 'Python for Data Science and Machine Learning Bootcamp',
  'suitable_for': 'Intermediate',
  'enrollment': 387789,
  'stars': 4.6,
  'number_of_ratings': 87403},
 {'course_id': 6,
  'course_name': 'React - The Complete Guide (incl Hooks, React Router, Redux)',
  'suitable_for': 'Intermediate',
  'enrollment': 304670,
  'stars': 4.6,
  'number_of_ratings': 90964},
 {'course_id': 18,
  'course_name': 'Selenium WebDriver with Java -Basics to Advanced+Frameworks',
  'suitable_for': 'Advanced',
  'enrollment': 148562,
  'stars': 4.6,
  'number_of_ratings': 49947},
 {'course_id': 21,
  'course_name': 'Spring & Hibernate for Beginners (includes Spring Boot)',
  'suitable_for': 'Advanced',
  'enrollment': 177053,
  'stars': 4.6,
  'number_of_ratings': 45329},
 {'course_id': 7,
  'course_name': 'The Complete 2020 Web Development Bootcamp',
  'suitable_for': 'Beginner',
  'enrollment': 270656,
  'stars': 4.7,
  'number_of_ratings': 88098},
 {'course_id': 9,
  'course_name': 'The Complete JavaScript Course 2020: Build Real Projects!',
  'suitable_for': 'Intermediate',
  'enrollment': 347979,
  'stars': 4.6,
  'number_of_ratings': 83521},
 {'course_id': 16,
  'course_name': 'The Complete Node.js Developer Course (3rd Edition)',
  'suitable_for': 'Advanced',
  'enrollment': 202922,
  'stars': 4.7,
  'number_of_ratings': 50885},
 {'course_id': 13,
  'course_name': 'The Complete Web Developer Course 2.0',
  'suitable_for': 'Intermediate',
  'enrollment': 273598,
  'stars': 4.5,
  'number_of_ratings': 63175},
 {'course_id': 11,
  'course_name': 'The Data Science Course 2020: Complete Data Science Bootcamp',
  'suitable_for': 'Beginner',
  'enrollment': 325047,
  'stars': 4.5,
  'number_of_ratings': 76907},
 {'course_id': 20,
  'course_name': 'The Ultimate MySQL Bootcamp: Go from SQL Beginner to Expert',
  'suitable_for': 'Beginner',
  'enrollment': 203366,
  'stars': 4.6,
  'number_of_ratings': 45382},
 {'course_id': 2,
  'course_name': 'The Web Developer Bootcamp',
  'suitable_for': 'Beginner',
  'enrollment': 596726,
  'stars': 4.6,
  'number_of_ratings': 182997},
 {'course_id': 19,
  'course_name': 'Unreal Engine C++ Developer: Learn C++ and Make Video Games',
  'suitable_for': 'Advanced',
  'enrollment': 229005,
  'stars': 4.5,
  'number_of_ratings': 45860},
 {'course_id': 17,
  'course_name': 'iOS 13 & Swift 5 - The Complete iOS App Development Bootcamp',
  'suitable_for': 'Advanced',
  'enrollment': 179598,
  'stars': 4.8,
  'number_of_ratings': 49972}]

# COMMAND ----------

df=spark.createDataFrame(courses)

# COMMAND ----------

display(df)

# COMMAND ----------

df.sort("suitable_for","enrollment").show()

# COMMAND ----------

df.sort("suitable_for",col("enrollment").desc()).show()

# COMMAND ----------

df.sort("suitable_for",desc(col("enrollment"))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### sort courses in ascending order by suitable_for and then in descending order by number_of_ratings

# COMMAND ----------

df.\
sort(col("suitable_for"),
        desc(col("number_of_ratings"))
       ).\
show()

# COMMAND ----------

employees = [(1, "Scott", "Tiger", 1000.0, 
                      "united states", "+1 123 456 7890", "123 45 6789"
                     ),
                     (2, "Henry", "Ford", 1250.0, 
                      "India", "+91 234 567 8901", "456 78 9123"
                     ),
                     (3, "Nick", "Junior", 750.0, 
                      "united KINGDOM", "+44 111 111 1111", "222 33 4444"
                     ),
                     (4, "Bill", "Gomes", 1500.0, 
                      "AUSTRALIA", "+61 987 654 3210", "789 12 6118"
                     )
                ]

# COMMAND ----------

dfemp=spark.createDataFrame(employees,schema)

# COMMAND ----------

schema= """ employee_id INT, first_name STRING, last_name STRING, salary Float, nationality STRING, mobile STRING, ssn STRING
"""

# COMMAND ----------

display(dfemp)

# COMMAND ----------

display(dfemp.withColumn("nationality_upper", upper(col("nationality"))).\
withColumn("nationality_lower", lower(col("nationality"))).\
withColumn("nationality_init", initcap(col("nationality"))).\
withColumn("nationality_length", length(col("nationality"))))

# COMMAND ----------

help(substring)

# COMMAND ----------

df1=dfemp.withColumn("ssn_last4", substring(col("ssn"),1,4).cast("int"))
display(df1)

# COMMAND ----------

display(df1)
df1.printSchema()

# COMMAND ----------

help(split)

# COMMAND ----------

display(dfemp.withColumn("new_mobile",split("mobile", " "))\
.withColumn("country",split("mobile", " ")[0].cast("int"))
.withColumn("area_code",split("mobile", " ")[1].cast("int"))      
       )

# COMMAND ----------

simpleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]
schema = ["employee_name","department","state","salary","age","bonus"]

# COMMAND ----------

df=spark.createDataFrame(data=simpleData, schema=schema)

# COMMAND ----------

display(df)

# COMMAND ----------

df.groupBy("department").count().show()

# COMMAND ----------

# MAGIC %md 
# MAGIC .Count is an ACTION or Transformation??

# COMMAND ----------

df.groupBy("department").count().show()

# COMMAND ----------

help(df.agg)

# COMMAND ----------

df.groupBy("department").sum("salary").show()

# COMMAND ----------

df.groupBy("department").agg(sum("salary").alias("sum_salary"),
                             avg("salary").alias("avg_salary"),
                             min("salary")
                            ).show()

# COMMAND ----------

display(df.describe())

# COMMAND ----------

data = [("James","M",60000),("Michael","M",70000),
        ("Robert",None,400000),("Maria","F",500000),
        ("Jen","T",None)]

columns = ["name","gender","salary"]
df = spark.createDataFrame(data = data, schema = columns)
display(df)

# COMMAND ----------

help(when)

# COMMAND ----------

df.withColumn("new_gender", when(col("gender")=="M","Male")
             .when(col("gender")=="F","Female")
             .when(col("gender").isNull(),"Unknown")
             .otherwise(col("gender"))
             ).show()

# COMMAND ----------

l=[10,20,30,40]

# COMMAND ----------

df=spark.createDataFrame(10,'a')

# COMMAND ----------

df=spark.createDataFrame([(10,'A'),(50,'B')],['ID','NAME'])

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.filter(col("ID").isin(l)))

# COMMAND ----------

