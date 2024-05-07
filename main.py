import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PySpark MySQL Connection") \
    .config("spark.jars", "/home/worker-1/mysql-connector/mysql-connector-java-8.0.33.jar") \
    .getOrCreate()

url = "jdbc:mysql://localhost:3306/pyspark_database"

properties = {
    "user": "ilokuda",
    "password": "ilokudatangjiro",
    "driver": "com.mysql.jdbc.Driver"
    }

table_name = "Weather"

df = spark.read.jdbc(url, table_name, properties=properties)

#df.createOrReplaceTempView("temporary_table")

#sql_query = "SELECT * FROM temporary_table"

#result_df = spark.sql(sql_query)
