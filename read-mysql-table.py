from pyspark.sql import SparkSession

if __name__ == "__main__":
  print("Read MySQL Table - Application Started!\n")

  spark = SparkSession \
          .builder \
          .appName("PySpark MySQL Connection") \
          .master("local[*]") \
          .config("spark.jars", "file:///home/worker-1/mysql-connector/mysql-connector-java-8.0.33.jar") \
          .config("spark.executor.extraClassPath", "file:///home/worker-1/mysql-connector/mysql-connector-java-8.0.33.jar") \
          .config("spark.executor.extraLibrary", "file:///home/worker-1/mysql-connector/mysql-connector-java-8.0.33.jar") \
          .config("spark.driver.extraClassPath", "file:///home/worker-1/mysql-connector/mysql-connector-java-8.0.33.jar") \
          .enableHiveSupport() \
          .getOrCreate()
  
  spark.sparkContext.setLogLevel("ERROR")

  mysql_db_driver_class = "com.cj.mysql.jdbc.Driver"
  table_name = "Weather"
  host_name = "localhost"
  port_no = str(3306)
  user_name = "ilokuda"
  password = "ilokudatangjiro"
  database_name = "pyspark_database"

  mysql_select_query = None
  mysql_select_query = "(SELECT * FROM " + table_name + ") AS " + table_name
  print("\nMySQL Select query:")
  print(mysql_select_query,"\n")

  mysql_jdbc_url = "jdbc:mysql://" + host_name + ":" + port_no + "/" + database_name

  print("JDBC url: " + mysql_jdbc_url,"\n")

  users_data_df = spark.read.format("jdbc") \
                  .option("url", mysql_jdbc_url) \
                  .option("driver", mysql_db_driver_class) \
                  .option("dbtable", mysql_select_query) \
                  .option("user", user_name) \
                  .option("password", password) \
                  .load()
  
  users_data_df.show(10, False)

  print("Read MySQL Table - Application Completed!")
