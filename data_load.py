from pyspark.sql import SparkSession

data_lake_dir = 'hdfs:///datalake/uber-data-analytics'
spark = SparkSession\
    .builder\
    .appName('load data to warehouse')\
    .config('spark.driver.extraClassPath','/home/hadoop/hive/lib/postgresql-42.7.1.jar')\
    .getOrCreate()


datetime_dim = spark.read.parquet(f'{data_lake_dir}/transformed_data/datetime_dim.csv')
passenger_count_dim = spark.read.parquet(f'{data_lake_dir}/transformed_data/passenger_count_dim.csv')
trip_distance_dim = spark.read.parquet(f'{data_lake_dir}/transformed_data/trip_distance_dim.csv')
rate_code_dim = spark.read.parquet(f'{data_lake_dir}/transformed_data/rate_code_dim.csv')
payment_type_dim = spark.read.parquet(f'{data_lake_dir}/transformed_data/payment_type_dim.csv')
pickup_location_dim = spark.read.parquet(f'{data_lake_dir}/transformed_data/pickup_location_dim.csv')
dropoff_location_dim = spark.read.parquet(f'{data_lake_dir}/transformed_data/dropoff_location_dim.csv')
fact_table = spark.read.parquet(f'{data_lake_dir}/transformed_data/fact_table.csv')

# load to db
datetime_dim.write\
    .format('jdbc')\
    .mode('overwrite')\
    .option('url', 'jdbc:postgresql://localhost:5432/OLAP_Uber_Data_Analytics')\
    .option("driver", "org.postgresql.Driver")\
    .option("table", "datetime_dim")\
    .option("user", "postgres")\
    .option("password", "123456")\
    .save()

passenger_count_dim.write\
    .format('jdbc')\
    .mode('overwrite')\
    .option('url', 'jdbc:postgresql://localhost:5432/OLAP_Uber_Data_Analytics')\
    .option("driver", "org.postgresql.Driver")\
    .option("table", "passenger_count_dim")\
    .option("user", "postgres")\
    .option("password", "123456")\
    .save()

trip_distance_dim.write\
    .format('jdbc')\
    .mode('overwrite')\
    .option('url', 'jdbc:postgresql://localhost:5432/OLAP_Uber_Data_Analytics')\
    .option("driver", "org.postgresql.Driver")\
    .option("table", "trip_distance_dim")\
    .option("user", "postgres")\
    .option("password", "123456")\
    .save()

rate_code_dim.write\
    .format('jdbc')\
    .mode('overwrite')\
    .option('url', 'jdbc:postgresql://localhost:5432/OLAP_Uber_Data_Analytics')\
    .option("driver", "org.postgresql.Driver")\
    .option("table", "rate_code_dim")\
    .option("user", "postgres")\
    .option("password", "123456")\
    .save()

payment_type_dim.write\
    .format('jdbc')\
    .mode('overwrite')\
    .option('url', 'jdbc:postgresql://localhost:5432/OLAP_Uber_Data_Analytics')\
    .option("driver", "org.postgresql.Driver")\
    .option("table", "payment_type_dim")\
    .option("user", "postgres")\
    .option("password", "123456")\
    .save()

pickup_location_dim.write\
    .format('jdbc')\
    .mode('overwrite')\
    .option('url', 'jdbc:postgresql://localhost:5432/OLAP_Uber_Data_Analytics')\
    .option("driver", "org.postgresql.Driver")\
    .option("table", "pickup_location_dim")\
    .option("user", "postgres")\
    .option("password", "123456")\
    .save()

dropoff_location_dim.write\
    .format('jdbc')\
    .mode('overwrite')\
    .option('url', 'jdbc:postgresql://localhost:5432/OLAP_Uber_Data_Analytics')\
    .option("driver", "org.postgresql.Driver")\
    .option("table", "dropoff_location_dim")\
    .option("user", "postgres")\
    .option("password", "123456")\
    .save()

fact_table.write\
    .format('jdbc')\
    .mode('overwrite')\
    .option('url', 'jdbc:postgresql://localhost:5432/OLAP_Uber_Data_Analytics')\
    .option("driver", "org.postgresql.Driver")\
    .option("table", "fact_table")\
    .option("user", "postgres")\
    .option("password", "123456")\
    .save()

spark.stop()