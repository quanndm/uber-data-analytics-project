from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('data-ingestion').getOrCreate()


df = spark.read\
    .option('header','true')\
    .option('inferSchema','true')\
    .csv('/user/hadoop/my_folder/project/uber-data-analytics/uber-data.csv')

data_lake_dir = 'hdfs:///datalake/uber-data-analytics'
# save file to hdfs
df.write.mode('overwrite').parquet(f'{data_lake_dir}/raw_data/uber_data.csv')

# end section
print("ingest raw data to data lake!")

spark.stop()