from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('data-ingestion').getOrCreate()

my_folder_path = 'hdfs:///user/hadoop/my_file'
df = spark.read\
    .option('header','true')\
    .option('inferSchema','true')\
    .csv(f'{my_folder_path}/uber-data.csv')

data_lake_dir = 'hdfs:///datalake/uber-data-analytics'
# save file to hdfs
df.write.mode('overwrite').parquet(f'{data_lake_dir}/raw_data/uber_data.parquet')

# end section
print("ingest raw data to data lake!")

spark.stop()