from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, hour, col, day, month, year,weekday, create_map, lit

def mapping_data_from_dict(custom_dict):
    return create_map([lit(x) for i in custom_dict.items() for x in i])

data_lake_dir = 'hdfs:///datalake/uber-data-analytics'
spark = SparkSession.builder.appName('data-transformation')\
    .getOrCreate()


df = spark.read.parquet(f'{data_lake_dir}/raw_data/uber_data.csv')

# transform
#  datetime_dim
datetime_dim = df.select('tpep_pickup_datetime', 'tpep_dropoff_datetime').dropDuplicates()
datetime_dim = datetime_dim\
    .withColumn('pick_hour', hour(col('tpep_pickup_datetime')))\
    .withColumn('pick_day', day(col('tpep_pickup_datetime')))\
    .withColumn('pick_month', month(col('tpep_pickup_datetime')))\
    .withColumn('pick_year', year(col('tpep_pickup_datetime')))\
    .withColumn('pick_weekday', weekday(col('tpep_pickup_datetime')))\
    .withColumn('drop_hour', hour(col('tpep_dropoff_datetime')))\
    .withColumn('drop_day', day(col('tpep_dropoff_datetime')))\
    .withColumn('drop_month', month(col('tpep_dropoff_datetime')))\
    .withColumn('drop_year', year(col('tpep_dropoff_datetime')))\
    .withColumn('drop_weekday', weekday(col('tpep_dropoff_datetime')))\
    .withColumn('datetime_id', monotonically_increasing_id() + 1)

datetime_dim = datetime_dim.select('datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month', 'pick_year', 'pick_weekday', 'tpep_dropoff_datetime', 'drop_hour', 'drop_day', 'drop_month', 'drop_year','drop_weekday')
datetime_dim.show()

# passenger_count_dim
passenger_count_dim = df.select('passenger_count')\
    .dropDuplicates()\
    .orderBy(df.passenger_count.asc())
passenger_count_dim = passenger_count_dim\
    .withColumn('passenger_count_id', monotonically_increasing_id() + 1)
passenger_count_dim = passenger_count_dim.select('passenger_count_id', 'passenger_count')

passenger_count_dim.show()

# trip_distance_dim
trip_distance_dim = df.select('trip_distance')\
    .dropDuplicates()\
    .orderBy(df.trip_distance.asc())
trip_distance_dim = trip_distance_dim\
    .withColumn('trip_distance_id', monotonically_increasing_id() + 1)
trip_distance_dim = trip_distance_dim.select('trip_distance_id', 'trip_distance')
trip_distance_dim.show()

# rate_code_dim
rate_code_type = {
    1 : 'Standard rate',
    2 : 'JFK',
    3 : 'Neward',
    4 : 'Nassau or Westchester',
    5 : 'Negotiated fare',
    6 : 'Group ride'
}

rate_code_dim = df.select('RatecodeID')\
    .dropDuplicates()\
    .orderBy(df.RatecodeID.asc())
rate_code_dim = rate_code_dim\
    .withColumn('rate_code_name', mapping_data_from_dict(rate_code_type)[col('RatecodeID')])\
    .withColumn('rate_code_id', monotonically_increasing_id() + 1)
rate_code_dim = rate_code_dim.select('rate_code_id','RatecodeID','rate_code_name')
rate_code_dim.show()

# payment_type_dim
payment_type = {
    1 : 'Credit Card',
    2 : 'Cash',
    3 : 'No charge',
    4 : 'Dispute',
    5 : 'Unknown',
    6 : 'Voided trip'
}

payment_type_dim = df.select('payment_type')\
    .dropDuplicates()\
    .orderBy(df.payment_type.asc())
payment_type_dim = payment_type_dim\
    .withColumn('payment_type_name', mapping_data_from_dict(payment_type)[col('payment_type')])\
    .withColumn('payment_type_id', monotonically_increasing_id() + 1)
payment_type_dim = payment_type_dim.select('payment_type_id', 'payment_type', 'payment_type_name')
payment_type_dim.show()

# pickup_location_dim
pickup_location_dim = df.select('pickup_longitude','pickup_latitude').dropDuplicates()
pickup_location_dim = pickup_location_dim.withColumn('pickup_location_id', monotonically_increasing_id() + 1)
pickup_location_dim = pickup_location_dim.select('pickup_location_id', 'pickup_longitude', 'pickup_latitude')
pickup_location_dim.show()

# dropoff_location_dim
dropoff_location_dim = df.select('dropoff_longitude','dropoff_latitude').dropDuplicates()
dropoff_location_dim = dropoff_location_dim.withColumn('dropoff_location_id', monotonically_increasing_id() + 1)
dropoff_location_dim = dropoff_location_dim.select('dropoff_location_id', 'dropoff_longitude', 'dropoff_latitude')
dropoff_location_dim.show()

# fact_table
fact_table = df\
    .join(passenger_count_dim, on='passenger_count')\
    .join(trip_distance_dim, on='trip_distance')\
    .join(rate_code_dim, on='RatecodeID')\
    .join(pickup_location_dim, on=['pickup_longitude','pickup_latitude'])\
    .join(dropoff_location_dim, on=['dropoff_longitude','dropoff_latitude'])\
    .join(datetime_dim, on=['tpep_pickup_datetime','tpep_dropoff_datetime'])\
    .join(payment_type_dim, on='payment_type')\
    .select('VendorID', 'datetime_id', 'passenger_count_id', 'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag', 'pickup_location_id', 'dropoff_location_id', 'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount')
fact_table.show()


# save file to data lake again
datetime_dim.write.mode('overwrite').parquet(f'{data_lake_dir}/transformed_data/datetime_dim.csv')
passenger_count_dim.write.mode('overwrite').parquet(f'{data_lake_dir}/transformed_data/passenger_count_dim.csv')
trip_distance_dim.write.mode('overwrite').parquet(f'{data_lake_dir}/transformed_data/trip_distance_dim.csv')
rate_code_dim.write.mode('overwrite').parquet(f'{data_lake_dir}/transformed_data/rate_code_dim.csv')
payment_type_dim.write.mode('overwrite').parquet(f'{data_lake_dir}/transformed_data/payment_type_dim.csv')
pickup_location_dim.write.mode('overwrite').parquet(f'{data_lake_dir}/transformed_data/pickup_location_dim.csv')
dropoff_location_dim.write.mode('overwrite').parquet(f'{data_lake_dir}/transformed_data/dropoff_location_dim.csv')
fact_table.write.mode('overwrite').parquet(f'{data_lake_dir}/transformed_data/fact_table.csv')

spark.stop()