0. prepare data
- push raw data to any folder on hdfs

1. run command
```bash
spark-submit --master yarn --queue dev <<file python>>
```
2. to check spark job, go to hadoop yarn cluster url on browser
```url
http://localhost:8088/cluster
```

3. config superset
- export FLASK_APP=superset
- export SUPERSET_CONFIG_PATH=//path/to/superset_config.py 
- create secret key with command: openssl rand -base64 42 (option: run this command if run once)
- export SUPERSET_SECRET_KEY with generated key above (option: run this command if run once)
- superset db upgrade 
- superset fab create-admin (run once when init)
- superset load_examples (run once when init)
- superset init (run once when init)
- superset run
account : admin
password: admin

4. some example of query in data warehouse
- Find top 10 pickup locations base on number of trips
- Find total number of trips by passenger count
- Find average fare amount by hour of the day

5. create data analytics after finishing step load data.
```sql
DROP TABLE IF EXISTS tbl_analytics; 
create table tbl_analytics as (
select 
	f."VendorID",
	d.tpep_pickup_datetime,
	d.tpep_dropoff_datetime,
	passenger.passenger_count,
	td.trip_distance,
	r.rate_code_name,
	pick.pickup_latitude,
	pick.pickup_longitude,
	dropoff.dropoff_latitude,
	dropoff.dropoff_longitude,
	payment.payment_type_name,
	f.fare_amount,
	f.extra,
	f.mta_tax,
	f.tip_amount,
	f.tolls_amount,
	f.improvement_surcharge
from fact_table as f
join datetime_dim as d on d.datetime_id=f.datetime_id
join passenger_count_dim as passenger on passenger.passenger_count_id = f.passenger_count_id
join trip_distance_dim as td on td.trip_distance_id = f.trip_distance_id
join rate_code_dim as r on r.rate_code_id = f.rate_code_id
join pickup_location_dim as pick on pick.pickup_location_id = f.pickup_location_id
join dropoff_location_dim as dropoff on dropoff.dropoff_location_id = f.dropoff_location_id
join payment_type_dim as payment on payment.payment_type_id = f.payment_type_id);
```