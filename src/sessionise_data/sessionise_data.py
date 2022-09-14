import pyspark
import pyspark.sql.functions as sf
from pyspark.sql.functions import from_json
from pyspark.sql.types import *
from pyspark.sql import DataFrame
import sessionise_methods as sm


# pipe method for convenience
def pipe(self, func, *args, **kwargs):
    return func(self, *args, **kwargs)
DataFrame.pipe = pipe

# build a lil spark session
master = 'local[4]'
spark = (
    pyspark.sql.SparkSession.builder
    .master(master)
    .config("spark.jars", "gcs-connector-hadoop3-latest.jar")
    .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.0")
    .getOrCreate()
)
spark

# configs for google cloud connection

conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

# read event/events data from google
event_raw_data_path = "gs://webshop-simulation-streaming-landing/datalake/raw/event"
events_raw_data_path = "gs://webshop-simulation-streaming-landing/datalake/raw/events"

raw = spark.read.parquet(event_raw_data_path)
raw_events = spark.read.parquet(events_raw_data_path)

# flatten event data
schema = ArrayType(
    StructType([StructField("product", IntegerType()),
               StructField("customer-id", IntegerType())]))

flattened_data = (
    raw
    .withColumn('event', from_json(sf.col('event'), schema))
    .withColumn('product', sf.col('event.product').getItem(0))
    .withColumn('customer-id', sf.col('event.customer-id').getItem(0))
    .drop('event')
)

# sessionise data
sessionised_data = (
    raw_events
    .pipe(sm.union_dfs, flattened_data)
    .pipe(sm.merge_timestamps, 'timestamp', 'modified_at', 1, 1000000)
    .pipe(sm.session_df, 'timestamp', 'customer-id', session_length=1800)
)

# write to google cloud
sessionised_data.write.parquet("gs://webshop-simulation-streaming-landing/datalake/silver/sessionised")