import pyspark
import pyspark.sql.functions as sf
from pyspark.sql.functions import from_json
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql.window import Window


def union_dfs(df, df1):
#     union two data frames of different shapes, missing cols are null values
    return(df.unionByName(df1, allowMissingColumns=True))

def merge_timestamps(df, timestamp, timestamp1, timestamp_divisor, timestamp1_divisor):
#     merge unix time cols from two unioned tables, and convert to timestamp. Use divisors where cols need conversion from ms to s etc.
    return (
        df
        .withColumn(timestamp, sf.when(sf.col(timestamp).isNull(), sf.round(sf.col(timestamp1)/timestamp1_divisor)).otherwise(sf.col(timestamp)/timestamp_divisor))
        .withColumn(timestamp, sf.to_timestamp(sf.from_unixtime(sf.col(timestamp))))
    )


def session_df(df, time_col, group_by, session_length):
    window = Window.partitionBy(group_by).orderBy(time_col)

    window_session_id = (Window.partitionBy(group_by).orderBy(time_col)
                         .rangeBetween(Window.unboundedPreceding, 0))
    return (
        df
        .filter(sf.col(group_by).isNotNull())
        .withColumn('time_diff', sf.col(time_col).cast("long") - sf.lag(time_col).over(window).cast("long"))
        .withColumn('new_session', sf.col('time_diff') > session_length)
        .withColumn('new_session',
                    sf.when(sf.col('new_session').isNull(), sf.lit(False)).otherwise(sf.col('new_session')))
        .withColumn('session_id', sf.sum(sf.col('new_session').cast('long')).over(window_session_id))
        .drop('new_session')
        .drop('time_diff')

    )