import dlt
from pyspark.sql.functions import *


#Transformations Procedures

@dlt.view(
    name = "flights_transformation",
    comment = "Transformed Procedures"
)

def flights_transformation():
    df = spark.readStream.format("delta").load("abfss://bronze@dbdatalakejess.dfs.core.windows.net/dim_flights")
    df = df.withColumn("flight_date", to_date(col("flight_date")))\
    .drop("_rescued_data")\
    .withColumn("update_date", current_timestamp())
    return df

#Final Table 
dlt.create_streaming_table("silver_flights")

dlt.create_auto_cdc_flow(
    target = "silver_flights",
    source = "flights_transformation",
    keys = ["flight_id"],
    sequence_by= "update_date",
    stored_as_scd_type = 1
)