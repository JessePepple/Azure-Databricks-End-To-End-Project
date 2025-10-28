import dlt
from pyspark.sql.functions import *

expectations = {
    "rule 1": "airport_id IS NOT NULL",
    "rule 2": "city IS NOT NULL",
    "rule 3" : "country IS NOT NULL"
}
@dlt.view(
    name = "airports_trans",
)
@dlt.expect_all_or_fail(expectations)
def airports_trans():
    df = spark.readStream.format("delta").load("abfss://bronze@dbdatalakejess.dfs.core.windows.net/dim_airports")
    df = df.drop("_rescued_data")\
    .withColumn("update_date", current_timestamp())

    return df

dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
    target = "silver_airports",
    source = "airports_trans",
    keys = ["airport_id"],
    sequence_by = "update_date",
    stored_as_scd_type = 1
)
