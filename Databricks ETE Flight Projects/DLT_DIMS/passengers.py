import dlt

from pyspark.sql.functions import *

expectations = {
    "rule_1": "name IS NOT NULL",
    "rule_2": "gender IS NOT NULL",
    "rule_3": "nationality IS NOT NULL"
}

#Creating View For Transformations
@dlt.view()
@dlt.expect_all_or_fail(expectations)
def trans_passengers():
    df = spark.readStream.format("delta").load("abfss://bronze@dbdatalakejess.dfs.core.windows.net/dim_passengers")
    df = df.withColumn("update_date", current_timestamp())\
    .drop("_rescued_data")
    return df


#Creating Our Final Table With Slowly Changing Dimension Type 1(UPSERT)
dlt.create_streaming_table("silver_passengers")

dlt.create_auto_cdc_flow(
    target = "silver_passengers",
    source = "trans_passengers",
    keys = ["passenger_id"],
    sequence_by = "update_date",
    stored_as_scd_type = 1
)
