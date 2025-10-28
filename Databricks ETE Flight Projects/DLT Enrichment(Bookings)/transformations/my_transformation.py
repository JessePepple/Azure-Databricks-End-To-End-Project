import dlt
from pyspark.sql.functions import col, current_timestamp, to_date
from pyspark.sql.types import *


# Setting Expectations

expectations = {
    "booking_id_not_null": "booking_id IS NOT NULL",
    "passenger_id_not_null": "passenger_id IS NOT NULL",
    "flight_id_not_null": "flight_id IS NOT NULL",
    "airport_id_not_null": "airport_id IS NOT NULL"
}


 # Bronze → DLT table

@dlt.table(
    comment="Raw booking data loaded from the bronze layer"
)
def bookings():
    return (
        spark.readStream.format("delta")
        .load("abfss://bronze@dbdatalakejess.dfs.core.windows.net/bookings")
    )

#  Silver View (Transformation)

@dlt.view(
    comment="Transformed booking data for the silver layer"
)
def trans_bookings():
    df = dlt.read_stream("bookings")

    df = (
        df.drop("_rescued_data")
        .withColumnRenamed("amount", "booking_amount")
        .withColumn("booking_amount", col("booking_amount").cast("double"))
        .withColumn("update_date", current_timestamp())
        .withColumn("booking_date", to_date(col("booking_date")))
    )
    return df


# Silver Table (With Expectations)

@dlt.table(
    name="silver_bookings",
    comment="Cleansed booking data after validation and transformation"
)
@dlt.expect_all_or_fail(expectations)
def silver_bookings():
    return dlt.read_stream("trans_bookings")
