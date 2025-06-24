from pyspark.sql.types import *
from pyspark.sql.functions import *

# Define schema for consistency
schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("order_date", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("delivery_status", StringType(), True)
])

# Read the raw CSV from DBFS or mounted location
df_raw = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load("abfss://lakehouse@udemydatalakestorage001.dfs.core.windows.net/") \
    .withColumn("source_file", input_file_name())

# Save to Delta table (bronze layer)
df_raw.write.format("delta").mode("overwrite").saveAsTable("uc_catalog.edulytics.bronze_orders")
