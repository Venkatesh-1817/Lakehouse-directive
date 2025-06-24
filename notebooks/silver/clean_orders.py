from pyspark.sql.functions import col, initcap, when

# Read from bronze table
df_bronze = spark.table("uc_catalog.edulytics.bronze_orders")

# Clean and transform
df_silver = (
    df_bronze.dropDuplicates(["order_id"])  # Drop duplicate orders
    .withColumn("price", when(col("price").isNull(), 0.0).otherwise(col("price")))
    .withColumn("payment_method", when(col("payment_method").isNull(), "Unknown").otherwise(col("payment_method")))
    .withColumn("category", initcap(col("category")))  # 'electronics' -> 'Electronics'
)

# Save to silver Delta table
df_silver.write.format("delta").mode("overwrite").saveAsTable("uc_catalog.edulytics.silver_orders")
