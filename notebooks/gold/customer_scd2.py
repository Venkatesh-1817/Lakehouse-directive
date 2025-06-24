from pyspark.sql.functions import *
from delta.tables import DeltaTable

# Read cleaned data
df_silver = spark.table("uc_catalog.edulytics.silver_orders")

# Create current snapshot of customer dimension
df_customer = (
    df_silver.groupBy("customer_id")
    .agg(
        {"payment_method": "first", "country": "first"}  
    )
    .withColumnRenamed("first(payment_method)", "payment_method")
    .withColumnRenamed("first(country)", "country")
    .withColumn("effective_start_time", current_timestamp())
    .withColumn("effective_end_time", lit(None).cast("timestamp"))
    .withColumn("is_current", lit(True))
    .withColumn("updated_at", current_timestamp())
)

# Target SCD2 table name
target_table = "uc_catalog.edulytics.dim_customer"

# If the table doesn't exist, create it
if not spark._jsparkSession.catalog().tableExists(target_table):
    df_customer.write.format("delta").saveAsTable(target_table)
else:
    dim_customer = DeltaTable.forName(spark, target_table)

    # Perform SCD2 merge
    dim_customer.alias("target").merge(
        df_customer.alias("source"),
        "target.customer_id = source.customer_id AND target.is_current = true AND (target.payment_method != source.payment_method OR target.country != source.country)"
    ).whenMatchedUpdate(set={
        "effective_end_time": "current_timestamp()",
        "is_current": "false",
        "updated_at": "current_timestamp()"
    }).whenNotMatchedInsertAll().execute()
