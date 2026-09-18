import dlt
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StringType, DoubleType

# 1. Define the schema
json_schema = StructType() \
  .add("user_id", StringType()) \
  .add("event_time", StringType()) \
  .add("event_type", StringType()) \
  .add("item_id", StringType()) \
  .add("price", DoubleType())

# 2. Create the Silver table with the 5-byte fix
@dlt.table(
  name="silver_checkouts_schema_registry",
  comment="Parsed JSON checkout data with Confluent header removed."
)
def silver_checkouts():
  return (
    dlt.read_stream("bronze_checkouts")
    
    # MAGIC FIX: Skip the 5-byte Confluent header! (Spark substring is 1-indexed, so we start at position 6)
    .withColumn("clean_bytes", expr("substring(value, 6)"))
    
    # Cast the clean bytes to a string
    .withColumn("json_text", col("clean_bytes").cast("string"))
    
    # Parse the perfectly clean JSON
    .withColumn("parsed_data", from_json(col("json_text"), json_schema))
    
    # Select only the columns, dropping the raw Kafka fields
    .select("parsed_data.*")
  )