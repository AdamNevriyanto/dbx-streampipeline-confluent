from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType

# 1. Define the schema so Spark knows what to look for inside the JSON
json_schema = StructType() \
  .add("user_id", StringType()) \
  .add("event_time", StringType()) \
  .add("event_type", StringType()) \
  .add("item_id", StringType()) \
  .add("price", DoubleType())

# 2. Create the Silver table
@dlt.table(
  name="silver_checkouts",
  comment="Parsed JSON checkout data."
)
def silver_checkouts():
  return (
    dlt.read_stream("bronze_checkouts")
    # Step A: Convert the hex/binary 'value' into readable JSON text
    .withColumn("json_text", col("value").cast("string"))
    
    # Step B: Apply the schema to the JSON text to extract the columns
    .withColumn("parsed_data", from_json(col("json_text"), json_schema))
    
    # Step C: Select only our clean columns and drop the raw Kafka garbage
    .select("parsed_data.*")
  )