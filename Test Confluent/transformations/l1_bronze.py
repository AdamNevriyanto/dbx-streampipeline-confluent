import dlt
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

# Authentication for Confluent Cloud
kafka_options = {
  "kafka.bootstrap.servers": "pkc-oz2po.ap-southeast-3.aws.confluent.cloud:9092",
  "kafka.security.protocol": "SASL_SSL",
  "kafka.sasl.mechanism": "PLAIN",
  "kafka.sasl.jaas.config": "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='5VKYMR4MQY6UAGCP' password='cfltd4oABC/hyYb62fZ4bqb4Y4iKg4ZAtMNyfwlXsAH2oTUGnv+9d8qKWaPsTnRQ';",
  "startingOffsets": "earliest",
  "subscribe": "checkouts_only" 
}

@dlt.table(
  name="bronze_checkouts",
  comment="Raw Kafka payloads from Flink."
)
def bronze_checkouts():
  return spark.readStream.format("kafka").options(**kafka_options).load()