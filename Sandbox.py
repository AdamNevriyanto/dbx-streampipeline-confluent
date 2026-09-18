# Databricks notebook source
import dlt
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

# Authentication for Confluent Cloud
kafka_options = {
  "kafka.bootstrap.servers": "<your-aws-bootstrap-server>:9092",
  "kafka.security.protocol": "SASL_SSL",
  "kafka.sasl.mechanism": "PLAIN",
  "kafka.sasl.jaas.config": "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='<API_KEY>' password='<API_SECRET>';",
  "startingOffsets": "earliest",
  "subscribe": "checkouts_only" 
}

@dlt.table(
  name="bronze_checkouts",
  comment="Raw Kafka payloads from Flink."
)
def bronze_checkouts():
  return spark.readStream.format("kafka").options(**kafka_options).load()

# COMMAND ----------

# DBTITLE 1,Install kafka-python
# MAGIC %pip install kafka-python

# COMMAND ----------

from kafka import KafkaProducer

# 1. Connect to bootstrap server
producer = KafkaProducer(
    bootstrap_servers=['cdp-pvc1.metrodataanalytics.co.id:30874'],
    api_version=(2, 0, 0),
    request_timeout_ms=5000
)

# 2. Force Kafka to fetch cluster metadata
producer.partitions_for('__test__')

# 3. Print discovered brokers
print("\n--- Kafka Cluster Discovery ---")
brokers = producer._cluster.brokers()

if brokers:
    for b in brokers:
        print(f"Broker ID: {b.nodeId} -> Host: {b.host} | Port: {b.port}")
else:
    print("No brokers returned in metadata.")

producer.close()
