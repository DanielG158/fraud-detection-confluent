"""
Real-Time Fraud Detection - Alert Consumer
Reads fraud alerts from Kafka and displays them in real-time
"""

import os
import time
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
KAFKA_API_KEY = os.getenv("KAFKA_API_KEY")
KAFKA_API_SECRET = os.getenv("KAFKA_API_SECRET")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
SR_API_KEY = os.getenv("SCHEMA_REGISTRY_API_KEY")
SR_API_SECRET = os.getenv("SCHEMA_REGISTRY_API_SECRET")
TOPIC = os.getenv("TOPIC_FRAUD_ALERTS", "fraud-alerts")

def run():
    sr_client = SchemaRegistryClient({
        "url": SCHEMA_REGISTRY_URL,
        "basic.auth.user.info": f"{SR_API_KEY}:{SR_API_SECRET}",
    })
    avro_deserializer = AvroDeserializer(sr_client)
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": KAFKA_API_KEY,
        "sasl.password": KAFKA_API_SECRET,
        "group.id": "fraud-alert-monitor",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])
    print(f"\n Monitoring fraud alerts on: {TOPIC}")
    print("=" * 60)
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Error: {msg.error()}")
                break
            alert = avro_deserializer(msg.value(), SerializationContext(TOPIC, MessageField.VALUE))
            print(f"\n FRAUD ALERT: {alert.get('fraud_reason')} | User: {alert.get('user_id')} | Amount: ${alert.get('amount', 0):.2f}")
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        consumer.close()

if __name__ == "__main__":
    run()
