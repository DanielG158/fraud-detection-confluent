import os, uuid, time, random
from dotenv import load_dotenv
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from faker import Faker

load_dotenv()
fake = Faker()

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
KAFKA_API_KEY = os.getenv("KAFKA_API_KEY")
KAFKA_API_SECRET = os.getenv("KAFKA_API_SECRET")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
SR_API_KEY = os.getenv("SCHEMA_REGISTRY_API_KEY")
SR_API_SECRET = os.getenv("SCHEMA_REGISTRY_API_SECRET")
TOPIC = os.getenv("TOPIC_RAW_TRANSACTIONS", "raw-transactions")

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "../schemas/transaction.avsc")
with open(SCHEMA_PATH) as f:
    SCHEMA_STR = f.read()

CATEGORIES = ["GROCERY","ELECTRONICS","RESTAURANT","FUEL","TRAVEL","ONLINE_RETAIL","ATM","PHARMACY"]
COUNTRIES = ["ID","SG","MY","US","GB","AU","JP","CN"]
CITIES = {
    "ID": ["Jakarta","Surabaya","Bandung"],
    "SG": ["Singapore"],
    "MY": ["Kuala Lumpur","Penang"],
    "US": ["New York","Los Angeles"],
    "GB": ["London","Manchester"],
    "AU": ["Sydney","Melbourne"],
    "JP": ["Tokyo","Osaka"],
    "CN": ["Shanghai","Beijing"],
}
USER_PROFILES = {
    f"user_{i:03d}": {
        "avg_amount": random.uniform(20, 200),
        "home_country": random.choice(["ID","SG","MY"]),
    }
    for i in range(1, 21)
}

def make_tx(user_id, fraud_type=None):
    p = USER_PROFILES[user_id]
    country = p["home_country"]
    amount = round(random.gauss(p["avg_amount"], p["avg_amount"]*0.3), 2)
    amount = max(1.0, amount)
    city = random.choice(CITIES[country])
    is_online = random.random() < 0.3

    tx = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "amount": amount,
        "currency": "USD",
        "merchant": fake.company(),
        "merchant_category": random.choice(CATEGORIES),
        "location_city": city,
        "location_country": country,
        "timestamp_ms": int(time.time() * 1000),
        "card_last4": str(random.randint(1000,9999)),
        "is_online": is_online,
        "device_fingerprint": str(uuid.uuid4()) if random.random() < 0.5 else None,
    }

    if fraud_type == "AMOUNT":
        tx["amount"] = round(p["avg_amount"] * random.uniform(15, 40), 2)
        tx["merchant_category"] = "ELECTRONICS"
        print(f"  FRAUD [AMOUNT_ANOMALY] user={user_id} amount=${tx['amount']:.2f}")
    elif fraud_type == "GEO":
        fc = random.choice(["US","GB","AU","JP"])
        tx["location_country"] = fc
        tx["location_city"] = random.choice(CITIES[fc])
        tx["is_online"] = True
        tx["amount"] = round(random.uniform(300, 800), 2)
        print(f"  FRAUD [GEO_IMPOSSIBLE] user={user_id} loc={tx['location_city']},{fc}")
    elif fraud_type == "VELOCITY":
        tx["is_online"] = True
        tx["merchant_category"] = "ONLINE_RETAIL"
        print(f"  FRAUD [VELOCITY] user={user_id} tx={tx['transaction_id'][:8]}")

    return tx

def delivery_report(err, msg):
    if err:
        print(f"FAIL: {err}")
    else:
        print(f"OK -> partition={msg.partition()} offset={msg.offset()}")

def run():
    sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL, "basic.auth.user.info": f"{SR_API_KEY}:{SR_API_SECRET}"})
    avro_ser = AvroSerializer(sr, SCHEMA_STR)
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": KAFKA_API_KEY,
        "sasl.password": KAFKA_API_SECRET,
    })

    users = list(USER_PROFILES.keys())
    print(f"\nProducing to topic: {TOPIC}\n")

    for _ in range(30):
        tx = make_tx(random.choice(users))
        producer.produce(topic=TOPIC, key=tx["user_id"],
            value=avro_ser(tx, SerializationContext(TOPIC, MessageField.VALUE)),
            on_delivery=delivery_report)
        producer.poll(0)
        time.sleep(0.1)

    fraud_user = random.choice(users)
    for ftype in ["AMOUNT", "GEO"]:
        tx = make_tx(fraud_user, ftype)
        producer.produce(topic=TOPIC, key=tx["user_id"],
            value=avro_ser(tx, SerializationContext(TOPIC, MessageField.VALUE)),
            on_delivery=delivery_report)
        producer.poll(0)

    for _ in range(8):
        tx = make_tx(fraud_user, "VELOCITY")
        producer.produce(topic=TOPIC, key=tx["user_id"],
            value=avro_ser(tx, SerializationContext(TOPIC, MessageField.VALUE)),
            on_delivery=delivery_report)
        producer.poll(0)
        time.sleep(0.05)

    producer.flush()
    print("\nDone!")

if __name__ == "__main__":
    run()
