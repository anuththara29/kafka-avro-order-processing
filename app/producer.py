import json, random, time, uuid, pathlib
from confluent_kafka.avro import AvroProducer
from confluent_kafka import avro
from config import BROKER_URL, SCHEMA_REGISTRY_URL, TOPIC_ORDERS

SCHEMA_PATH = str(pathlib.Path(__file__).parent.parent / "schemas" / "order.avsc")

def load_schema():
    with open(SCHEMA_PATH, "r") as f:
        schema_dict = json.load(f)
    value_schema = avro.loads(json.dumps(schema_dict))
    key_schema = avro.loads(json.dumps({"type": "string"}))
    return key_schema, value_schema

def make_order():
    """Generate VALID order"""
    return {
        "orderId": str(uuid.uuid4()),
        "product": random.choice(["Item1", "Item2", "Item3", "Item4"]),
        "price": round(random.uniform(10, 200), 2)
    }

def make_bad_order():
    """Generate INVALID order for DLQ"""
    return {
        "orderId": str(uuid.uuid4()),
        "product": "BAD_ITEM",
        "price": -50.0        # negative price → permanent error in consumer
    }

def main(n=10):
    key_schema, value_schema = load_schema()
    producer = AvroProducer(
        {"bootstrap.servers": BROKER_URL, "schema.registry.url": SCHEMA_REGISTRY_URL},
        default_key_schema=key_schema,
        default_value_schema=value_schema,
    )

    for _ in range(n):
        # 20% probability create a BAD message
        if random.random() < 0.2:
            order = make_bad_order()
            print("Produced BAD ORDER (will go to DLQ):", order)
        else:
            order = make_order()
            print("Produced:", order)

        producer.produce(topic=TOPIC_ORDERS, key=order["orderId"], value=order)
        time.sleep(0.5)

    producer.flush()

if __name__ == "__main__":
    main()
