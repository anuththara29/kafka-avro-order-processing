import json, random, time
from confluent_kafka.avro import AvroConsumer, CachedSchemaRegistryClient
from confluent_kafka import Producer
from config import BROKER_URL, SCHEMA_REGISTRY_URL, TOPIC_ORDERS, TOPIC_AVG, TOPIC_DLQ

def maybe_transient_error():
    if random.random() < 0.1:
        raise RuntimeError("Simulated temporary failure")

def is_permanent_error(msg):
    return msg.get("price", 1) < 0

def backoff(attempt):
    delay = min(0.2 * (2 ** attempt), 5)
    time.sleep(delay)

def main():
    registry = CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    consumer = AvroConsumer({
        "bootstrap.servers": BROKER_URL,
        "group.id": "avg-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    }, schema_registry=registry)

    consumer.subscribe([TOPIC_ORDERS])
    producer = Producer({"bootstrap.servers": BROKER_URL})

    count = 0
    total = 0

    print("Consumer started")

    while True:
        msg = consumer.poll(1)
        if msg is None:
            continue
        if msg.error():
            print("Error:", msg.error())
            continue

        key = msg.key()
        value = msg.value()

        if is_permanent_error(value):
            producer.produce(TOPIC_DLQ, key=str(key), value=json.dumps(value))
            producer.flush()
            consumer.commit(msg)
            continue

        attempts = 0
        while True:
            try:
                maybe_transient_error()

                price = float(value["price"])
                total += price
                count += 1
                avg = total / count

                payload = json.dumps({"count": count, "avg": avg})
                producer.produce(TOPIC_AVG, key="avg", value=payload)
                producer.flush()
                print("Processed:", value, "AVG:", avg)

                consumer.commit(msg)
                break

            except RuntimeError:
                attempts += 1
                if attempts <= 5:
                    backoff(attempts)
                else:
                    producer.produce(TOPIC_DLQ, key=str(key), value=json.dumps(value))
                    producer.flush()
                    consumer.commit(msg)
                    break

if __name__ == "__main__":
    main()
