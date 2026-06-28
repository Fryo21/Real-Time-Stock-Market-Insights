import json
from kafka import KafkaConsumer

"""
Verification script — confirms messages arrive on the stock_analysis topic.


Run locally (outside Docker) while Kafka is up:

    python src/Consumer/kafka_consumer.py


For the production pipeline (Kafka -> PostgreSQL), use consumer.py via Docker Compose.

"""


TOPIC = "stock_analysis"

BOOTSTRAP_SERVERS = ["localhost:9094"]



def main():

    consumer = KafkaConsumer(

        TOPIC,

        bootstrap_servers=BOOTSTRAP_SERVERS,

        auto_offset_reset="earliest",

        enable_auto_commit=True,

        group_id="verification-consumer-group",

        value_deserializer=lambda x: json.loads(x.decode("utf-8")),

    )

    print(f"Listening on '{TOPIC}' at {BOOTSTRAP_SERVERS[0]} (Ctrl+C to stop)...")

    try:

        for message in consumer:
            print(f"  Received: {message.value}")

    except KeyboardInterrupt:
        print("\nStopped.")

    finally:
        consumer.close()



if __name__ == "__main__":

    main()


