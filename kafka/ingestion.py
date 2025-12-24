import json
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
import requests
from sseclient import SSEClient

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC_NAME = os.getenv("TOPIC_NAME")
WIKI_STREAM_URL = os.getenv("WIKI_STREAM_URL")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5
)


def publish_wikipedia_events():
    print("Connecting to Wikipedia Recent Changes stream...")
    count = 0
    headers = {
    "Accept": "text/event-stream",
    "User-Agent": "KafkaStreamingProject/1.0 (contact: your_email@example.com)"
    }

    response = requests.get(WIKI_STREAM_URL, stream=True,headers=headers)
    client = SSEClient(response)

    for event in client.events():
        if event.event == "message":
            try:
                data = json.loads(event.data)

                # Use unique event ID as Kafka key
                event_id = data.get("meta", {}).get("id", "unknown")

                producer.send(
                    topic=TOPIC_NAME,
                    key=event_id,
                    value=data
                )
                if count >= 20:
                    print("Published 20 events, stopping producer.")
                    break

            except json.JSONDecodeError:
                print("Received invalid JSON, skipping event")
            except Exception as e:
                print(f"Kafka publish error: {e}")

if __name__ == "__main__":
    try:
        publish_wikipedia_events()
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()
        producer.close()