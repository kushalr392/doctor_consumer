import json
import os
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Configuration from environment variables
BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
SASL_USERNAME = os.environ.get("SASL_USERNAME", "user")
SASL_PASSWORD = os.environ.get("SASL_PASSWORD", "password")
INPUT_TOPIC = os.environ.get("INPUT_TOPIC", "appointment_events")
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "doctor_summary")
DEAD_LETTER_TOPIC = os.environ.get("DEAD_LETTER_TOPIC", "appointment_events_dead_letter")
GROUP_ID = os.environ.get("GROUP_ID", "appointment_processor")
SUMMARY_INTERVAL_SECONDS = int(os.environ.get("SUMMARY_INTERVAL_SECONDS", "10"))

def kafka_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=SASL_USERNAME,
        sasl_plain_password=SASL_PASSWORD,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(0, 11, 5)  # Specify API version
    )


def kafka_consumer():
    return KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=SASL_USERNAME,
        sasl_plain_password=SASL_PASSWORD,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        api_version=(0, 11, 5)  # Specify API version
    )


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10),
       retry=retry_if_exception_type(KafkaError))
def safe_send(producer, topic, message):
    try:
        producer.send(topic, message).get(timeout=10)  # Get blocks until completion or timeout
    except KafkaError as e:
        print(f"Error sending to Kafka: {e}")
        raise


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10),
       retry=retry_if_exception_type(KafkaError))
def safe_consume(consumer, process_message):
    try:
        for message in consumer:
            process_message(message.value)
    except KafkaError as e:
        print(f"Error consuming from Kafka: {e}")
        raise


def main():
    workload_map = defaultdict(lambda: {"hours_booked": 0, "schedule": []})
    cancellations_last_minute = 0
    last_summary_time = time.time()
    cancellations_reset_time = time.time()  # Time when cancellations were last reset
    recent_cancellations = []  # List to hold timestamps of recent cancellations

    producer = kafka_producer()
    consumer = kafka_consumer()

    def process_appointment_event(event):
        nonlocal workload_map, cancellations_last_minute, recent_cancellations

        try:
            event_type = event.get("event_type")
            doctor_id = event.get("doctor_id")
            start_time = event.get("scheduled_time")
            event_id = event.get("appointment_id", str(uuid.uuid4()))  # Generate event_id if missing

            if not all([event_type, doctor_id, start_time]):
                print(f"Invalid event: {event}.  Missing required fields.")
                if not event_type:
                    print(f"Missing event_type fields.")
                if not doctor_id:
                    print(f"Missing doctor_id fields.")
                if not start_time:
                    print(f"Missing scheduled_time fields.")

                safe_send(producer, DEAD_LETTER_TOPIC, {"error": "Missing required fields", "event": event})
                return

            start_datetime = datetime.fromisoformat(start_time.replace('Z', '+00:00'))

            if event_type == "appointment_created":
                workload_map[doctor_id]["schedule"].append({"start_time": start_time, "event_id": event_id})

            elif event_type == "appointment_cancelled":
                # Cancellation tracking
                cancellation_time = datetime.utcnow()
                recent_cancellations.append(cancellation_time)

                # Remove from schedule
                original_length = len(workload_map[doctor_id]["schedule"])
                workload_map[doctor_id]["schedule"] = [
                    slot for slot in workload_map[doctor_id]["schedule"]
                    if slot.get("event_id") != event_id  # Match on event_id
                ]

                removed_count = original_length - len(workload_map[doctor_id]["schedule"])
                if removed_count == 0:
                    print(f"Warning: Cancellation {event_id} not found for doctor {doctor_id}")

                # workload_map[doctor_id]["hours_booked"] = sum(
                #     (datetime.fromisoformat(s["end_time"].replace('Z', '+00:00')) - datetime.fromisoformat(s["start_time"].replace('Z', '+00:00'))).total_seconds() / 3600
                #     for s in workload_map[doctor_id]["schedule"]
                # )

            else:
                print(f"Unknown event type: {event_type}")
                safe_send(producer, DEAD_LETTER_TOPIC, {"error": "Unknown event type", "event": event})

        except Exception as e:
            print(f"Error processing event: {e}. Event: {event}")
            safe_send(producer, DEAD_LETTER_TOPIC, {"error": str(e), "event": event})

    def generate_summary():
        nonlocal workload_map, last_summary_time, cancellations_last_minute, cancellations_reset_time, recent_cancellations

        # Calculate top 3 busiest doctors
        busiest_doctors = sorted(workload_map.items(), key=lambda item: item[1]["hours_booked"], reverse=True)[:3]
        busiest_doctors_summary = [{"doctor_id": doc_id, "hours_booked": doc_data["hours_booked"]} for doc_id, doc_data in busiest_doctors]

        # Find idle doctors
        idle_doctors = [doc_id for doc_id, doc_data in workload_map.items() if doc_data["hours_booked"] == 0]

        # Calculate cancellations in the last minute
        current_time = datetime.utcnow()
        one_minute_ago = current_time - timedelta(minutes=1)
        cancellations_last_minute = sum(1 for cancel_time in recent_cancellations if cancel_time >= one_minute_ago)

        # Remove older cancellation records
        recent_cancellations = [cancel_time for cancel_time in recent_cancellations if cancel_time >= one_minute_ago]

        summary = {
            "top_3_busiest_doctors": busiest_doctors_summary,
            "idle_doctors": idle_doctors,
            "total_cancellations_last_minute": cancellations_last_minute,
            "timestamp": datetime.utcnow().isoformat()
        }

        safe_send(producer, OUTPUT_TOPIC, summary)
        print(f"Summary sent: {summary}")
        last_summary_time = time.time()

    try:
        while True:
            safe_consume(consumer, process_appointment_event)  # Consume and process messages

            if time.time() - last_summary_time >= SUMMARY_INTERVAL_SECONDS:
                generate_summary()

    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        producer.close()
        consumer.close()


if __name__ == "__main__":
    main()
