import os
import json
import time
import uuid
import logging
from collections import defaultdict, Counter
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

# Configuration (from environment variables)
BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS")
SASL_USERNAME = os.environ.get("SASL_USERNAME")
SASL_PASSWORD = os.environ.get("SASL_PASSWORD")
INPUT_TOPIC = os.environ.get("INPUT_TOPIC")
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC")
DEAD_LETTER_TOPIC = os.environ.get("DEAD_LETTER_TOPIC", "appointment_events_dead_letter")
SUMMARY_INTERVAL = int(os.environ.get("SUMMARY_INTERVAL", "10"))  # seconds
GROUP_ID = os.environ.get("GROUP_ID", "appointment_consumer_group")

# In-memory workload map
doctor_workload = defaultdict(lambda: {"hours": 0, "schedule": []})
cancellations_last_minute = 0
last_summary_time = time.time()
last_cancellation_reset_time = time.time()


def kafka_consumer_config():
    """Returns Kafka consumer configuration."""
    return {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'sasl_mechanism': 'SCRAM-SHA-512',
        'security_protocol': 'SASL_PLAINTEXT',
        'sasl_plain_username': SASL_USERNAME,
        'sasl_plain_password': SASL_PASSWORD,
        'group_id': GROUP_ID,
        'auto_offset_reset': 'earliest',  # or 'latest'
        'enable_auto_commit': True,
        'consumer_timeout_ms': 1000  # 1 second
    }


def kafka_producer_config():
    """Returns Kafka producer configuration."""
    return {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'sasl_mechanism': 'SCRAM-SHA-512',
        'security_protocol': 'SASL_PLAINTEXT',
        'sasl_plain_username': SASL_USERNAME,
        'sasl_plain_password': SASL_PASSWORD,
        'linger_ms': 100  # Reduce latency
    }


def serialize_message(message):
    """Serializes a message to JSON."""
    try:
        serialized = json.dumps(message).encode('utf-8')
        logging.debug("Serialized message successfully")
        return serialized
    except Exception as e:
        logging.exception("Error serializing message")
        return None


def deserialize_message(message_value):
    """Deserializes a message from JSON."""
    try:
        decoded = message_value.decode('utf-8') if isinstance(message_value, (bytes, bytearray)) else message_value
        obj = json.loads(decoded)
        logging.debug("Deserialized message: %s", obj)
        return obj
    except Exception as e:
        logging.exception("Error deserializing message")
        return None


def send_to_dead_letter_queue(message, error):
    """Sends a message to the dead-letter topic."""
    producer_conf = kafka_producer_config()
    producer = KafkaProducer(**producer_conf)
    try:
        if hasattr(message, 'value') and message.value is not None:
            deserialized_message = deserialize_message(message.value)
        else:
            deserialized_message = None

        headers = list(message.headers) if hasattr(message, 'headers') and message.headers is not None else []
        headers.append(('error', str(error).encode('utf-8')))

        payload = {
            "original_message": deserialized_message,
            "error": str(error)
        }

        serialized = serialize_message(payload)
        if serialized is None:
            logging.error("Dead-letter payload serialization failed; dropping message: %s", payload)
            return

        producer.send(
            DEAD_LETTER_TOPIC,
            value=serialized,
            headers=headers
        )
        producer.flush()  # Ensure message is sent
        logging.info("Sent message to dead-letter topic: %s", error)
    except Exception as e:
        logging.exception("Failed to send to dead-letter topic")
    finally:
        producer.close()


def process_appointment_event(event):
    """Processes an appointment event and updates the workload map."""
    global doctor_workload, cancellations_last_minute
    try:
        event_type = event.get('event_type')
    except Exception:
        logging.error("Event is not a dict or missing 'event_type': %s", event)
        return

    if event_type == 'appointment_created':
        doctor_id = event.get('doctor_id')
        scheduled_time_str = event.get('scheduled_time')
        if doctor_id is None or scheduled_time_str is None:
            logging.error("Missing doctor_id or scheduled_time in event: %s", event)
            return

        try:
            scheduled_time = datetime.fromisoformat(scheduled_time_str.replace('Z', '+00:00'))
        except Exception:
            try:
                scheduled_time = datetime.strptime(scheduled_time_str, '%Y-%m-%dT%H:%M:%S.%f')
            except Exception:
                logging.exception("Failed to parse scheduled_time: %s", scheduled_time_str)
                return

        # Assuming each appointment is 1 hour for simplicity
        doctor_workload[doctor_id]["hours"] += 1
        doctor_workload[doctor_id]["schedule"].append(scheduled_time)
        logging.info("Appointment created for doctor %s at %s", doctor_id, scheduled_time)

    elif event_type == 'appointment_cancelled':
        cancellations_last_minute += 1
        appointment_id = event.get('appointment_id')
        cancellation_time = event.get('cancellation_time')
        logging.info("Processing cancellation for appointment_id=%s at %s", appointment_id, cancellation_time)

        if not cancellation_time:
            logging.warning("Cancellation event missing cancellation_time: %s", event)
            return

        # Find and remove the appointment from the doctor's schedule
        removed = False
        for doctor_id, workload in doctor_workload.items():
            for i, scheduled_time in enumerate(workload["schedule"]):
                #  Naive approach.  Consider indexing if scale becomes an issue.
                try:
                    scheduled_time_str = scheduled_time.isoformat()
                except Exception:
                    scheduled_time_str = scheduled_time.strftime('%Y-%m-%dT%H:%M:%S.%f')

                if scheduled_time_str in cancellation_time:
                    doctor_workload[doctor_id]["hours"] -= 1
                    del workload["schedule"][i]
                    logging.info("Appointment cancelled for appointment ID %s (doctor=%s)", appointment_id, doctor_id)
                    removed = True
                    break  # Exit inner loop after finding and removing
            if removed:
                break
        if not removed:
            logging.warning("Could not find appointment to cancel for event: %s", event)
    else:
        logging.warning("Unknown event type: %s", event_type)


def generate_summary():
    """Generates a summary of the doctor workload."""
    global doctor_workload, cancellations_last_minute

    # Top 3 busiest doctors
    sorted_doctors = sorted(doctor_workload.items(), key=lambda item: item[1]["hours"], reverse=True)[:3]
    top_doctors = [{"doctor_id": doctor_id, "hours": workload["hours"]} for doctor_id, workload in sorted_doctors]

    # Idle doctors
    idle_doctors = [doctor_id for doctor_id, workload in doctor_workload.items() if workload["hours"] == 0]

    summary = {
        "top_doctors": top_doctors,
        "idle_doctors": idle_doctors,
        "total_cancellations_last_minute": cancellations_last_minute
    }

    return summary


def produce_summary(summary):
    """Produces the summary to the output topic."""
    producer_conf = kafka_producer_config()
    producer = KafkaProducer(**producer_conf)

    try:
        producer.send(
            OUTPUT_TOPIC,
            value=serialize_message(summary)
        )
        producer.flush()
        print(f"Summary sent to topic {OUTPUT_TOPIC}: {summary}")
    except Exception as e:
        print(f"Failed to produce summary: {e}")
    finally:
        producer.close()



def consume_appointments():
    """Consumes appointment events from Kafka."""
    global cancellations_last_minute, last_summary_time, last_cancellation_reset_time

    consumer_conf = kafka_consumer_config()
    consumer = KafkaConsumer(**consumer_conf, value_deserializer=deserialize_message)
    consumer.subscribe([INPUT_TOPIC])
    print("Started consuming appointment events...")

    try:
        for message in consumer:
            try:
                print("Processing new message...")
                event = message.value
                if event:
                    print(f"Received message: {event}")
                    process_appointment_event(event)
            except Exception as e:
                print(f"Error processing message: {e}")
                send_to_dead_letter_queue(message, e)
                continue

            # Generate and send summary every SUMMARY_INTERVAL seconds
            if time.time() - last_summary_time >= SUMMARY_INTERVAL:
                print("Generating summary...")
                summary = generate_summary()
                produce_summary(summary)
                last_summary_time = time.time()

                # Reset cancellation count every minute
                if time.time() - last_cancellation_reset_time >= 60:
                    print("Resetting cancellation count...")
                    cancellations_last_minute = 0
                    last_cancellation_reset_time = time.time()

    except KeyboardInterrupt:
        print("Shutting down consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    print("aijdojasiodjoaijsdoiajoisdjoiajsdojaosjdoiajsdoijao")
    consume_appointments()
