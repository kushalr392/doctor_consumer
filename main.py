import os
import json
import time
import uuid
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
        return json.dumps(message).encode('utf-8')
    except Exception as e:
        print(f"Error serializing message: {e}")
        return None


def deserialize_message(message_value):
    """Deserializes a message from JSON."""
    try:
        return json.loads(message_value.decode('utf-8'))
    except Exception as e:
        print(f"Error deserializing message: {e}")
        return None


def send_to_dead_letter_queue(message, error):
    """Sends a message to the dead-letter topic."""
    producer_conf = kafka_producer_config()
    producer = KafkaProducer(**producer_conf)
    try:
        if message.value is not None:
          deserialized_message = deserialize_message(message.value)
        else:
          deserialized_message = None
        headers = message.headers if hasattr(message, 'headers') else []
        headers.append(('error', str(error).encode('utf-8')))


        producer.send(
            DEAD_LETTER_TOPIC,
            value=serialize_message({
                "original_message": deserialized_message,
                "error": str(error)
            }),
            headers=headers
        )
        producer.flush()  # Ensure message is sent
        print(f"Sent message to dead-letter topic: {error}")
    except Exception as e:
        print(f"Failed to send to dead-letter topic: {e}")
    finally:
        producer.close()


def process_appointment_event(event):
    """Processes an appointment event and updates the workload map."""
    global doctor_workload, cancellations_last_minute

    if event['event_type'] == 'appointment_created':
        doctor_id = event['doctor_id']
        scheduled_time_str = event['scheduled_time']
        try:
            scheduled_time = datetime.fromisoformat(scheduled_time_str.replace('Z', '+00:00'))
        except ValueError:
            scheduled_time = datetime.strptime(scheduled_time_str, '%Y-%m-%dT%H:%M:%S.%f')

        # Assuming each appointment is 1 hour for simplicity
        doctor_workload[doctor_id]["hours"] += 1
        doctor_workload[doctor_id]["schedule"].append(scheduled_time)
        print(f"Appointment created for doctor {doctor_id} at {scheduled_time}")

    elif event['event_type'] == 'appointment_cancelled':
        cancellations_last_minute += 1
        appointment_id = event['appointment_id']
        # Find and remove the appointment from the doctor's schedule
        for doctor_id, workload in doctor_workload.items():
            for i, scheduled_time in enumerate(workload["schedule"]):
                #  Naive approach.  Consider indexing if scale becomes an issue.
                #  Assuming appointment_id is not stored, but can be improved.
                try:
                    scheduled_time_str = scheduled_time.isoformat()
                except AttributeError:  # Handle timezone-naive datetimes
                    scheduled_time_str = scheduled_time.strftime('%Y-%m-%dT%H:%M:%S.%f')

                if scheduled_time_str in event['cancellation_time']:  # Likely match
                    doctor_workload[doctor_id]["hours"] -= 1
                    del workload["schedule"][i]
                    print(f"Appointment cancelled for appointment ID {appointment_id}")
                    break  # Exit inner loop after finding and removing
            else:
                continue  # Only executed if the inner loop did NOT break
            break  # Exit outer loop if inner loop broke
    else:
        print(f"Unknown event type: {event['event_type']}")


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

    try:
        for message in consumer:
            try:
                event = message.value
                if event:
                    process_appointment_event(event)
            except Exception as e:
                print(f"Error processing message: {e}")
                send_to_dead_letter_queue(message, e)
                continue

            # Generate and send summary every SUMMARY_INTERVAL seconds
            if time.time() - last_summary_time >= SUMMARY_INTERVAL:
                summary = generate_summary()
                produce_summary(summary)
                last_summary_time = time.time()

                # Reset cancellation count every minute
                if time.time() - last_cancellation_reset_time >= 60:
                    cancellations_last_minute = 0
                    last_cancellation_reset_time = time.time()

    except KeyboardInterrupt:
        print("Shutting down consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    consume_appointments()
