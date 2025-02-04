import asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

# Kafka settings
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "iot-sensor-data"
OUTPUT_TOPIC = "iot-alerts"

async def process_messages():
    consumer = AIOKafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest"
    )
    
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    # Start consumer and producer
    await consumer.start()
    await producer.start()

    try:
        async for msg in consumer:
            message = msg.value.decode("utf-8")
            print(f"Received: {message}")

            # Extract temperature and filter high temperatures
            temp_data = eval(message)  # Convert JSON string to dictionary
            if temp_data["temperature"] > 35:
                alert_msg = f"🔥 ALERT: High Temperature Detected! {temp_data}"
                await producer.send_and_wait(OUTPUT_TOPIC, alert_msg.encode("utf-8"))
                print(f"Sent Alert: {alert_msg}")

    finally:
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(process_messages())