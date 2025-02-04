'''
Why this worked ? 

Kafka kept failing, due to broken kafka and python environemnt 
conflict I usedd alokafka, which fixed those things


What is happening here ? 

The script listens for MQTT messages 
Transfers from aiokafka(a high speed data transfering platform)
'''
import asyncio
from aiokafka import AIOKafkaProducer
import paho.mqtt.client as mqtt

# Asynchronous function to send messages to Kafka
async def send_to_kafka(message):
    # Initialize the Kafka producer with the specified broker address
    producer = AIOKafkaProducer(bootstrap_servers="localhost:9092")
    await producer.start()  # Start the producer
    try:
        # Send the message to the 'iot-sensor-data' topic in Kafka
        await producer.send_and_wait("iot-sensor-data", message.encode('utf-8'))
    finally:
        await producer.stop()  # Ensure the producer is properly closed

# Callback function triggered upon receiving an MQTT message
def on_message(client, userdata, msg):
    # Run the asynchronous Kafka sending function within the event loop
    asyncio.run(send_to_kafka(msg.payload.decode('utf-8')))
    # Log the successful forwarding of the message
    print(f"✅ Sent to Kafka: {msg.payload.decode('utf-8')}")

# Set up the MQTT client
client = mqtt.Client()
client.connect("mqtt.eclipseprojects.io")  # Connect to the MQTT broker
client.subscribe("iot/sensors")  # Subscribe to the 'iot/sensors' topic
client.on_message = on_message  # Assign the message handling callback
client.loop_forever()  # Start the network loop to process MQTT messages
