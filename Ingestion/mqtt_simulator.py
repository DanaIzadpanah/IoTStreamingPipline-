import paho.mqtt.client as mqtt  # Import MQTT client library
import random  # Import random for sensor values
import time  # Import time for delays

# Define the MQTT broker (Alternative free brokers if needed)
broker = "mqtt.eclipseprojects.io"  
# broker = "broker.hivemq.com"  # Alternative broker
# broker = "test.mosquitto.org"  # Alternative broker

# How did I fix the broker ? 
# installed paho-mqtt package 
# 

# Create an MQTT client instance with a unique client ID
client = mqtt.Client(client_id="IoT_Simulator")

# Callback function to handle connection success or failure
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Connected to MQTT Broker!")
    else:
        print(f"⚠️ Connection failed with error code {rc}")

# Attach the connection callback function
client.on_connect = on_connect

# Try connecting to the broker
try:
    client.connect(broker, port=1883, keepalive=60)  # Default MQTT port is 1883
except Exception as e:
    print(f"❌ Failed to connect to MQTT Broker: {e}")
    exit()

# Start the network loop to maintain the connection
client.loop_start()

# Infinite loop to simulate continuous sensor data publishing
while True:
    # Generate random sensor values
    temperature = round(random.uniform(20, 40), 2)
    humidity = round(random.uniform(30, 80), 2)

    # Create a JSON payload
    payload = f'{{"temperature": {temperature}, "humidity": {humidity}}}'

    # Publish to MQTT topic
    result = client.publish("iot/sensors", payload)

    # Check if the message was successfully published
    status = result.rc
    if status == 0:
        print(f"✅ Published: {payload}")
    else:
        print(f"⚠️ Failed to publish message: {status}")

    # Wait 2 seconds before sending the next data
    time.sleep(2)