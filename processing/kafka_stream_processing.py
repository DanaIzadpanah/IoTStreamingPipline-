'''
What is the purpose of this script?

Reads incoming sensor data from kafka
filters out high tempreture data
Sends alerts if dangersous conditions are detected
'''


from pyflink.datastream import StreamExecutionEnvironment


# Create a streaming execution environment
env = StreamExecutionEnvironment.get_execution_environment()

# Ingest data from a Kafka source (assumes a topic named "iot-sensor-data")
# Filter out sensor readings where the temperature exceeds 35 degrees
# Send the filtered anomaly readings to a different Kafka topic ("iot-alerts")
env.add_source(KafkaSource("iot-sensor-data")) \
   .filter(lambda x: x["temperature"] > 35) \
   .sink_to(KafkaSink("iot-alerts"))
   
# Execute the streaming pipeline with a job name
env.execute("IoT Anomaly Detection")