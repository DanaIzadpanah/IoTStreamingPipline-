from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
env.add_source(KafkaSource("iot-sensor-data")) \
   .filter(lambda x: x["temperature"] > 35) \
   .sink_to(KafkaSink("iot-alerts"))
   
env.execute("IoT Anomaly Detection")