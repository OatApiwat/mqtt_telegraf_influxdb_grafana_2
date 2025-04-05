import paho.mqtt.client as mqtt
import json
import time
import random

MQTT_BROKER = "mosquitto"
MQTT_PORT = 1883
MQTT_TOPIC = "iot_sensors/got/mc_02" #iot_sensors/sensor_type(measurement)/machine_name
count = 0
def generate_data():
    global count
    count = count+1
    return {
        "data_id": count,
        "process": "assambly",
        "value_1": random.random(),
        "value_2": random.random(),
        "value_3": random.random(),
        "value_4": random.random(),
        "value_5": random.random(),
        "value_6": random.random(),
        "value_7": random.random(),
        "value_8": random.random()
    }

def main():
    while True:
        data = generate_data()
        client.publish(MQTT_TOPIC, json.dumps(data))
        print(f"Published: {data}")
        time.sleep(1)

if __name__ == "__main__":
    client = mqtt.Client()
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    main()
