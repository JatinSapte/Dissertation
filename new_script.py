#!/usr/bin/python

# Importing libraries
import paho.mqtt.client as paho
import ssl
from time import sleep
import datetime
import random

# MQTT connection flag
connflag = False

# Callback when connected to MQTT broker
def on_connect(client, userdata, flags, rc):
    global connflag
    if rc == 0:
        print("Connected to AWS IoT Core")
        connflag = True
    else:
        print("Connection failed with code:", rc)

# Callback when a message is received (not used here, just logging)
def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.payload))

# Setup MQTT client
mqttc = paho.Client()
mqttc.on_connect = on_connect
mqttc.on_message = on_message

# AWS IoT connection details
awshost = "arvy56vj95qxz-ats.iot.us-east-1.amazonaws.com"
awsport = 8883
clientId = "iot-thing-aws"
thingName = "iot-thing-aws"

# Certificate and key paths
caPath = "certificates/AmazonRootCA1.pem"
certPath = "certificates/1e7f73e6bd9643a71a62647ed05b60cea7a07085a21cd74c4b3b074f14fd0aa6-certificate.pem.crt"
keyPath = "certificates/1e7f73e6bd9643a71a62647ed05b60cea7a07085a21cd74c4b3b074f14fd0aa6-private.pem.key"

mqttc.tls_set(caPath, certfile=certPath, keyfile=keyPath, cert_reqs=ssl.CERT_REQUIRED,
              tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)

# Connect and start loop
mqttc.connect(awshost, awsport, keepalive=60)
mqttc.loop_start()

# Simulated device IDs
device_ids = ["raspberry-pi-sim-001", "raspberry-pi-sim-002", "raspberry-pi-sim-003"]

# Data publishing loop
while True:
    sleep(5)
    if connflag:
        # Randomly select a device ID
        device_id = random.choice(device_ids)
        # Generate heart rate value
        heart_rate = random.randint(100,120)
        # Timestamp in ISO 8601 format
        timestamp = datetime.datetime.utcnow().isoformat() + "Z"

        # Construct message payload
        payload = {
            "device_id": device_id,
            "timestamp": timestamp,
            "sensor_type": "heart_rate",
            "value": heart_rate
        }

        # Publish message
        mqttc.publish("heart-rate-topic", str(payload).replace("'", '"'), 1)
        print("Message sent:", payload)
    else:
        print("Waiting for connection...")
