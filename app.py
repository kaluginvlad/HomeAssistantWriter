import json
from influxdb_client import InfluxDBClient
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from paho.mqtt import client as mqtt_client
from device import Device
import logging
import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("writer.log"),
        logging.StreamHandler()
    ]
)

config = json.loads(open("config.json").read())

# Configure InfluxDB
client = InfluxDBClient(config['InfluxDB']['url'], config['InfluxDB']['token'])
write_api = client.write_api(write_options=SYNCHRONOUS)

devices = {}

logging.info("Started")

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("MQTT connect ok!")
    else:
        logging.info("MQTT connect fail")


def on_message(client, userdata, msg):
    # Config messages
    if msg.topic[-7:] == "/config":
        device = Device(msg.payload.decode("utf-8"))
        client.subscribe(device.state_topic)
        devices[device.filter_topic] = device
        logging.info(f"Found device: {device.name}")
    # Data messages
    else:
        # Get device class by topic
        device = devices["".join(msg.topic.split("/")[2:])]
        data = device.get_data(msg.payload.decode("UTF-8"))
        logging.info(f"Data from {device.name}: {data}")
        # Write data to InfluxDB
        point = Point(device.name) \
          .tag("DeviceID", device.id) \
          .tag("DeviceModel", device.model) \
          .tag("DeviceManufacturer", device.manufacturer) \
          .tag("DeviceClass", device.dev_class) \
          .field(device.dev_class, int(data)) \
          .time(datetime.datetime.utcnow(), WritePrecision.NS)

        write_api.write(config['InfluxDB']['bucket'], config['InfluxDB']['org'], point)

# Init MQTT connection
client = mqtt_client.Client()
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(config['mqtt']['user'], config['mqtt']['pass'])
client.connect(config['mqtt']['host'], config['mqtt']['port'])

# Configuration topic
client.subscribe("homeassistant/sensor/+/config")
client.subscribe("homeassistant/binary_sensor/+/config")

client.loop_forever()