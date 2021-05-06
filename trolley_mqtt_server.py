import paho.mqtt.client as mqtt
import pymysql as mysql
import json
from datetime import datetime

MAC_ADDRESS_STRING = 'M'
BEACON_STRING = 'B'

mqtt_username = 'mqttuser'
mqtt_password = 'mqttpassword'
mqtt_host = 'localhost'
mqtt_port = 1883
mqtt_topic = 'trolley'

mysql_host = 'localhost'
mysql_port = 3306
mysql_database = 'trolley'
mysql_user = 'trolley'
mysql_password = 'password'
mysql_table = 'trolley'
mysql_cursor = None
mysql_connection = None
mysql_mac_column = 'mac_address'
mysql_beacon_column = 'beacons'

def mysql_start():
    global mysql_cursor, mysql_connection
    mysql_connection = mysql.connect(host=mysql_host,
                port=mysql_port,
                database=mysql_database,
                user=mysql_user,
                password=mysql_password,
                charset='utf8')

    mysql_cursor = mysql_connection.cursor()

def mysql_write(mac, beacons):
    global mysql_cursor
    count = mysql_cursor.execute("INSERT INTO " + mysql_table + " (" + mysql_mac_column + ", " + mysql_beacon_column + ") VALUES ('" + mac + "','" + beacons + "')")
    print('Inserted ' + str(count) + ' column(s).\n')
    mysql_connection.commit()
    # mysql_cursor.close()

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("MQTT connected with result code "+str(rc)+"\n")

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(mqtt_topic)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    message = json.loads(msg.payload.decode("utf-8"))
    now = datetime.now()
    print(now)
    print('MQTT Received: \nmac_address = ' + message[MAC_ADDRESS_STRING] + '\nbeacon = ' + json.dumps(message[BEACON_STRING]))
    mysql_write(message[MAC_ADDRESS_STRING], json.dumps(message[BEACON_STRING]))

def mqtt_start():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.username_pw_set(mqtt_username, mqtt_password)
    client.connect(mqtt_host, mqtt_port, 60)

    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.
    client.loop_forever()


mysql_start()
mqtt_start()