import time
import datetime
import pymssql
from influxdb import InfluxDBClient
from datetime import timedelta
import paho.mqtt.client as mqtt
import re
import json

# ==========================
# 🔹 CONFIGURATION SETTINGS
# ==========================
# INFLUXDB_HOST = 'influxdb'
INFLUXDB_HOST = 'localhost'
INFLUXDB_PORT = 8086
INFLUXDB_DATABASE = 'iot_data'

# MSSQL_SERVER = 'host.docker.internal'
MSSQL_SERVER = 'localhost'
MSSQL_USER = 'sa'
MSSQL_PASSWORD = 'NewStrong!Passw0rd'
MSSQL_DATABASE = 'iot_db'
MSSQL_PORT = 1435

# MQTT_BROKER = 'mosquitto'
MQTT_BROKER = 'localhost'
MQTT_PORT = 1883
MQTT_TOPIC_CANNOT_INSERT = "iot_sensors/insert_status/host"

INTERVAL = 1
DELAY = 5

# 🔹 Measurement-to-Topics Mapping
MEASUREMENT_TOPIC_MAP = {
    'got': ['iot_sensors/got/mc_01', 'iot_sensors/got/mc_02'],
    'machine_temp': ['iot_sensors/machine_temp/mc_01', 'iot_sensors/machine_temp/mc_02'],
    'machine_vibration': ['iot_sensors/machine_vibration/mc_01', 'iot_sensors/machine_vibration/mc_02']
}

# ==========================
# 🔹 CONNECT TO INFLUXDB
# ==========================
def connect_influxdb():
    try:
        client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT)
        client.ping()
        client.switch_database(INFLUXDB_DATABASE)
        print("✅ connected to influxdb")
        return client
    except Exception as e:
        print(f"🚨 InfluxDB Connection Error: {e}")
        return None

influx_client = connect_influxdb()
if not influx_client:
    exit(1)

# ==========================
# 🔹 CONNECT TO MSSQL
# ==========================
def connect_mssql(retries=3, delay=1):
    for attempt in range(retries):
        try:
            conn = pymssql.connect(server=MSSQL_SERVER, user=MSSQL_USER, password=MSSQL_PASSWORD, database=MSSQL_DATABASE, port=MSSQL_PORT)
            return conn
        except pymssql.Error as e:
            print(f"⚠️ MSSQL Connection Failed (Attempt {attempt+1}/{retries}): {e}")
            time.sleep(delay)
    print("🚨 MSSQL Connection Failed. Exiting...")
    exit(1)

# ==========================
# 🔹 MQTT SETUP
# ==========================
mqtt_client = mqtt.Client()
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)


# ==========================
# 🔹 CREATE TABLES BY TOPIC
# ==========================
def create_mssql_tables():
    conn = connect_mssql()
    cursor = conn.cursor()

    for measurement, topics in MEASUREMENT_TOPIC_MAP.items():
        for topic in topics:
            table_name = sanitize_table_name(topic)

            cursor.execute(f"SELECT COUNT(*) FROM sysobjects WHERE name='{table_name}' AND xtype='U'")
            if cursor.fetchone()[0] > 0:
                print(f"⚡ Table '{table_name}' already exists.")
                continue

            query = f"SELECT * FROM \"{measurement}\" WHERE topic = '{topic}' LIMIT 1"
            result = influx_client.query(query)
            points = list(result.get_points())

            if not points:
                print(f"⚠️ No sample data for topic '{topic}', skipping table creation.")
                continue

            data_keys = [key for key in points[0].keys() if key not in ['time', 'topic','host']]  # Exclude 'time' and 'topic'
            columns_sql = ", ".join([f"{key} FLOAT" for key in data_keys])

            create_query = f"""
            CREATE TABLE {table_name} (
                time DATETIME PRIMARY KEY,
                topic VARCHAR(255),
                {columns_sql}
            );
            """
            cursor.execute(create_query)
            conn.commit()
            print(f"✅ Table '{table_name}' created with columns: {', '.join(data_keys)}")

    cursor.close()
    conn.close()

# ==========================
# 🔹 CREATE TABLES BY TOPIC
# ==========================

# ==========================
# 🔹 SANITIZE TABLE NAME
# ==========================
def sanitize_table_name(topic):
    if topic.startswith("iot_sensors/"):
        topic = topic[len("iot_sensors/"):]
    return f"raw_{re.sub(r'[^a-zA-Z0-9_]', '_', topic)}"

# ==========================
# 🔹 FETCH DATA FROM INFLUXDB
# ==========================
def fetch_influxdb_data():
    now = datetime.datetime.utcnow()
    # start_time = now - datetime.timedelta(minutes=1, seconds=now.second, microseconds=now.microsecond)
    start_time = now - datetime.timedelta(minutes=INTERVAL * 3, seconds=now.second, microseconds=now.microsecond)
    end_time = start_time + datetime.timedelta(minutes=INTERVAL*3)

    all_data = {}

    for measurement, topics in MEASUREMENT_TOPIC_MAP.items():
        for topic in topics:
            table_name = sanitize_table_name(topic)
            if table_name not in all_data:
                all_data[table_name] = []

            query = f"""
                SELECT * FROM \"{measurement}\"
                WHERE time >= '{start_time.isoformat()}Z' AND time < '{end_time.isoformat()}Z'
                AND topic = '{topic}'
            """
            result = influx_client.query(query)
            all_data[table_name].extend(list(result.get_points()))

    return all_data

# ==========================
# 🔹 INSERT INTO MSSQL
# ==========================
def insert_data_to_mssql(data):
    conn = connect_mssql()
    cursor = conn.cursor()

    for table_name, rows in data.items():
        for row in rows:
            try:
                timestamp = datetime.datetime.strptime(row['time'], '%Y-%m-%dT%H:%M:%S.%fZ') + timedelta(hours=7)
                timestamp_str = timestamp.isoformat()
                topic = row['topic']
                values = {key: row[key] for key in row if key not in ['time', 'topic', 'host']}

                check_query = f"SELECT COUNT(*) FROM {table_name} WHERE time = %s AND topic = %s"
                cursor.execute(check_query, (timestamp, topic))
                if cursor.fetchone()[0] == 0:
                    columns = ', '.join(['topic'] + list(values.keys()))
                    placeholders = ', '.join(['%s'] * (len(values) + 1))
                    insert_query = f"INSERT INTO {table_name} (time, {columns}) VALUES (%s, {placeholders})"
                    cursor.execute(insert_query, (timestamp, topic, *values.values()))
                    conn.commit()

                    mqtt_message = {
                        "data_id": values.get("data_id", None),
                        "status": "success",
                        "error" : "ok",
                        "timestamp": timestamp_str,
                        "table_name": table_name
                    }
                    mqtt_client.publish(MQTT_TOPIC_CANNOT_INSERT, json.dumps(mqtt_message))
                    print(f"✅ Inserted: {timestamp} | Table: {table_name}")
                    time.sleep(0.1)
                else:
                    print(f"⚠️ Data already exists for: {timestamp} | Table: {table_name}")
            except Exception as e:
                conn.rollback()
                mqtt_message = {
                    "data_id": row.get("data_id", None),
                    "status": "fail",
                    "error": str(e),
                    "timestamp": timestamp_str,
                    "table_name": table_name
                }
                mqtt_client.publish(MQTT_TOPIC_CANNOT_INSERT, json.dumps(mqtt_message))
                print(f"⚠️ Failed to insert: {timestamp} | Table: {table_name}")
                print(f"📡 Published to MQTT: {mqtt_message}")
                time.sleep(0.1)

    cursor.close()
    conn.close()

# ==========================
# 🔹 MAIN LOOP
# ==========================
def main():
    while True:
        time.sleep(DELAY)
        try:
            influx_data = fetch_influxdb_data()
            if influx_data:
                create_mssql_tables()
                insert_data_to_mssql(influx_data)
            else:
                print("❌ No new data found!")
        except Exception as e:
            mqtt_message = {
                    "data_id": -1,
                    "status": "fail",
                    "error": str(e),
                    "timestamp": datetime.datetime.utcnow(),
                    "table_name": "-1"
                }
            mqtt_client.publish(MQTT_TOPIC_CANNOT_INSERT, json.dumps(mqtt_message))
            print(f"🚨 Error: {e}")
        time.sleep(INTERVAL * 60 - DELAY)

# ==========================
# 🔹 RUN SCRIPT
# ==========================
if __name__ == "__main__":
    main()
