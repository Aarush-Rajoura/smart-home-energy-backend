import mysql.connector
import random
from datetime import datetime
import time

# -------------------- DB CONFIG --------------------
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="mysql",
    database="SmartHomeEnergyUsageDB"
)
cursor = db.cursor()

# -------------------- Simulation Setup --------------------
device_ids = [f'device_{i}' for i in range(1, 101)]
room_ids = ['room_0', 'room_1', 'room_2', 'room_3','room_4']
weather_conditions = ['Sunny', 'Cloudy', 'Rainy', 'Windy', 'Humid']

def get_current_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def generate_temperature(hour):
    if 6 <= hour <= 18:
        return round(random.uniform(24, 32), 2)
    else:
        return round(random.uniform(18, 24), 2)

def generate_humidity():
    return round(random.uniform(40, 80), 2)

def generate_power_consumption(device_id):
    if 'AC' in device_id:
        return round(random.uniform(1000, 2000), 2)
    elif 'Light' in device_id:
        return round(random.uniform(20, 80), 2)
    elif 'Fridge' in device_id:
        return round(random.uniform(80, 150), 2)
    else:
        return round(random.uniform(30, 100), 2)

def generate_device_status(power):
    return 1 if power > 0 else 0

def generate_occupancy(hour):
    if 7 <= hour <= 10 or 18 <= hour <= 22:
        return random.randint(1, 4)
    else:
        return random.choice([0, 1])

# -------------------- Main Simulation Loop --------------------
def simulate_data_insert():
    timestamp = get_current_timestamp()
    hour = datetime.now().hour

    for device_id in device_ids:
        power = generate_power_consumption(device_id)
        status = generate_device_status(power)
        cursor.execute("""
            INSERT INTO device_usage_log (timestamp, device_id, power_consumption, device_status)
            VALUES (%s, %s, %s, %s)
        """, (timestamp, device_id, power, status))

    for room_id in room_ids:
        temp = generate_temperature(hour)
        humid = generate_humidity()
        occupancy = generate_occupancy(hour)
        weather = random.choice(weather_conditions)

        cursor.execute("""
            INSERT INTO environmental_data (timestamp, room_id, temperature, humidity, weather_condition)
            VALUES (%s, %s, %s, %s, %s)
        """, (timestamp, room_id, temp, humid, weather))

        cursor.execute("""
            INSERT INTO occupancy_log (timestamp, room_id, occupancy_count)
            VALUES (%s, %s, %s)
        """, (timestamp, room_id, occupancy))

    # External weather (simulated global)
    temp_out = generate_temperature(hour) + random.uniform(-2, 2)
    humid_out = generate_humidity()
    weather_out = random.choice(weather_conditions)
    cursor.execute("""
        INSERT INTO external_weather_log (timestamp, temperature, humidity, weather_condition)
        VALUES (%s, %s, %s, %s)
    """, (timestamp, temp_out, humid_out, weather_out))

    db.commit()
    print(f"✅ Data inserted at {timestamp}")

# -------------------- Run Once or Loop --------------------
simulate_data_insert()
# To simulate real-time every minute:
# while True:
#     simulate_data_insert()
#     time.sleep(60)
