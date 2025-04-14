import pandas as pd
import mysql.connector
import joblib

# ------------------ Load the Anomaly Detection Model ------------------
model = joblib.load("anomaly detection.joblib")

# ------------------ Connect to MySQL ------------------
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="mysql",
    database="SmartHomeEnergyUsageDB"
)
cursor = db.cursor(dictionary=True)

# ------------------ Query New Data ------------------
query = """
SELECT
    d.device_id,
    d.device_type,
    d.room_id,
    dul.timestamp,
    dul.power_consumption,
    dul.device_status,
    ed.temperature,
    ed.humidity,
    ed.weather_condition,
    ol.occupancy_count
FROM device_usage_log AS dul
JOIN devices AS d ON dul.device_id = d.device_id
JOIN environmental_data AS ed ON d.room_id = ed.room_id AND DATE(dul.timestamp) = DATE(ed.timestamp) AND HOUR(dul.timestamp) = HOUR(ed.timestamp)
JOIN occupancy_log AS ol ON d.room_id = ol.room_id AND DATE(dul.timestamp) = DATE(ol.timestamp) AND HOUR(dul.timestamp) = HOUR(ol.timestamp)
ORDER BY dul.timestamp DESC
LIMIT 100
"""
cursor.execute(query)
rows = cursor.fetchall()
cursor.close()
db.close()

# ------------------ Preprocess ------------------
df = pd.DataFrame(rows)
df['timestamp'] = pd.to_datetime(df['timestamp'])
df['day_of_week'] = df['timestamp'].dt.day_name()
df['hour'] = df['timestamp'].dt.hour
df['is_weekend'] = df['day_of_week'].isin(['Saturday', 'Sunday']).astype(int)
df.rename(columns={'occupancy_count': 'occupancy'}, inplace=True)

# Rolling average power per device
df['rolling_avg_power_3h'] = df.groupby('device_id')['power_consumption']\
    .rolling(window=3, min_periods=1).mean().reset_index(0, drop=True)

# Power consumption z-score per device
df['power_z_score'] = df.groupby('device_id')['power_consumption']\
    .transform(lambda x: (x - x.mean()) / x.std(ddof=0))

if 'device_status' not in df.columns:
    df['device_status'] = (df['power_consumption'] > 0).astype(int)


df['occupancy_mismatch'] = ((df['occupancy'] == 0) & (df['power_consumption'] > 0)).astype(int)

# Temp-to-status mismatch (e.g., AC off in high temp)
df['high_temp'] = (df['temperature'] > 30).astype(int)
df['ac_should_be_on'] = ((df['device_type'] == 'AC') & (df['high_temp'] == 1)).astype(int)
df['ac_mismatch'] = ((df['ac_should_be_on'] == 1) & (df['device_status'] == 0)).astype(int)

# Rolling average deviation
df['rolling_avg_power'] = df.groupby('device_id')['power_consumption'].transform(lambda x: x.rolling(3, min_periods=1).mean())
df['power_spike'] = ((df['power_consumption'] > 1.5 * df['rolling_avg_power']) & (df['power_consumption'] > 0)).astype(int)

device_freq = df['device_id'].value_counts(normalize=True)
df['device_id_freq'] = df['device_id'].map(device_freq)



X = df.drop('device_id',axis=1)

# ------------------ Predict Anomalies ------------------
df['anomaly'] = model.predict(X)  # -1 = anomaly
df['anomaly'] = df['anomaly'].map({1: 0, -1: 1})  # 1 = anomaly

# ------------------ Output ------------------
print("🚨 Real-Time Anomaly Detection:")
print(df[['timestamp', 'device_id', 'power_consumption', 'anomaly']].head())

df.to_csv("latest_detected_anomalies.csv", index=False)
print("✅ Anomalies saved to latest_detected_anomalies.csv")
