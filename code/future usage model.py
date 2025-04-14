import pandas as pd
import mysql.connector
import joblib
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import category_encoders as ce
# ------------------ Load the Future Usage Model ------------------
model = joblib.load("future usage prediction 3.joblib")

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
    ol.occupancy_count,
    ed.temperature,
    ed.humidity,
    ed.weather_condition
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

# ------------------ Data Prep ------------------
df = pd.DataFrame(rows)
df['timestamp'] = pd.to_datetime(df['timestamp'])



df['day_of_week'] = df['timestamp'].dt.day_name()
df['hour'] = df['timestamp'].dt.hour
df['is_weekend'] = df['day_of_week'].isin(['Saturday', 'Sunday']).astype(int)
df.rename(columns={'occupancy_count': 'occupancy'}, inplace=True)

df['prev_hour_usage'] = df.groupby('device_id')['power_consumption'].shift(1)

df['rolling_avg_3hr'] = (
    df.groupby('device_id')['power_consumption']
    .shift(1).rolling(window=3, min_periods=1).mean().reset_index(0, drop=True)
)

df['rolling_avg_24hr'] = (
    df.groupby('device_id')['power_consumption']
    .shift(1).rolling(window=24, min_periods=1).mean().reset_index(0, drop=True)
)
df['month']=df['timestamp'].dt.month

device_dummies = pd.get_dummies(df['device_type'], prefix='temp_device')
for col in device_dummies.columns:
    df[col] = device_dummies[col] * df['temperature']
# Drop non-feature cols and match training input
X = df.drop(['timestamp'], axis=1)
num_cols=[c for c in X.columns if X[c].dtype in ['int64','float64']]
cat_cols=[cols for cols in X.columns if X[cols].dtype=='object']

print(X.columns)
scaler = MinMaxScaler()
energy_data_scaled = X.copy()
energy_data_scaled[num_cols] = scaler.fit_transform(energy_data_scaled[num_cols])


# ------------------ Predict ------------------
predictions = model.predict(energy_data_scaled)
df['predicted_next_hour_usage'] = predictions

# ------------------ Output ------------------
print("📈 Future Usage Predictions (latest 100):")
print(df[['timestamp', 'device_id', 'power_consumption', 'predicted_next_hour_usage']].head())

df.to_csv("latest_future_usage_predictions.csv", index=False)
# print("✅ Predictions saved to latest_future_usage_predictions.csv")
