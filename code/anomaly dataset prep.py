import pandas as pd

# Load from CSV (or from SQL if you're using MySQL)
df = pd.read_csv("future_usage_prediction_dataset_3.csv")

# Sort by time
df['timestamp'] = pd.to_datetime(df['timestamp'])
df = df.sort_values(by='timestamp')

# Optional: Drop future consumption if it exists
if 'next_hour_power_consumption' in df.columns:
    df = df.drop(columns=['next_hour_power_consumption'])

# Rolling average power per device
df['rolling_avg_power_3h'] = df.groupby('device_id')['power_consumption']\
    .rolling(window=3, min_periods=1).mean().reset_index(0, drop=True)

# Power consumption z-score per device
df['power_z_score'] = df.groupby('device_id')['power_consumption']\
    .transform(lambda x: (x - x.mean()) / x.std(ddof=0))

# Final feature selection
selected_features = [
    'device_id', 'device_type', 'room_id',
    'power_consumption', 'occupancy', 'temperature', 'humidity',
    'hour', 'day_of_week', 'is_weekend',
    'rolling_avg_power_3h', 'power_z_score'
]

df = df[selected_features]

df.to_csv("anomaly_detection_dataset.csv", index=False)
print("✅ Dataset for anomaly detection saved.")
