import pandas as pd
import numpy as np
import joblib
from keras.models import load_model
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
import os
import sys
from dotenv import load_dotenv

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
if not os.path.exists(dotenv_path):
    print(f"Warning: .env file not found at {dotenv_path}", file=sys.stderr)
load_dotenv(dotenv_path=dotenv_path)

# Database configurations
LOCAL_DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'energy_data'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'root'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432')
}

NEON_DB_URL = os.getenv('NEON_DB_URL')
if not NEON_DB_URL:
    print("Error: NEON_DB_URL not found in .env file.", file=sys.stderr)
    sys.exit(1)

# Load trained model and scaler
model_path = os.path.join(os.path.dirname(__file__), 'unified_lstm_model.h5')
scaler_path = os.path.join(os.path.dirname(__file__), 'power_scaler.joblib')

try:
    model = load_model(model_path)
    power_scaler = joblib.load(scaler_path)
except Exception as e:
    print(f"Error loading model or scaler: {e}", file=sys.stderr)
    sys.exit(1)

def predict_future(model, last_sequence, device_id, n_steps, scaler):
    """Generate future predictions using the trained model."""
    future_predictions = []
    current_sequence = last_sequence.copy()
    
    try:
        device_id_numeric = int(device_id)
    except ValueError:
        device_id_numeric = 0

    for _ in range(n_steps):
        current_power = current_sequence.reshape((1, len(current_sequence), 1))
        current_device = np.array([device_id_numeric]).reshape(1, 1)
        pred = model.predict([current_power, current_device], verbose=0)
        future_predictions.append(pred[0, 0])
        current_sequence = np.append(current_sequence[1:], pred[0, 0])
    
    return scaler.inverse_transform(np.array(future_predictions).reshape(-1, 1))

# Database connections
local_conn = None
local_cursor = None
neon_conn = None
neon_cursor = None

try:
    # Connect to local database
    local_conn = psycopg2.connect(**LOCAL_DB_CONFIG)
    local_cursor = local_conn.cursor()
    print("Local database connected")

    # Fetch energy data
    local_cursor.execute("""
        SELECT device_id, house_id, timestamp, power
        FROM energy_readings
        ORDER BY device_id, house_id, timestamp
    """)

    data = local_cursor.fetchall()
    if not data:
        print("No data found in energy_readings table", file=sys.stderr)
        sys.exit(0)

    # Process data
    columns = ['device_id', 'house_id', 'timestamp', 'power']
    df = pd.DataFrame(data, columns=columns)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.rename(columns={'power': 'Power'}, inplace=True)
    df['power_scaled'] = power_scaler.transform(df[['Power']])

    # Generate predictions for each device
    device_ids = df['device_id'].unique()
    time_step = 100
    future_steps = 60

    print(f"Processing {len(device_ids)} devices, predicting {future_steps} steps ahead")
    total_predictions_inserted_locally = 0

    for device_id in device_ids:
        device_data = df[df['device_id'] == device_id].sort_values('timestamp')
        
        if device_data.empty:
            continue
            
        current_house_id = device_data['house_id'].iloc[0]

        if len(device_data) >= time_step:
            last_sequence = device_data['power_scaled'].values[-time_step:]
            predictions = predict_future(model, last_sequence, device_id, future_steps, power_scaler)

            # Generate future timestamps
            last_timestamp = device_data['timestamp'].iloc[-1]
            data_freq = pd.infer_freq(device_data['timestamp'].iloc[-5:])
            
            if data_freq is None:
                if len(device_data['timestamp']) > 1:
                    time_diff = (device_data['timestamp'].iloc[-1] - device_data['timestamp'].iloc[-2])
                    if time_diff == timedelta(hours=1):
                        data_freq = 'h'
                    elif time_diff == timedelta(minutes=1):
                        data_freq = 't'
                    elif time_diff == timedelta(seconds=1):
                        data_freq = 's'
                    else:
                        data_freq = 'h'
                else:
                    data_freq = 'h'

            # Ensure frequency format is correct
            if len(data_freq) == 1 and data_freq.isalpha():
                adjusted_freq = '1' + data_freq
            else:
                adjusted_freq = data_freq

            # Convert uppercase frequency to lowercase if needed
            if len(adjusted_freq) == 1 and adjusted_freq.isupper() and adjusted_freq in ['S', 'H', 'T', 'M', 'Y']:
                adjusted_freq = adjusted_freq.lower()
            elif len(adjusted_freq) > 1 and adjusted_freq[0].isdigit() and len(adjusted_freq[1:]) == 1 and adjusted_freq[1:].isupper() and adjusted_freq[1:] in ['S', 'H', 'T', 'M', 'Y']:
                adjusted_freq = adjusted_freq[0] + adjusted_freq[1:].lower()

            try:
                time_delta_increment = pd.Timedelta(adjusted_freq)
                future_dates = pd.date_range(
                    start=last_timestamp + time_delta_increment,
                    periods=future_steps,
                    freq=adjusted_freq
                )
            except ValueError as e:
                print(f"Error creating date range for device {device_id}: {e}", file=sys.stderr)
                continue

            # Insert predictions into local database
            insert_count = 0
            for date, pred in zip(future_dates, predictions.flatten()):
                try:
                    house_id_for_insert = int(current_house_id)
                    local_cursor.execute(
                        """
                        INSERT INTO predictions (device_id, house_id, timestamp, predicted_power)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (device_id, timestamp) DO UPDATE SET
                            house_id = EXCLUDED.house_id,
                            predicted_power = EXCLUDED.predicted_power;
                        """,
                        (str(device_id), house_id_for_insert, date, float(pred))
                    )
                    if local_cursor.rowcount > 0:
                        insert_count += 1
                except Exception as e:
                    print(f"Error inserting prediction for device {device_id}: {e}", file=sys.stderr)
                    local_conn.rollback()

            local_conn.commit()
            total_predictions_inserted_locally += insert_count
            print(f"Inserted {insert_count} predictions for device {device_id}")
        else:
            print(f"Skipping device {device_id}: insufficient data")

    print(f"Total predictions inserted locally: {total_predictions_inserted_locally}")

    # Aggregate and upload to Neon if predictions were made
    if total_predictions_inserted_locally > 0:
        print("Aggregating predictions for Neon upload...")
        
        aggregation_interval_minutes = 5
        aggregation_sql = f"""
            SELECT
                device_id,
                house_id,
                to_timestamp(FLOOR(EXTRACT(EPOCH FROM timestamp) / ({aggregation_interval_minutes} * 60)) * ({aggregation_interval_minutes} * 60)) AT TIME ZONE 'UTC' AS aggregated_timestamp,
                AVG(predicted_power) AS avg_predicted_power
            FROM predictions
            GROUP BY device_id, house_id, aggregated_timestamp
            ORDER BY device_id, house_id, aggregated_timestamp;
        """
        
        local_cursor.execute(aggregation_sql)
        aggregated_results = local_cursor.fetchall()
        print(f"Aggregated {len(aggregated_results)} prediction rows")

        if aggregated_results:
            aggregated_data_to_upload = [
                (str(row[0]), int(row[1]), row[2], float(row[3])) for row in aggregated_results
            ]

            # Upload to Neon
            neon_conn = psycopg2.connect(NEON_DB_URL)
            neon_cursor = neon_conn.cursor()
            print("Connected to Neon database")

            insert_sql_neon = """
                INSERT INTO predictions (device_id, house_id, timestamp, predicted_power)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (device_id, timestamp) DO UPDATE SET
                    house_id = EXCLUDED.house_id,
                    predicted_power = EXCLUDED.predicted_power
            """

            psycopg2.extras.execute_batch(
                neon_cursor, insert_sql_neon, aggregated_data_to_upload, page_size=1000
            )
            neon_conn.commit()
            print(f"Uploaded {len(aggregated_data_to_upload)} aggregated predictions to Neon")

except psycopg2.DatabaseError as db_err:
    print(f"Database error: {db_err}", file=sys.stderr)
    if local_conn:
        local_conn.rollback()
except Exception as e:
    print(f"Unexpected error: {e}", file=sys.stderr)
    if local_conn:
        local_conn.rollback()
finally:
    # Close database connections
    if local_cursor:
        local_cursor.close()
    if local_conn:
        local_conn.close()
    if neon_cursor and not neon_cursor.closed:
        neon_cursor.close()
    if neon_conn and not neon_conn.closed:
        neon_conn.close()

print("Script finished")