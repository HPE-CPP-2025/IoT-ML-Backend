import pandas as pd
import numpy as np
import joblib
from keras.models import load_model
# import matplotlib.pyplot as plt # Keep if plotting is needed later, otherwise remove
import psycopg2
import psycopg2.extras # Import extras for batch execution
from datetime import datetime, timedelta # Import timedelta
import os  # Import os
import sys # Import sys
from dotenv import load_dotenv # Import load_dotenv
import time # Import time for potential aggregation logic

# Load environment variables from .env file in parent directory
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
if not os.path.exists(dotenv_path):
    print(f"Warning: .env file not found at {dotenv_path}. Using hardcoded credentials (if any) or defaults.", file=sys.stderr)
load_dotenv(dotenv_path=dotenv_path)

# --- Local Database Configuration ---
LOCAL_DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'energy_data'), # Default if not in .env
    'user': os.getenv('DB_USER', 'postgres'),     # Default if not in .env
    'password': os.getenv('DB_PASSWORD', 'root'), # Default if not in .env
    'host': os.getenv('DB_HOST', 'localhost'),    # Default if not in .env
    'port': os.getenv('DB_PORT', '5432')         # Default if not in .env
}

# --- Neon Database Configuration ---
NEON_DB_URL = os.getenv('NEON_DB_URL')
if not NEON_DB_URL:
    print("Error: NEON_DB_URL not found in .env file.", file=sys.stderr)
    sys.exit(1)

# --- End Database Configuration ---


# Load data and trained model
model_path = os.path.join(os.path.dirname(__file__), 'unified_lstm_model.h5')
scaler_path = os.path.join(os.path.dirname(__file__), 'power_scaler.joblib')

try:
    model = load_model(model_path)
    power_scaler = joblib.load(scaler_path)
except Exception as e:
    print(f"Error loading model or scaler: {e}", file=sys.stderr)
    sys.exit(1)


# Function to make predictions
def predict_future(model, last_sequence, device_id, n_steps, scaler):
    future_predictions = []
    current_sequence = last_sequence.copy()
    # Ensure device_id is numeric for the model if necessary
    try:
        device_id_numeric = int(device_id)
    except ValueError:
        print(f"Warning: Could not convert device_id '{device_id}' to int. Using 0.", file=sys.stderr)
        device_id_numeric = 0 # Or handle as appropriate

    for _ in range(n_steps):
        current_power = current_sequence.reshape((1, len(current_sequence), 1))
        # Use the numeric device ID
        current_device = np.array([device_id_numeric]).reshape(1, 1)
        pred = model.predict([current_power, current_device], verbose=0)
        future_predictions.append(pred[0, 0])
        current_sequence = np.append(current_sequence[1:], pred[0, 0])
    return scaler.inverse_transform(np.array(future_predictions).reshape(-1, 1))

# PostgreSQL setup for local DB (reading and writing predictions)
local_conn = None
local_cursor = None
# Setup for Neon DB (writing aggregated predictions)
neon_conn = None
neon_cursor = None

# --- Main Prediction and Local Insertion Logic ---
try:
    # Connect to local DB to fetch data and insert predictions
    print(f"Connecting to local database '{LOCAL_DB_CONFIG['dbname']}' on {LOCAL_DB_CONFIG['host']}...")
    local_conn = psycopg2.connect(**LOCAL_DB_CONFIG)
    local_cursor = local_conn.cursor()
    print("Local database connection successful.")

    # Fetch data from energy_readings table based on actual columns
    print("Fetching data from local 'energy_readings' table...")
    local_cursor.execute("""
    SELECT device_id, timestamp, power
    FROM energy_readings
    ORDER BY device_id, timestamp
    """)
    print("Data fetched. Processing...")

    # Convert query results to pandas DataFrame
    columns = ['device_id', 'timestamp', 'power']
    data = local_cursor.fetchall()
    if not data:
        print("No data found in local 'energy_readings' table. Exiting.", file=sys.stderr)
        sys.exit(0) # Exit gracefully if no data

    df = pd.DataFrame(data, columns=columns)

    # Convert timestamp to datetime objects
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Rename 'power' column to 'Power' to match scaler's expectation
    df.rename(columns={'power': 'Power'}, inplace=True) # Added this line

    # Scale the power values using the loaded scaler
    df['power_scaled'] = power_scaler.transform(df[['Power']]) # Changed 'power' to 'Power'
    print("Data processed and scaled.")

    # Predict future energy consumption for each device
    device_ids = df['device_id'].unique()
    time_step = 100 # Ensure this matches training
    future_steps = 60 # Predict next 60 steps (e.g., hours) - Adjust as needed

    print(f"Found {len(device_ids)} unique device IDs. Predicting {future_steps} steps ahead and inserting into local DB...")

    total_predictions_inserted_locally = 0

    for device_id in device_ids:
        print(f"\nProcessing device_id: {device_id}")
        device_data = df[df['device_id'] == device_id].sort_values('timestamp') # Ensure sorted

        if len(device_data) >= time_step:
            print(f"Device {device_id} has enough data ({len(device_data)} rows). Preparing last sequence...")
            last_sequence = device_data['power_scaled'].values[-time_step:]
            predictions = predict_future(model, last_sequence, device_id, future_steps, power_scaler)

            # Get the last timestamp and frequency
            last_timestamp = device_data['timestamp'].iloc[-1]

            # Infer frequency or set manually (e.g., 'H' for hourly)
            data_freq = pd.infer_freq(device_data['timestamp'].iloc[-5:])
            if data_freq is None:
                # Attempt to calculate frequency manually if inference fails
                if len(device_data['timestamp']) > 1:
                    time_diff = (device_data['timestamp'].iloc[-1] - device_data['timestamp'].iloc[-2])
                    # Basic check for common frequencies, default to Hourly
                    if time_diff == timedelta(hours=1): data_freq = 'H'
                    elif time_diff == timedelta(minutes=1): data_freq = 'T'
                    elif time_diff == timedelta(seconds=1): data_freq = 'S'
                    elif time_diff == timedelta(days=1): data_freq = 'D'
                    else:
                        data_freq = 'H'; print(f"Could not determine regular frequency for device {device_id}. Defaulting to Hourly ('H'). Last diff: {time_diff}", file=sys.stderr)
                else:
                    data_freq = 'H'; print(f"Only one data point for device {device_id}. Defaulting frequency to Hourly ('H').", file=sys.stderr)

            print(f"Inferred/Set data frequency unit: {data_freq}")

            # Ensure data_freq is a valid offset string (e.g., '1H', '1S')
            if len(data_freq) == 1 and data_freq.isalpha(): adjusted_freq = '1' + data_freq
            else: adjusted_freq = data_freq # Use as is if already multi-char or not purely alpha

            # Generate future dates starting *after* the last recorded timestamp
            # Use the determined frequency
            try:
                # Use adjusted_freq for Timedelta and date_range
                time_delta_increment = pd.Timedelta(adjusted_freq)
                future_dates = pd.date_range(start=last_timestamp + time_delta_increment, periods=future_steps, freq=adjusted_freq)
            except ValueError as e:
                 print(f"Error creating date range for device {device_id} with frequency '{adjusted_freq}': {e}", file=sys.stderr)
                 print(f"Skipping prediction insertion for device {device_id}.", file=sys.stderr)
                 continue # Skip to the next device if date range fails

            print(f"Inserting/Updating {len(predictions)} raw predictions into local 'predictions' table for device {device_id}...")
            insert_count = 0
            # Store raw predictions in LOCAL database using ON CONFLICT
            for date, pred in zip(future_dates, predictions.flatten()):
                try:
                    local_cursor.execute(
                        """
                        INSERT INTO predictions (device_id, timestamp, predicted_power)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (device_id, timestamp) DO UPDATE SET
                            predicted_power = EXCLUDED.predicted_power;
                        """,
                        (str(device_id), date, float(pred))
                    )
                    if local_cursor.rowcount > 0:
                        insert_count += 1
                except psycopg2.Error as insert_err:
                    print(f"Error inserting prediction for device {device_id} at {date}: {insert_err}", file=sys.stderr)
                    local_conn.rollback() # Rollback this specific insertion attempt
                    # Consider whether to continue with the next prediction or stop for this device
                    # break # Example: Stop processing this device on error
                except Exception as general_err:
                    print(f"Unexpected error during prediction insert for device {device_id} at {date}: {general_err}", file=sys.stderr)
                    local_conn.rollback()
                    # break

            local_conn.commit() # Commit after each device's predictions are inserted
            total_predictions_inserted_locally += insert_count
            print(f"Committed {insert_count} raw predictions for device {device_id} to local DB.")

        else:
            print(f"Skipping device {device_id}: Insufficient data ({len(device_data)} rows, need {time_step}).")

    print(f"\nRaw prediction generation and local insertion completed. Total inserted/updated locally: {total_predictions_inserted_locally}")

    # --- Aggregation and Neon Upload Logic ---
    if total_predictions_inserted_locally > 0: # Proceed only if predictions were inserted
        print("\nStarting aggregation of local predictions...")
        aggregated_data_to_upload = []
        try:
            # Define the aggregation interval (e.g., 10 minutes)
            # Using DATE_TRUNC('minute', ...) for simplicity. Adjust as needed.
            # For 10-min intervals: EXTRACT(EPOCH FROM timestamp)::integer / 600 * 600
            aggregation_interval_minutes = 10
            aggregation_sql = f"""
                SELECT
                    device_id,
                    -- Truncate timestamp to the desired interval (e.g., 10 minutes)
                    to_timestamp(FLOOR(EXTRACT(EPOCH FROM timestamp) / ({aggregation_interval_minutes} * 60)) * ({aggregation_interval_minutes} * 60)) AT TIME ZONE 'UTC' AS aggregated_timestamp,
                    AVG(predicted_power) AS avg_predicted_power
                FROM predictions
                -- Optional: Add a WHERE clause to only aggregate recent predictions if needed
                -- WHERE timestamp > NOW() - INTERVAL '1 day'
                GROUP BY device_id, aggregated_timestamp
                ORDER BY device_id, aggregated_timestamp;
            """
            print(f"Executing aggregation query on local DB:\n{aggregation_sql}")
            # Use a new cursor or reuse if appropriate (ensure no pending transactions)
            # Reusing local_cursor after commit is fine
            local_cursor.execute(aggregation_sql)
            aggregated_results = local_cursor.fetchall()
            print(f"Fetched {len(aggregated_results)} aggregated prediction rows from local DB.")

            if aggregated_results:
                # Prepare data for Neon batch insert (device_id, aggregated_timestamp, avg_predicted_power)
                aggregated_data_to_upload = [
                    (str(row[0]), row[1], float(row[2])) for row in aggregated_results
                ]

                # Connect to Neon DB for batch upload
                print(f"\nConnecting to Neon database for batch upload of {len(aggregated_data_to_upload)} aggregated predictions...")
                neon_conn = psycopg2.connect(NEON_DB_URL)
                neon_cursor = neon_conn.cursor()
                print("Neon database connection successful.")

                # Define Neon insert statement
                dest_table_neon = 'predictions'
                target_cols_neon = ["device_id", "timestamp", "predicted_power"] # Neon table columns
                cols_sql_neon = ', '.join(f'"{col}"' for col in target_cols_neon)
                placeholders_neon = ', '.join(['%s'] * len(target_cols_neon))
                update_cols_neon = [f'"{col}" = EXCLUDED."{col}"' for col in target_cols_neon if col not in ('device_id', 'timestamp')]
                update_sql_neon = ', '.join(update_cols_neon)

                insert_sql_neon = f"""
                    INSERT INTO {dest_table_neon} ({cols_sql_neon})
                    VALUES ({placeholders_neon})
                    ON CONFLICT (device_id, timestamp) DO UPDATE SET {update_sql_neon}
                """
                print(f"Preparing to batch insert/update into Neon table '{dest_table_neon}'...")
                print(f"Using SQL:\n{insert_sql_neon}")

                # Use execute_batch for efficiency
                psycopg2.extras.execute_batch(neon_cursor, insert_sql_neon, aggregated_data_to_upload, page_size=1000)
                neon_conn.commit()
                print(f"Successfully inserted/updated {len(aggregated_data_to_upload)} aggregated predictions in Neon DB.")

            else:
                print("No aggregated data found to upload to Neon.")

        except (Exception, psycopg2.DatabaseError) as agg_neon_error:
            print(f"\nError during aggregation or Neon DB upload: {agg_neon_error}", file=sys.stderr)
            if neon_conn:
                print("Rolling back Neon transaction...")
                neon_conn.rollback()
        finally:
            # Close Neon cursor and connection if they were opened
            if neon_cursor:
                neon_cursor.close()
                print("Neon database cursor closed.")
            if neon_conn:
                neon_conn.close()
                print("Neon database connection closed.")
    else:
        print("\nSkipping aggregation and Neon upload as no raw predictions were inserted locally.")
    # --- End Aggregation and Neon Upload ---


except psycopg2.DatabaseError as db_err:
    print(f"\nLocal Database error: {db_err}", file=sys.stderr)
    if local_conn:
        local_conn.rollback()
except FileNotFoundError as fnf_err:
     print(f"\nFile not found error: {fnf_err}", file=sys.stderr)
except Exception as e:
    print(f"\nAn unexpected error occurred: {e}", file=sys.stderr)
    if local_conn:
        local_conn.rollback() # Rollback local connection on general error too
finally:
    # Close local cursor and connection
    if local_cursor:
        local_cursor.close()
        print("Local database cursor closed.")
    if local_conn:
        local_conn.close()
        print("Local database connection closed.")

    # Ensure Neon connection is closed if an error occurred before its finally block
    if neon_cursor and not neon_cursor.closed:
        neon_cursor.close()
        print("Neon database cursor closed (outer finally).")
    if neon_conn and not neon_conn.closed:
        neon_conn.close()
        print("Neon database connection closed (outer finally).")

print("\nScript finished.")