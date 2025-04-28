import sys
import json
from datetime import datetime
import random
import psycopg2
import time
from threading import Event
import os # Added
from dotenv import load_dotenv # Added

# --- Configuration --- Added ---
load_dotenv() # Load .env variables
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not all([DB_NAME, DB_USER, DB_PASSWORD]):
    print("Error: Missing DB config in .env (DB_NAME, DB_USER, DB_PASSWORD)", file=sys.stderr)
    # Decide how to handle this - exit or try defaults? Exiting is safer.
    sys.exit(1)

# Construct connection string for local DB
DB_CONN_STRING = f"host='{DB_HOST}' port='{DB_PORT}' dbname='{DB_NAME}' user='{DB_USER}' password='{DB_PASSWORD}'"
# --- End Configuration --- Added ---


# Database connection URL - REMOVED Hardcoded URL
# DB_URL = "postgresql://neondb_owner:npg_gY3AR9uUQjpy@ep-falling-pond-a19wy8o7-pooler.ap-southeast-1.aws.neon.tech/my_energy_db?sslmode=require" # REMOVED

def connect_to_db():
    """Connects to the local PostgreSQL database using config from .env""" # Updated docstring
    try:
        # Use the connection string derived from .env variables
        conn = psycopg2.connect(DB_CONN_STRING) # UPDATED
        return conn
    except Exception as e:
        print(f"Error connecting to the local database: {e}", file=sys.stderr) # Updated message
        return None

# ... existing generate_energy_reading function ...
def generate_energy_reading(device_id):
    # Generate values similar to those in the image
    base_voltage = 238.5
    base_current = 0.45
    base_power = 109.5

    return {
        "device_id": device_id,
        # Use timezone-naive datetime consistent with serial_to_db.py insert
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), # UPDATED format
        "voltage": round(base_voltage + random.uniform(-0.2, 0.2), 2),
        "current": round(base_current + random.uniform(-0.01, 0.01), 2),
        "power": round(base_power + random.uniform(-0.1, 0.1), 2),
        "energy": 0.005,  # Constant as shown in image
        "frequency": 50.0,  # Constant as shown in image
        "power_factor": 1.00,  # Constant as shown in image
    }

def insert_energy_readings(conn, readings):
    """Inserts simulated energy readings into the energy_readings table.""" # Updated docstring
    # Ensure this matches the schema used by serial_to_db.py
    sql = """
        INSERT INTO energy_readings
        (device_id, timestamp, voltage, current, power, energy, frequency, power_factor)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """ # UPDATED SQL to match serial_to_db.py (removed is_simulated if not needed)
    cursor = None # Initialize cursor to None
    try:
        cursor = conn.cursor()
        for reading in readings:
            # Convert timestamp string back to datetime object if needed by DB adapter,
            # or ensure the string format matches the DB column type.
            # psycopg2 generally handles ISO format strings well for timestamp columns.
            ts_obj = datetime.strptime(reading["timestamp"], '%Y-%m-%d %H:%M:%S') # Convert string to datetime

            values = ( # UPDATED values tuple
                reading["device_id"],
                ts_obj, # Use datetime object
                reading["voltage"],
                reading["current"],
                reading["power"],
                reading["energy"],
                reading["frequency"],
                reading["power_factor"]
            )
            cursor.execute(sql, values) # Use updated SQL and values
        conn.commit()
        # Updated print statement for clarity
        print(f"[CRON DB Insert OK] Simulated data for device(s): {[r['device_id'] for r in readings]}", file=sys.stderr)
    except Exception as e:
        print(f"[CRON DB Insert ERR] {e}", file=sys.stderr) # Updated error message
        if conn: conn.rollback() # Check if conn exists before rollback
    finally:
        # Ensure cursor exists before trying to close
        if cursor: cursor.close() # UPDATED check

# ... existing print_reading_to_console function ...
def print_reading_to_console(reading):
    """Print the reading in the format shown in the image"""
    print(f"Custom Address:{reading['device_id']}")
    print(f"Voltage: {reading['voltage']}V")
    print(f"Current: {reading['current']}A")
    print(f"Power: {reading['power']}W")
    print(f"Energy: {reading['energy']}kWh")
    print(f"Frequency: {reading['frequency']}Hz")
    print(f"PF: {reading['power_factor']:.2f}")
    print()  # Add empty line between readings


# ... existing generate_and_insert_data function ...
def generate_and_insert_data(device_ids, stop_event):
    print(f"[CRON] Starting data generation for devices: {device_ids}", file=sys.stderr)

    while not stop_event.is_set():
        # Generate data only for the specified device IDs
        readings = [generate_energy_reading(device_id) for device_id in device_ids]

        # Connect to the database
        conn = connect_to_db()
        if conn:
            try: # Add try block for database operations
                # Insert the generated data into the database
                insert_energy_readings(conn, readings)
            finally: # Ensure connection is closed even if insertion fails
                conn.close()
        else:
            # Changed message to be more specific
            print("[CRON] Failed to connect to the local database. Skipping insertion.", file=sys.stderr)

        # Output the generated data in the same format as the image
        # for reading in readings:
        #     print_reading_to_console(reading) # Optional: Comment out if console spam is too much

        # Wait for 1 second before next iteration
        # Use stop_event.wait for a more responsive stop
        stopped = stop_event.wait(timeout=1.0) # UPDATED wait method
        if stopped:
             print(f"[CRON] Stop signal received during wait for devices: {device_ids}", file=sys.stderr)
             break # Exit loop immediately if stopped during wait

        # Check if stop event is set (redundant after wait, but safe)
        if stop_event.is_set():
            print(f"[CRON] Stopping data generation for devices: {device_ids}", file=sys.stderr)
            break

# ... existing main function and _main_ block ...
def main(device_ids):
    if not device_ids:
        print("Error: No device IDs provided", file=sys.stderr)
        sys.exit(1)

    print(f"Starting data generation for devices: {', '.join(device_ids)}", file=sys.stderr)
    print("Press Ctrl+C to stop...", file=sys.stderr)

    stop_event = Event()

    try:
        generate_and_insert_data(device_ids, stop_event)
    except KeyboardInterrupt:
        print("\nStopping data generation...", file=sys.stderr)
        stop_event.set()

# Corrected the name check
if __name__ == "__main__":
    # Parse device IDs from command-line arguments
    device_ids = sys.argv[1:]
    main(device_ids)