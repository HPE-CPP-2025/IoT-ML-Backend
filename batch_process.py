import psycopg2
import psycopg2.extras
import time
import os
import sys
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
LOCAL_DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

NEON_DB_URL = os.getenv('NEON_DB_URL')

# Check if environment variables are loaded
if not all(LOCAL_DB_CONFIG.values()) or not NEON_DB_URL:
    print("Error: Database configuration missing in .env file.", file=sys.stderr)
    sys.exit(1)

SOURCE_TABLE = 'energy_readings'
# *** Destination table is now the same as the source table name ***
DEST_TABLE = 'energy_readings'
BATCH_SIZE = 1000              # Number of aggregated rows to process per batch
RUN_INTERVAL_SECONDS = 300
# --- Main Logic ---
def transfer_data():
    local_conn = None
    neon_conn = None
    local_cur = None
    neon_cur = None
    processed_rows = 0
    total_rows_fetched = 0
    # Define target columns explicitly for clarity and mapping
    target_cols = [
        "timestamp", "device_id", "house_id", "voltage", "current", # Added house_id
        "power", "energy", "frequency", "power_factor"
    ]
    # Define source aliases from the aggregation query in the corresponding order
    source_aliases = [
        "minute_timestamp", "device_id", "house_id", "avg_voltage", "avg_current", # Added house_id
        "avg_power", "avg_energy", "avg_frequency", "avg_power_factor"
    ]
    # Identify aliases corresponding to float values that need rounding
    float_aliases = {
        "avg_voltage", "avg_current", "avg_power",
        "avg_energy", "avg_frequency", "avg_power_factor"
    }
    insert_sql = "" # Define insert_sql outside the loop scope
    valid_neon_device_ids = set() # Initialize an empty set for valid device IDs

    try:
        print("Connecting to databases...")
        # Connect to local database
        local_conn = psycopg2.connect(**LOCAL_DB_CONFIG)
        # Use a server-side cursor for efficient batch fetching from large tables
        local_cur = local_conn.cursor(name='fetch_data_cursor', cursor_factory=psycopg2.extras.DictCursor)
        local_cur.itersize = BATCH_SIZE # Controls rows fetched from backend per network roundtrip

        # Connect to Neon database
        neon_conn = psycopg2.connect(NEON_DB_URL)
        neon_cur = neon_conn.cursor()
        print("Connections established.")

        # Fetch valid device IDs from Neon
        try:
            print("Fetching valid device_ids from Neon's 'devices' table...")
            neon_cur.execute("SELECT device_id FROM devices;")
            fetched_device_ids = neon_cur.fetchall()
            valid_neon_device_ids = {row[0] for row in fetched_device_ids}
            print(f"Found {len(valid_neon_device_ids)} valid device_ids in Neon database.")
        except Exception as e:
            print(f"Error fetching device_ids: {e}", file=sys.stderr)

        # Aggregate data by minute
        aggregation_sql = f"""
            SELECT
                DATE_TRUNC('minute', timestamp) AS minute_timestamp,
                device_id,
                house_id,
                AVG(voltage) AS avg_voltage,
                AVG(current) AS avg_current,
                AVG(power) AS avg_power,
                AVG(energy) AS avg_energy,
                AVG(frequency) AS avg_frequency,
                AVG(power_factor) AS avg_power_factor
            FROM {SOURCE_TABLE}
            GROUP BY minute_timestamp, device_id, house_id
            ORDER BY minute_timestamp, device_id, house_id
        """
        
        local_cur.execute(aggregation_sql)
        batch = local_cur.fetchmany(BATCH_SIZE)
        total_rows_fetched += len(batch)

        if not batch:
            print("No data found after aggregation.")
            return

        # Prepare INSERT statement with conflict resolution
        cols_sql = ', '.join(f'"{col}"' for col in target_cols)
        placeholders = ', '.join(['%s'] * len(target_cols))
        
        conflict_target_sql = '"device_id", "timestamp"'
        update_cols = [f'"{col}" = EXCLUDED."{col}"' for col in target_cols if col not in ["device_id", "timestamp"]]
        update_sql = ', '.join(update_cols)
        
        insert_sql = f"""
            INSERT INTO {DEST_TABLE} ({cols_sql}) 
            VALUES ({placeholders})
            ON CONFLICT ({conflict_target_sql}) DO UPDATE SET {update_sql}
        """

        while batch:
            data_to_insert = []
            skipped_count = 0
            
            for row in batch:
                current_device_id = row['device_id']
                
                if valid_neon_device_ids and current_device_id not in valid_neon_device_ids:
                    skipped_count += 1
                    continue

                row_data = []
                for alias in source_aliases:
                    value = row[alias]
                    if alias in float_aliases and value is not None:
                        row_data.append(round(float(value), 2))
                    elif alias == "house_id" and value is not None:
                        row_data.append(int(value))
                    else:
                        row_data.append(value)
                data_to_insert.append(tuple(row_data))

            if data_to_insert:
                psycopg2.extras.execute_batch(neon_cur, insert_sql, data_to_insert, page_size=BATCH_SIZE)
                neon_conn.commit()
            
            processed_rows += len(data_to_insert)
            batch = local_cur.fetchmany(BATCH_SIZE)
            total_rows_fetched += len(batch)

        print(f"Transfer completed. Rows fetched: {total_rows_fetched}, processed: {processed_rows}")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error during data transfer: {error}", file=sys.stderr)
        if neon_conn:
            neon_conn.rollback()
    finally:
        for resource in [local_cur, local_conn, neon_cur, neon_conn]:
            if resource:
                resource.close()

if __name__ == "__main__":
    while True:
        print(f"Starting transfer cycle at {time.ctime()}...")
        try:
            transfer_data()
            print(f"Cycle finished. Waiting {RUN_INTERVAL_SECONDS} seconds...")
        except Exception as e:
            print(f"Error in transfer cycle: {e}", file=sys.stderr)
        time.sleep(RUN_INTERVAL_SECONDS)