import psycopg2
import psycopg2.extras
import time
import os
import sys
from dotenv import load_dotenv # Import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
# Local PostgreSQL connection details from environment variables
LOCAL_DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

# Neon DB Connection URL from environment variable
NEON_DB_URL = os.getenv('NEON_DB_URL')

# Check if environment variables are loaded
if not all(LOCAL_DB_CONFIG.values()) or not NEON_DB_URL:
    print("Error: Database configuration missing in .env file.", file=sys.stderr)
    print("Ensure DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, and NEON_DB_URL are set.", file=sys.stderr)
    sys.exit(1) # Exit if configuration is incomplete

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
        "timestamp", "device_id", "voltage", "current",
        "power", "energy", "frequency", "power_factor"
    ]
    # Define source aliases from the aggregation query in the corresponding order
    source_aliases = [
        "minute_timestamp", "device_id", "avg_voltage", "avg_current",
        "avg_power", "avg_energy", "avg_frequency", "avg_power_factor"
    ]
    # Identify aliases corresponding to float values that need rounding
    float_aliases = {
        "avg_voltage", "avg_current", "avg_power",
        "avg_energy", "avg_frequency", "avg_power_factor"
    }
    insert_sql = "" # Define insert_sql outside the loop scope

    try:
        # Connect to local database
        print("Connecting to local PostgreSQL database...")
        local_conn = psycopg2.connect(**LOCAL_DB_CONFIG)
        # Use a server-side cursor for efficient batch fetching from large tables
        local_cur = local_conn.cursor(name='fetch_data_cursor', cursor_factory=psycopg2.extras.DictCursor)
        local_cur.itersize = BATCH_SIZE # Controls rows fetched from backend per network roundtrip

        # Connect to Neon database
        print("Connecting to Neon database...")
        neon_conn = psycopg2.connect(NEON_DB_URL)
        neon_cur = neon_conn.cursor()
        print("Connections established.")

        # Check if destination table exists and optionally clear it
        # print(f"Checking destination table '{DEST_TABLE}'...")
        # Consider if TRUNCATE is appropriate if you're replacing data entirely
        # neon_cur.execute(f"TRUNCATE TABLE {DEST_TABLE};")
        # print(f"Destination table '{DEST_TABLE}' truncated.")
        # neon_conn.commit()

        # Fetch aggregated data from local table
        print(f"Fetching aggregated data (by minute) from local table '{SOURCE_TABLE}'...")

        # --- AGGREGATION QUERY (Outputs aliases like avg_voltage) ---
        aggregation_sql = f"""
            SELECT
                DATE_TRUNC('minute', timestamp) AS minute_timestamp,
                device_id,
                AVG(voltage) AS avg_voltage,
                AVG(current) AS avg_current,
                AVG(power) AS avg_power,
                AVG(energy) AS avg_energy, -- Or SUM(energy) if appropriate
                AVG(frequency) AS avg_frequency,
                AVG(power_factor) AS avg_power_factor
            FROM {SOURCE_TABLE}
            GROUP BY minute_timestamp, device_id
            ORDER BY minute_timestamp, device_id
        """
        print(f"Executing aggregation query:\n{aggregation_sql}")
        local_cur.execute(aggregation_sql)
        # --- END AGGREGATION QUERY ---

        # Fetch the first batch
        print(f"\nFetching first batch of up to {BATCH_SIZE} aggregated rows...")
        batch = local_cur.fetchmany(BATCH_SIZE)
        total_rows_fetched += len(batch)

        if not batch:
            print("No data found after aggregation.")
        else:
            # --- Insertion Logic for Aggregated Data into Original Table Structure ---
            print(f"Fetched {len(batch)} aggregated rows. Preparing for insertion into '{DEST_TABLE}'...")

            # Build INSERT statement mapping aggregated aliases to target columns
            # Use explicit target column names
            cols_sql = ', '.join(f'"{col}"' for col in target_cols)
            placeholders = ', '.join(['%s'] * len(target_cols))

            # Build the ON CONFLICT clause to update existing rows
            # Assumes primary key is (device_id, timestamp)
            update_cols = [f'"{col}" = EXCLUDED."{col}"' for col in target_cols if col not in ('device_id', 'timestamp')]
            update_sql = ', '.join(update_cols)
            # Insert into the original table name, map columns, handle conflicts
            insert_sql = f"""
                INSERT INTO {DEST_TABLE} ({cols_sql})
                VALUES ({placeholders})
                ON CONFLICT (device_id, timestamp) DO UPDATE SET {update_sql}
            """
            print(f"Using INSERT SQL for aggregated data:\n{insert_sql}")

            while batch:
                # Convert batch data: Fetch values using source_aliases order
                # Round float values to 2 decimal places
                data_to_insert = []
                for row in batch:
                    row_data = []
                    for alias in source_aliases:
                        value = row[alias]
                        # Check if the alias is one that needs rounding and value is not None
                        if alias in float_aliases and value is not None:
                            try:
                                # Round to 2 decimal places
                                row_data.append(round(float(value), 2))
                            except (ValueError, TypeError):
                                # Handle cases where conversion to float might fail unexpectedly
                                print(f"Warning: Could not round value for {alias}: {value}. Inserting as is.", file=sys.stderr)
                                row_data.append(value)
                        else:
                            # Keep original value (timestamp, device_id, or None)
                            row_data.append(value)
                    data_to_insert.append(tuple(row_data))


                print(f"Inserting/Updating {len(data_to_insert)} aggregated rows into Neon table '{DEST_TABLE}'...")
                # Use execute_batch
                psycopg2.extras.execute_batch(neon_cur, insert_sql, data_to_insert, page_size=BATCH_SIZE)

                # Commit the transaction for this batch in Neon DB
                neon_conn.commit()
                processed_rows += len(batch)
                print(f"Aggregated batch inserted/updated and committed. Total aggregated rows processed so far: {processed_rows}")

                # Fetch the next batch
                print(f"\nFetching next batch of up to {BATCH_SIZE} aggregated rows...")
                batch = local_cur.fetchmany(BATCH_SIZE)
                total_rows_fetched += len(batch)
                if not batch:
                    print("No more aggregated data found.")
                    break
                print(f"Fetched {len(batch)} aggregated rows.")

        print(f"\nData transfer completed. Total aggregated rows fetched: {total_rows_fetched}. Total aggregated rows processed: {processed_rows}.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"\nError during data transfer: {error}", file=sys.stderr)
        if neon_conn:
            print("Rolling back Neon transaction due to error...")
            neon_conn.rollback()
    finally:
        # Close cursors and connections
        if local_cur:
            local_cur.close()
            print("Closed local cursor.")
        if local_conn:
            local_conn.close()
            print("Closed local connection.")
        if neon_cur:
            neon_cur.close()
            print("Closed Neon cursor.")
        if neon_conn:
            neon_conn.close()
            print("Closed Neon connection.")

if __name__ == "__main__":
    while True:
        print(f"Starting data transfer cycle at {time.ctime()}...")
        try:
            transfer_data()
            print(f"Data transfer cycle finished. Waiting for {RUN_INTERVAL_SECONDS} seconds...")
        except Exception as e:
            print(f"An error occurred in the transfer cycle: {e}", file=sys.stderr)
            # Decide if you want to wait or exit on error
        time.sleep(RUN_INTERVAL_SECONDS)