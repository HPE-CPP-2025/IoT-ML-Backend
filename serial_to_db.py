import serial
import psycopg2
import time
import os
import sys
from dotenv import load_dotenv
from datetime import datetime

# --- Configuration ---
load_dotenv() # Load .env variables
SERIAL_PORT = os.getenv("SERIAL_PORT")
print(f"--- DEBUG: Read SERIAL_PORT = '{SERIAL_PORT}'---") # Keep this simple
BAUD_RATE = int(os.getenv("BAUD_RATE", 115200))
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
CONTROL_FILE_PATH = "arduino_control.txt"
CONTROL_CHECK_INTERVAL = 0.5

if not all([SERIAL_PORT, DB_NAME, DB_USER, DB_PASSWORD]):
    print("Error: Missing config in .env (SERIAL_PORT, DB_NAME, DB_USER, DB_PASSWORD)")
    sys.exit(1)

DB_CONN_STRING = f"host='{DB_HOST}' port='{DB_PORT}' dbname='{DB_NAME}' user='{DB_USER}' password='{DB_PASSWORD}'"
# --- End Configuration ---

def connect_db():
    try: conn = psycopg2.connect(DB_CONN_STRING); print("[DB] Connection established."); return conn
    except Exception as e: print(f"[DB] Error connecting: {e}"); return None

def insert_reading(conn, data_parts):
    """Inserts a single REAL reading into the energy_readings table (no is_simulated)."""
    # *** UPDATED SQL: Removed is_simulated column and placeholder ***
    sql = """
        INSERT INTO energy_readings
        (timestamp, device_id, voltage, current, power, energy, frequency, power_factor)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    cursor = None
    try:
        ts = datetime.strptime(data_parts[0], '%Y-%m-%d %H:%M:%S')
        # *** UPDATED VALUES: Removed the last boolean value for is_simulated ***
        values = (
            ts,                # timestamp (datetime object)
            data_parts[1],     # device_id (hex string - assuming this matches device_id type in DB)
            float(data_parts[2]), # voltage
            float(data_parts[3]), # current
            float(data_parts[4]), # power
            float(data_parts[5]), # energy
            float(data_parts[6]), # frequency
            float(data_parts[7])  # power_factor
        )
        cursor = conn.cursor(); cursor.execute(sql, values); conn.commit()
        # print(f"[DB Insert OK] Real data for device {data_parts[1]} at {ts}") # Optional confirmation
    except psycopg2.Error as db_err: # Catch specific DB errors
        print(f"[DB Insert/Conflict ERR] {db_err} | Data: {data_parts}")
        if conn: conn.rollback() # Rollback on error
    except (ValueError, IndexError) as parse_err: # Catch data parsing errors
        print(f"[Data Parse ERR] {parse_err} | Raw Line: {','.join(data_parts)}")
        # No rollback needed if commit didn't happen
    except Exception as e: # Catch any other unexpected errors
        print(f"[DB Insert UNK ERR] {e} | Data: {data_parts}")
        if conn: conn.rollback()
    finally:
        if cursor: cursor.close()

# --- Functions: send_arduino_command, check_control_file, is_data_line remain the same ---
def send_arduino_command(ser, command: bytes, current_state: str | None) -> str | None:
    if not ser or not ser.is_open: print("[Serial ERR] Port not open."); return current_state
    if command not in [b'O', b'F']: print(f"[CMD Warn] Invalid command '{command.decode()}'"); return current_state
    new_state_char = command.decode()
    if new_state_char == current_state: return current_state # No change
    try: ser.write(command); print(f"[Serial CMD] Sent '{new_state_char}' to Arduino."); return new_state_char
    except Exception as e: print(f"[Serial ERR] Sending '{new_state_char}': {e}"); return current_state

def check_control_file(ser, last_command_sent: str | None) -> str | None:
    new_command_state = last_command_sent
    try:
        with open(CONTROL_FILE_PATH, "r") as f: command_from_file = f.read().strip().upper()
        if command_from_file == "ON": new_command_state = send_arduino_command(ser, b'O', last_command_sent)
        elif command_from_file == "OFF": new_command_state = send_arduino_command(ser, b'F', last_command_sent)
    except FileNotFoundError: pass
    except Exception as e: print(f"[File ERR] Reading {CONTROL_FILE_PATH}: {e}")
    return new_command_state

def is_data_line(line: str) -> bool:
    if not line or line.startswith("#"): return False
    lower_line = line.lower()
    if lower_line.startswith("error:") or "turned on" in lower_line or "turned off" in lower_line or "status:" in lower_line or "ready" in lower_line or "detected at address" in lower_line or "commands:" in lower_line or "already on" in lower_line or "already off" in lower_line or "pzem004t" in lower_line or "initial time set" in lower_line: return False
    if line.count(',') != 7: return False
    try: parts = line.split(','); datetime.strptime(parts[0], '%Y-%m-%d %H:%M:%S'); [float(p) for p in parts[2:]]; return True
    except: return False
# ------------------------------------------------------------------------------------

def main():
    """Main loop: Read serial, check control file, insert real data."""
    ser = None; db_conn = None
    last_command_sent = None; last_control_check_time = 0
    print("[SYS] Starting Serial-to-Postgres Controller...")

    while True:
        # --- Connect/Reconnect ---
        if ser is None or not ser.is_open:
            try:
                # Ensure SERIAL_PORT is stripped of potential whitespace
                port_to_use = SERIAL_PORT.strip() if SERIAL_PORT else None
                if not port_to_use:
                    print("[Serial ERR] SERIAL_PORT is not defined or empty. Retrying..."); time.sleep(5); continue
                print(f"[Serial] Connecting to {port_to_use}...");
                ser = serial.Serial(port_to_use, BAUD_RATE, timeout=1);
                print("[Serial] Connected."); time.sleep(2); ser.flushInput()
            except serial.SerialException as e:
                print(f"[Serial ERR] {e}. Retrying..."); ser = None; time.sleep(5); continue
            except Exception as e: # Catch other potential errors like AttributeError if SERIAL_PORT is None
                print(f"[Serial ERR] Unexpected error during connection: {e}. Retrying..."); ser = None; time.sleep(5); continue
        if db_conn is None or db_conn.closed != 0:
            db_conn = connect_db()
            if db_conn is None: print("[DB] Connection failed. Retrying..."); time.sleep(5); continue

        current_time = time.monotonic()
        # --- Check Control File ---
        if current_time - last_control_check_time >= CONTROL_CHECK_INTERVAL:
            last_command_sent = check_control_file(ser, last_command_sent)
            last_control_check_time = current_time

        # --- Read Serial Data ---
        try:
            if ser.in_waiting > 0:
                line_bytes = ser.readline()
                try: line_str = line_bytes.decode('utf-8').strip()
                except UnicodeDecodeError: continue
                if not line_str: continue
                if is_data_line(line_str): insert_reading(db_conn, line_str.split(','))
                # else: print(f"[Serial Ignore] {line_str}") # Optional debug
            else: time.sleep(0.01)
        # --- Error Handling ---
        except serial.SerialException as e: print(f"[Serial ERR] {e}"); ser.close(); ser = None; time.sleep(5)
        except psycopg2.Error as e: print(f"[DB ERR] {e}"); db_conn.close(); db_conn = None; time.sleep(5) # Attempt reconnect on DB error
        except KeyboardInterrupt: print("\n[SYS] Shutdown signal received."); break
        except Exception as e: print(f"[SYS ERR] Unexpected: {e}"); import traceback; traceback.print_exc(); time.sleep(2)

    # --- Cleanup ---
    print("[SYS] Cleaning up...")
    if ser and ser.is_open: ser.close(); print("[Serial] Port closed.")
    if db_conn and db_conn.closed == 0: db_conn.close(); print("[DB] Connection closed.")
    try:
        os.remove(CONTROL_FILE_PATH) # Clean control file
    except OSError:
        pass
    print("[SYS] Script finished.")

# Corrected the name check
if __name__ == "__main__":
    main()