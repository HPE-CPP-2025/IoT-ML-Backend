import serial
import psycopg2
import time
import os
import sys
from dotenv import load_dotenv
from datetime import datetime
from typing import Union

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
    try:
        conn = psycopg2.connect(DB_CONN_STRING)
        print("[DB] Connection established.")
        return conn
    except Exception as e:
        print(f"[DB] Error connecting: {e}")
        return None

def insert_reading(conn, data_parts):
    """Insert energy reading into database."""
    sql = """
        INSERT INTO energy_readings
        (timestamp, device_id, house_id, voltage, current, power, energy, frequency, power_factor)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    cursor = None
    try:
        ts = datetime.strptime(data_parts[0], '%Y-%m-%d %H:%M:%S')
        default_house_id = 1
        
        values = (
            ts,
            data_parts[1],
            default_house_id,
            float(data_parts[2]),
            float(data_parts[3]),
            float(data_parts[4]),
            float(data_parts[5]),
            float(data_parts[6]),
            float(data_parts[7])
        )
        
        cursor = conn.cursor()
        cursor.execute(sql, values)
        conn.commit()
    except psycopg2.Error as db_err:
        print(f"[DB Insert ERR] {db_err}")
        if conn:
            conn.rollback()
    except (ValueError, IndexError) as parse_err:
        print(f"[Data Parse ERR] {parse_err}")
    finally:
        if cursor:
            cursor.close()

def send_arduino_command(ser, command: bytes, current_state: Union[str, None]) -> Union[str, None]:
    if not ser or not ser.is_open:
        return current_state
    if command not in [b'O', b'F']:
        return current_state
    
    new_state_char = command.decode()
    if new_state_char == current_state:
        return current_state
    
    try:
        ser.write(command)
        print(f"[Serial CMD] Sent '{new_state_char}' to Arduino.")
        return new_state_char
    except Exception as e:
        print(f"[Serial ERR] Sending '{new_state_char}': {e}")
        return current_state

def check_control_file(ser, last_command_sent: Union[str, None]) -> Union[str, None]:
    try:
        with open(CONTROL_FILE_PATH, "r") as f:
            command_from_file = f.read().strip().upper()
        
        if command_from_file == "ON":
            return send_arduino_command(ser, b'O', last_command_sent)
        elif command_from_file == "OFF":
            return send_arduino_command(ser, b'F', last_command_sent)
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"[File ERR] Reading {CONTROL_FILE_PATH}: {e}")
    
    return last_command_sent

def is_data_line(line: str) -> bool:
    """Check if line contains valid energy data."""
    if not line or line.startswith("#"):
        return False
    
    lower_line = line.lower()
    skip_keywords = [
        "error:", "turned on", "turned off", "status:", "ready",
        "detected at address", "commands:", "already on", "already off",
        "pzem004t", "initial time set"
    ]
    
    if any(keyword in lower_line for keyword in skip_keywords):
        return False
    
    if line.count(',') != 7:
        return False
    
    try:
        parts = line.split(',')
        datetime.strptime(parts[0], '%Y-%m-%d %H:%M:%S')
        [float(p) for p in parts[2:]]
        return True
    except:
        return False

def main():
    """Main loop: Read serial data and control Arduino."""
    ser = None
    db_conn = None
    last_command_sent = None
    last_control_check_time = 0
    
    print("[SYS] Starting Serial-to-Postgres Controller...")

    while True:
        # Connect/Reconnect serial
        if ser is None or not ser.is_open:
            try:
                port_to_use = SERIAL_PORT.strip()
                if not port_to_use:
                    print("[Serial ERR] SERIAL_PORT not defined. Retrying...")
                    time.sleep(5)
                    continue
                
                ser = serial.Serial(port_to_use, BAUD_RATE, timeout=1)
                print(f"[Serial] Connected to {port_to_use}")
                time.sleep(2)
                ser.flushInput()
            except Exception as e:
                print(f"[Serial ERR] {e}. Retrying...")
                ser = None
                time.sleep(5)
                continue

        # Connect/Reconnect database
        if db_conn is None or db_conn.closed != 0:
            db_conn = connect_db()
            if db_conn is None:
                time.sleep(5)
                continue

        current_time = time.monotonic()
        
        # Check control file periodically
        if current_time - last_control_check_time >= CONTROL_CHECK_INTERVAL:
            last_command_sent = check_control_file(ser, last_command_sent)
            last_control_check_time = current_time

        # Read serial data
        try:
            if ser.in_waiting > 0:
                line_bytes = ser.readline()
                try:
                    line_str = line_bytes.decode('utf-8').strip()
                except UnicodeDecodeError:
                    continue
                
                if line_str and is_data_line(line_str):
                    insert_reading(db_conn, line_str.split(','))
            else:
                time.sleep(0.01)
                
        except serial.SerialException as e:
            print(f"[Serial ERR] {e}")
            ser.close()
            ser = None
            time.sleep(5)
        except psycopg2.Error as e:
            print(f"[DB ERR] {e}")
            db_conn.close()
            db_conn = None
            time.sleep(5)
        except KeyboardInterrupt:
            print("\n[SYS] Shutdown signal received.")
            break
        except Exception as e:
            print(f"[SYS ERR] Unexpected: {e}")
            time.sleep(2)

    # Cleanup
    print("[SYS] Cleaning up...")
    if ser and ser.is_open:
        ser.close()
    if db_conn and db_conn.closed == 0:
        db_conn.close()
    try:
        os.remove(CONTROL_FILE_PATH)
    except OSError:
        pass
    print("[SYS] Script finished.")

if __name__ == "__main__":
    main()