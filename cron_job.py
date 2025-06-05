import json
from datetime import datetime
import random
import psycopg2
import time
import os
from threading import Event
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Base values for realistic simulation
BASE_VOLTAGE = 230.0
BASE_CURRENT = 0.5
BASE_POWER = 100.0

def generate_energy_reading(device_id, house_id):
    """Generate simulated energy reading."""
    return {
        "device_id": device_id,
        "house_id": house_id,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "voltage": round(BASE_VOLTAGE + random.uniform(-2, 2), 2),
        "current": round(BASE_CURRENT + random.uniform(-0.1, 0.1), 2),
        "power": round(BASE_POWER + random.uniform(-10, 10), 2),
        "energy": round(random.uniform(0.001, 0.05), 3),
        "frequency": round(50.0 + random.uniform(-0.1, 0.1), 1),
        "power_factor": round(random.uniform(0.85, 1.00), 2),
    }

def insert_data(conn, reading):
    """Insert reading into database."""
    sql = """
        INSERT INTO energy_readings 
        (device_id, house_id, timestamp, voltage, current, power, energy, frequency, power_factor)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    cursor = None
    try:
        cursor = conn.cursor()
        values = (
            reading["device_id"],
            reading["house_id"],
            reading["timestamp"],
            reading["voltage"],
            reading["current"],
            reading["power"],
            reading["energy"],
            reading["frequency"],
            reading["power_factor"]
        )
        cursor.execute(sql, values)
        conn.commit()
    except psycopg2.Error as db_err:
        print(f"[CRON DB ERR] {db_err}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"[CRON ERR] {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()

def generate_and_insert_data(device_ids_list, stop_event):
    """Generate and insert data until stop signal."""
    conn = None
    try:
        conn_str = f"host='{DB_HOST}' port='{DB_PORT}' dbname='{DB_NAME}' user='{DB_USER}' password='{DB_PASSWORD}'"
        conn = psycopg2.connect(conn_str)
        print("[CRON] Database connected for simulation job")

        while not stop_event.is_set():
            for device_id_str in device_ids_list:
                try:
                    device_id = int(device_id_str)
                    default_house_id = 1
                    reading = generate_energy_reading(device_id, default_house_id)
                    insert_data(conn, reading)
                except ValueError:
                    print(f"[CRON ERR] Invalid device_id: {device_id_str}")
                except Exception as e:
                    print(f"[CRON ERR] Error processing device {device_id_str}: {e}")
            
            time.sleep(1)  # Generate data every second

    except psycopg2.Error as e:
        print(f"[CRON DB Connection ERR] {e}")
    except Exception as e:
        print(f"[CRON SYS ERR] {e}")
    finally:
        if conn:
            conn.close()
            print("[CRON] Database connection closed")
        print(f"[CRON] Stopped data generation for devices: {device_ids_list}")

if __name__ == "__main__":
    # Test mode
    print("Testing cron_job.py...")
    
    class MockStopEvent:
        def __init__(self):
            self._flag = False
        def is_set(self):
            return self._flag
        def set(self):
            self._flag = True

    test_device_ids = ["10", "11"]
    stop_event_test = MockStopEvent()
    
    import threading
    cron_thread = threading.Thread(
        target=generate_and_insert_data,
        args=(test_device_ids, stop_event_test)
    )
    cron_thread.start()
    
    time.sleep(5)
    stop_event_test.set()
    cron_thread.join()
    print("Test completed.")