from fastapi import FastAPI, HTTPException, Request, Header
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
# Ensure cron_job.py is importable (in the same directory or Python path)
from cron_job import generate_and_insert_data
from datetime import datetime
import os
from dotenv import load_dotenv
import logging
import httpx
import json
from sse_starlette.sse import EventSourceResponse
import asyncio
import threading

# ======================
# Configuration
# ======================
load_dotenv()

app = FastAPI(
    title="Energy Optimization Backend",
    description="Handles SSE, controls Arduino via file, manages simulated cron jobs to local DB",
    version="13.2", # Incremented version
    docs_url="/docs",
    redoc_url=None
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Adjust for production
    allow_methods=["*"],
    allow_headers=["*"],
)

# ======================
# Constants
# ======================
API_KEY = os.getenv("API_KEY", "EnergyOpt_50rBIeCMvy1u_AjpyB7qnTUpVxSQWzz1LVgVlUizJeg")
PRODUCTION_URL = os.getenv("PRODUCTION_URL", "https://energy-optimisation-backend.onrender.com")
# Corrected SSE_ENDPOINT to use a specific house ID (e.g., 1)
SSE_ENDPOINT = f"{PRODUCTION_URL}/api/device-status/house/1/subscribe"
TOGGLE_ENDPOINT = f"{PRODUCTION_URL}/api/device-status/{{deviceId}}/toggle"
ARDUINO_DEVICE_ID = "1" # Device ID for the physical Arduino bulb
CONTROL_FILE_PATH = "arduino_control.txt" # File for signaling serial script

# ======================
# Setup
# ======================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[ logging.StreamHandler(), logging.FileHandler("energy_backend.log") ]
)
# Corrected the name used for the logger
logger = logging.getLogger(__name__)

# ======================
# State Management
# ======================
active_cron_jobs = {} # Tracks simulated data jobs {device_id: {thread, stop_event, ...}}
last_arduino_state = None # Tracks 'on' or 'off' state for device ARDUINO_DEVICE_ID
sse_background_task = None # Will hold the background task for SSE subscription

# ======================
# Control & Cron Functions
# ======================
def write_control_file(state: str):
    """Writes 'ON' or 'OFF' to the control file."""
    try:
        with open(CONTROL_FILE_PATH, "w") as f: f.write(state.upper())
        logger.info(f"[CONTROL] Wrote '{state.upper()}' to {CONTROL_FILE_PATH}")
    except IOError as e: logger.error(f"[CONTROL] Error writing control file {CONTROL_FILE_PATH}: {e}")

def run_cron_job(device_id, stop_event):
    """Starts generate_and_insert_data in a thread for simulated devices."""
    try:
        device_ids_list = [str(device_id)] # Pass as a list
        thread = threading.Thread(target=generate_and_insert_data, args=(device_ids_list, stop_event), daemon=True)
        thread.start()
        logger.info(f"[CRON] Thread started for simulated device {device_id}")
        return thread
    except Exception as e: logger.error(f"[CRON] Error starting thread for device {device_id}: {e}"); return None

# ...existing code...
def process_device_status(devices):
    """Processes SSE device list: writes control file for Arduino, manages cron for others."""
    global last_arduino_state
    changes_summary = []

    for device in devices:
        device_id = str(device.get("deviceId"))
        status = device.get("on") # True (ON) or False (OFF)
        device_name = device.get("deviceName", f"Device {device_id}")

        # --- Handle Physical Arduino Control ---
        if device_id == ARDUINO_DEVICE_ID:
            current_state_str = "on" if status else "off"
            if current_state_str != last_arduino_state:
                logger.info(f"[CONTROL] State change for Arduino ({device_name}): {last_arduino_state} -> {current_state_str}")
                write_control_file("ON" if status else "OFF")
                last_arduino_state = current_state_str
                changes_summary.append(f"Arduino ({device_name}) state changed to {current_state_str.upper()}")
            continue # Don't process cron jobs for the Arduino device

        # --- Handle Simulated Device Cron Jobs ---
        # Only manage cron jobs for devices other than the Arduino
        if status and device_id not in active_cron_jobs:
            # Start cron job for simulated device turned ON
            logger.info(f"[CRON] Starting simulated job for {device_name} (ID: {device_id})")
            stop_event = threading.Event()
            thread = run_cron_job(device_id, stop_event)
            if thread:
                active_cron_jobs[device_id] = {"thread": thread, "stop_event": stop_event, "started_at": datetime.now().isoformat(), "device_name": device_name}
                changes_summary.append(f"Started sim job for {device_name}")
                logger.info(f"[CRON] Started sim job for {device_name}")

        elif not status and device_id in active_cron_jobs:
            # Stop cron job for simulated device turned OFF
            logger.info(f"[CRON] Stopping simulated job for {device_name} (ID: {device_id})")
            job_info = active_cron_jobs.get(device_id) # Use get for safety
            if job_info and job_info["thread"].is_alive():
                job_info["stop_event"].set()
                changes_summary.append(f"Stopped sim job for {job_info.get('device_name', device_name)}")
                # Consider joining threads on shutdown or periodically cleaning up finished ones
                # del active_cron_jobs[device_id] # Removing immediately might hide recently stopped jobs in /active-jobs
                logger.info(f"[CRON] Stop signal sent to sim job for {device_name}")
                # *** Remove the entry immediately after stopping ***
                del active_cron_jobs[device_id]
                logger.info(f"[CRON] Removed job entry for device {device_id}")
            elif job_info:
                 # Job exists but thread isn't alive, maybe already stopped
                 logger.info(f"[CRON] Job for {device_name} found but already stopped.")
                 # *** Clean up entry if it wasn't running ***
                 del active_cron_jobs[device_id] # Clean up entry if desired
                 logger.info(f"[CRON] Removed stale job entry for device {device_id}")
            else:
                 # This case should ideally not happen if device_id is in active_cron_jobs
                 logger.warning(f"[CRON] Received OFF for device {device_id}, but no active job found (though key existed).")
                 # *** Clean up entry just in case ***
                 if device_id in active_cron_jobs:
                     del active_cron_jobs[device_id]


    return changes_summary
# ...existing code...

# ======================
# SSE Background Task
# ======================
async def sse_background_listener():
    """Background task to connect to SSE and process events without requiring endpoint visits."""
    global last_arduino_state
    logger.info("[SSE BG] Starting background SSE connection listener...")
    
    # Set initial control state to OFF on connect
    try: 
        write_control_file("OFF")
        last_arduino_state = "off"
        logger.info("[CONTROL] Initialized control file to OFF.")
    except Exception as e: 
        logger.warning(f"Could not initialize control file: {e}")

    while True:  # Loop to allow reconnection attempts
        try:
            target_sse_url = SSE_ENDPOINT
            logger.info(f"[SSE BG] Attempting to connect to: {target_sse_url}")
            
            async with httpx.AsyncClient(timeout=None) as client:  # No timeout for streaming
                async with client.stream("GET", target_sse_url, headers={"x-api-key": API_KEY}) as response:
                    if response.status_code != 200:
                        logger.error(f"[SSE BG] Connection failed with status {response.status_code}. Retrying in 10s.")
                        try:
                            error_body = await response.aread()
                            logger.error(f"[SSE BG] Error response body: {error_body.decode()}")
                        except Exception as read_err:
                            logger.error(f"[SSE BG] Could not read error response body: {read_err}")
                        await asyncio.sleep(10)
                        continue  # Retry connection

                    logger.info(f"[SSE BG] Connected successfully to {target_sse_url}")
                    logger.info("[SSE BG] Connection is proper. Listening for events...")
                    
                    line_count = 0
                    async for line in response.aiter_lines():
                        line_count += 1
                        logger.debug(f"[SSE BG RAW Line {line_count}] Received: '{line}'")

                        if not line.strip():
                            logger.debug(f"[SSE BG Keep-Alive] Received empty line {line_count}.")
                            continue

                        if line.startswith("event:"):
                            logger.debug(f"[SSE BG Event Type] Line {line_count}: {line}")
                            continue

                        if line.startswith("data:"):
                            logger.debug(f"[SSE BG Data Line] Line {line_count}: Processing data...")
                            try:
                                raw_data = line[5:].strip()
                                devices_data = json.loads(raw_data)
                                if isinstance(devices_data, list):
                                    logger.info(f"[SSE BG DATA] Received {len(devices_data)} device states.")
                                    changes = process_device_status(devices_data)
                                    if changes: 
                                        logger.info(f"[SSE BG Processed] Actions: {'; '.join(changes)}")
                                    else: 
                                        logger.info("[SSE BG Processed] No actions taken for received data.")
                                else: 
                                    logger.warning(f"[SSE BG DATA] Expected list, got {type(devices_data)}")
                            except json.JSONDecodeError as e: 
                                logger.error(f"[SSE BG ERROR] JSON decode: {e}. Data: {raw_data[:100]}...")
                            except Exception as e: 
                                logger.error(f"[SSE BG ERROR] Processing failed: {e}", exc_info=True)

                    # If the loop finishes without error (server closed connection gracefully)
                    logger.warning("[SSE BG] Stream finished. Server might have closed connection. Attempting reconnect...")

        except httpx.RequestError as e:
            logger.error(f"[SSE BG ERROR] Connection error: {e}. Is the server reachable? Retrying in 10s.")
            await asyncio.sleep(10)
        except Exception as e:
            logger.error(f"[SSE BG ERROR] Unhandled exception in listener loop: {e}. Retrying in 10s.", exc_info=True)
            await asyncio.sleep(10)

# ======================
# API Endpoints
# ======================
@app.get("/sse-subscribe")
async def sse_subscribe(request: Request):
    """Endpoint to check SSE connection status and manually initiate if needed."""
    global sse_background_task
    
    if sse_background_task is None or sse_background_task.done():
        logger.warning("[SSE] Background task not running or completed. Status info will be returned.")
        
        # Return status only - background task will be started on app startup
        return {
            "status": "SSE background task not running or completed",
            "note": "The app will attempt to restart the SSE connection automatically.",
            "check": "View logs for connection details or check /health endpoint."
        }
    
    return {
        "status": "SSE background task running",
        "endpoint": SSE_ENDPOINT,
        "arduino_device_id": ARDUINO_DEVICE_ID,
        "last_arduino_state": last_arduino_state
    }


@app.post("/toggle-device/{device_id}")
async def toggle_device(device_id: str):
    """Proxy to toggle device in production AND update local control if it's the Arduino."""
    try:
        logger.info(f"[TOGGLE] Request to toggle device {device_id}")
        response = await make_authenticated_request(
            TOGGLE_ENDPOINT.format(deviceId=device_id), method="POST"
        )
        result = response.json() # Assuming production returns new state { "on": true/false, ... }
        logger.info(f"[TOGGLE] Response from production for {device_id}: {result}")

        # --- Update local control immediately on toggle ---
        if device_id == ARDUINO_DEVICE_ID:
             global last_arduino_state
             is_on = result.get("on", False)
             new_state_str = "on" if is_on else "off"
             if new_state_str != last_arduino_state:
                  logger.info(f"[CONTROL] Toggled Arduino {device_id} via API proxy. Updating control file to {new_state_str.upper()}.")
                  write_control_file(new_state_str.upper())
                  last_arduino_state = new_state_str
             else:
                  logger.info(f"[CONTROL] Toggle API for Arduino {device_id} resulted in same state ({new_state_str}). No control file change.")
        # -------------------------------------------------
        return result
    except Exception as e:
        logger.error(f"[TOGGLE] Failed for device {device_id}: {e}", exc_info=True)
        raise HTTPException(status_code=502, detail=f"Toggle request failed: {e}")


@app.get("/active-jobs")
async def get_active_jobs():
    """Lists active simulation cron jobs."""
    result = {}
    jobs_to_remove = []
    for device_id, job_info in active_cron_jobs.items(): # Iterate copy
        is_alive = job_info["thread"].is_alive()
        if not is_alive and not job_info.get("marked_for_removal"):
             logger.info(f"[CRON] Job thread for device {device_id} finished.")
             # Mark for removal after reporting it once as not running
             # active_cron_jobs[device_id]["marked_for_removal"] = True
             # Or remove immediately:
             jobs_to_remove.append(device_id)

        result[device_id] = {
            "device_name": job_info.get("device_name", "Unknown"),
            "started_at": job_info["started_at"],
            "running": is_alive,
            "elapsed_seconds": round((datetime.now() - datetime.fromisoformat(job_info["started_at"])).total_seconds())
        }

    # Clean up finished jobs
    for device_id in jobs_to_remove:
        del active_cron_jobs[device_id]
        logger.info(f"[CRON] Removed finished job entry for device {device_id}")

    return result


@app.post("/force-start-job/{device_id}")
async def force_start_job(device_id: str):
    """Manually start a simulation job (not for Arduino)."""
    if device_id == ARDUINO_DEVICE_ID: return {"status": "error", "message": "Cannot force start cron job for the physical Arduino device."}
    if device_id in active_cron_jobs and active_cron_jobs[device_id]["thread"].is_alive(): return {"status": "error", "message": f"Job for device {device_id} is already running"}
    logger.info(f"[CRON] Force starting job for device {device_id}")
    stop_event = threading.Event(); thread = run_cron_job(device_id, stop_event)
    if thread: active_cron_jobs[device_id] = {"thread": thread, "stop_event": stop_event, "started_at": datetime.now().isoformat(), "device_name": f"Device {device_id}"}; return {"status": "success", "message": f"Started job for device {device_id}"}
    else: return {"status": "error", "message": f"Failed to start job for device {device_id}"}


@app.post("/force-stop-job/{device_id}")
async def force_stop_job(device_id: str):
    """Manually stop a simulation job (not for Arduino)."""
    if device_id == ARDUINO_DEVICE_ID: return {"status": "error", "message": "Cannot force stop cron job for the physical Arduino device."}
    job_info = active_cron_jobs.get(device_id)
    if not job_info or not job_info["thread"].is_alive(): return {"status": "error", "message": f"No active job found for device {device_id}"}
    logger.info(f"[CRON] Force stopping job for device {device_id}")
    job_info["stop_event"].set();
    # Don't delete here, let /active-jobs handle cleanup after thread exits
    return {"status": "success", "message": f"Stop signal sent to job for device {device_id}"}


@app.get("/health")
async def health_check():
    """Provides health status including Arduino state and active simulation jobs."""
    global sse_background_task
    
    # Get current active jobs status without modifying the main dict
    active_jobs_list = []
    for d_id, j_info in active_cron_jobs.items():
         if j_info["thread"].is_alive(): # Only list truly running jobs
              active_jobs_list.append({"device_id": d_id, "device_name": j_info.get("device_name", "Unknown"), "running": True, "started_at": j_info["started_at"]})

    return {
        "status": "healthy", 
        "timestamp": datetime.now().isoformat(), 
        "production_url": PRODUCTION_URL, 
        "sse_endpoint": SSE_ENDPOINT, 
        "sse_background_task_running": sse_background_task is not None and not sse_background_task.done(),
        "arduino_device_id": ARDUINO_DEVICE_ID, 
        "last_arduino_state": last_arduino_state, 
        "control_file": CONTROL_FILE_PATH, 
        "active_sim_jobs_count": len(active_jobs_list), 
        "active_sim_jobs": active_jobs_list
    }


# ======================
# Helpers
# ======================
async def make_authenticated_request(url: str, method="GET", **kwargs):
    """Makes authenticated request to production backend with retries."""
    max_retries = 3; attempt_delay = 1
    last_exception = None
    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                headers = kwargs.pop("headers", {}); headers["x-api-key"] = API_KEY
                response = await client.request(method, url, headers=headers, **kwargs)
                response.raise_for_status() # Raise exception for 4xx/5xx
                return response
        except httpx.RequestError as e:
            logger.warning(f"Request attempt {attempt + 1} to {url} failed: {e}")
            last_exception = e
            await asyncio.sleep(attempt_delay)
            attempt_delay *= 2
        except httpx.HTTPStatusError as e:
            logger.warning(f"Request attempt {attempt + 1} to {url} failed: Status {e.response.status_code}")
            last_exception = e
            await asyncio.sleep(attempt_delay)
            attempt_delay *= 2
            if e.response.status_code < 500: # Don't retry client errors (4xx)
                break

    # If loop finishes without success
    detail = f"Request to {url} failed after {max_retries} attempts."
    if last_exception: detail += f" Last error: {last_exception}"
    raise HTTPException(status_code=502, detail=detail)


# ======================
# App Lifecycle Events
# ======================
@app.on_event("startup")
async def startup_event():
    """Starts the SSE background task on app startup."""
    global sse_background_task
    
    logger.info("====================================")
    logger.info(" Energy Optimization Backend Starting")
    logger.info(f" Auto-connecting to SSE from: {PRODUCTION_URL}")
    logger.info(f" Controlling Arduino (ID: {ARDUINO_DEVICE_ID}) via: {CONTROL_FILE_PATH}")
    logger.info(f" Managing simulated cron jobs (data to LOCAL DB)")
    logger.info("====================================")
    
    # Start background task
    logger.info("[STARTUP] Initializing background SSE connection...")
    sse_background_task = asyncio.create_task(sse_background_listener())
    logger.info("[STARTUP] Background SSE task created")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleans up simulation jobs and potentially resets Arduino state."""
    global sse_background_task
    
    logger.info("Shutting down: Stopping active simulation jobs...")
    
    # Cancel the SSE background task
    if sse_background_task:
        logger.info("[SHUTDOWN] Canceling SSE background task")
        sse_background_task.cancel()
        try:
            await sse_background_task
        except asyncio.CancelledError:
            logger.info("[SHUTDOWN] SSE background task canceled successfully")
        except Exception as e:
            logger.error(f"[SHUTDOWN] Error while canceling SSE task: {e}")
    
    # Stop all cron job threads
    threads_to_join = []
    for device_id, job_info in list(active_cron_jobs.items()):
        if job_info["thread"].is_alive():
            logger.info(f"Signaling stop for job {device_id}")
            job_info["stop_event"].set()
            threads_to_join.append(job_info["thread"])

    # Wait briefly for threads to potentially finish
    await asyncio.sleep(1)

    # Ensure Arduino is turned off on shutdown
    try: 
        write_control_file("OFF")
        logger.info("[CONTROL] Set control file to OFF on shutdown.")
    except Exception as e: 
        logger.warning(f"Could not set control file to OFF on shutdown: {e}")

    logger.info("Shutdown cleanup completed.")

# ======================
# SIMULATION ENDPOINT (Temporary)
# ======================
@app.get("/simulate-event") # Using GET for easy browser testing
async def simulate_event(device_id: str, on: bool):
    """
    TEMPORARY: Simulates receiving an SSE event for a single device.
    Use in browser: /simulate-event?device_id=1&on=true
                 or /simulate-event?device_id=3&on=false etc.
    """
    logger.info(f"[SIMULATE] Received request: device_id={device_id}, on={on}")
    # Construct the list format expected by process_device_status
    simulated_device_list = [
        {
            "deviceId": device_id,
            "deviceName": f"Simulated Device {device_id}", # Add a default name
            "on": on,
            # Add any other minimal fields if process_device_status relies on them
        }
    ]
    try:
        # Call the existing processing function directly
        # NOTE: process_device_status is synchronous, so we don't await it
        changes = process_device_status(simulated_device_list)

        if changes:
            logger.info(f"[SIMULATE] Processed changes: {'; '.join(changes)}")
            return {"status": "simulation_success", "processed_changes": changes}
        else:
            logger.info("[SIMULATE] No state change detected or action taken for this simulation.")
            return {"status": "simulation_success", "message": "No state change detected or action taken."}
    except Exception as e:
        logger.error(f"[SIMULATE] Error processing simulated event: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error processing simulation: {e}")

# ======================
# Startup
# ======================
# Corrected the name check
if __name__ == "__main__":
    uvicorn.run(
        "main:app", # Module:app_object format
        host="0.0.0.0",
        port=5001, # Changed port from 5000 to 5001
        log_level="info",
        reload=False # IMPORTANT: reload=False for threads/background tasks
    )