from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from cron_job import generate_and_insert_data
from datetime import datetime
import os
from dotenv import load_dotenv
import logging
import httpx
import json
import asyncio
import threading
from contextlib import asynccontextmanager

load_dotenv()

@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    # Startup
    global sse_background_task
    logger.info("Energy Optimization Backend Starting")
    logger.info(f"Auto-connecting to SSE: {PRODUCTION_URL}")
    logger.info(f"Controlling Arduino (ID: {ARDUINO_DEVICE_ID})")
    
    sse_background_task = asyncio.create_task(sse_background_listener())
    logger.info("Background SSE task created")
    
    yield
    
    # Shutdown
    logger.info("Shutting down: Stopping active jobs...")
    if sse_background_task and not sse_background_task.done():
        sse_background_task.cancel()
        try:
            await sse_background_task
        except asyncio.CancelledError:
            logger.info("SSE background task canceled")
    
    # Stop cron jobs
    for device_id, job_info in list(active_cron_jobs.items()):
        if job_info["thread"].is_alive():
            logger.info(f"Stopping job {device_id}")
            job_info["stop_event"].set()

    await asyncio.sleep(2)
    
    try:
        write_control_file("OFF")
        logger.info("Set control file to OFF on shutdown")
    except Exception as e:
        logger.warning(f"Could not set control file to OFF: {e}")

app = FastAPI(
    title="Energy Optimization Backend",
    description="Handles SSE, controls Arduino, manages simulated data jobs",
    version="14.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
API_KEY = os.getenv("API_KEY", "EnergyOpt_50rBIeCMvy1u_AjpyB7qnTUpVxSQWzz1LVgVlUizJeg")
PRODUCTION_URL = os.getenv("PRODUCTION_URL", "https://energy-optimisation-backend.onrender.com")
SSE_ENDPOINT = f"{PRODUCTION_URL}/api/device-status/house/1/subscribe"
TOGGLE_ENDPOINT = f"{PRODUCTION_URL}/api/device-status/{{deviceId}}/toggle"
ARDUINO_DEVICE_ID = "1"
CONTROL_FILE_PATH = "arduino_control.txt"

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler("energy_backend.log")]
)
logger = logging.getLogger(__name__)

# State management
active_cron_jobs = {}
last_arduino_state = None
sse_background_task = None

def write_control_file(state: str):
    """Write control state to file for Arduino communication."""
    try:
        with open(CONTROL_FILE_PATH, "w") as f:
            f.write(state.upper())
        logger.info(f"Wrote '{state.upper()}' to control file")
    except IOError as e:
        logger.error(f"Error writing control file: {e}")

def run_cron_job(device_id, stop_event):
    """Start data generation job for simulated device."""
    try:
        device_ids_list = [str(device_id)]
        thread = threading.Thread(
            target=generate_and_insert_data,
            args=(device_ids_list, stop_event),
            daemon=True
        )
        thread.start()
        logger.info(f"Started simulation job for device {device_id}")
        return thread
    except Exception as e:
        logger.error(f"Error starting job for device {device_id}: {e}")
        return None

def process_device_status(devices):
    """Process device status updates from SSE."""
    global last_arduino_state
    changes_summary = []

    for device in devices:
        device_id = str(device.get("deviceId"))
        status = device.get("on")
        device_name = device.get("deviceName", f"Device {device_id}")

        # Handle Arduino control
        if device_id == ARDUINO_DEVICE_ID:
            current_state_str = "on" if status else "off"
            if current_state_str != last_arduino_state:
                logger.info(f"Arduino state change: {last_arduino_state} -> {current_state_str}")
                write_control_file("ON" if status else "OFF")
                last_arduino_state = current_state_str
                changes_summary.append(f"Arduino changed to {current_state_str.upper()}")
            continue

        # Handle simulated device jobs
        if status and device_id not in active_cron_jobs:
            # Start simulation job
            stop_event = threading.Event()
            thread = run_cron_job(device_id, stop_event)
            if thread:
                active_cron_jobs[device_id] = {
                    "thread": thread,
                    "stop_event": stop_event,
                    "started_at": datetime.now().isoformat(),
                    "device_name": device_name
                }
                changes_summary.append(f"Started sim job for {device_name}")

        elif not status and device_id in active_cron_jobs:
            # Stop simulation job
            job_info = active_cron_jobs.get(device_id)
            if job_info and job_info["thread"].is_alive():
                job_info["stop_event"].set()
                changes_summary.append(f"Stopped sim job for {device_name}")
            
            if device_id in active_cron_jobs:
                del active_cron_jobs[device_id]

    return changes_summary

async def sse_background_listener():
    """Background task to listen for SSE events."""
    global last_arduino_state
    logger.info("Starting background SSE connection...")
    
    try:
        write_control_file("OFF")
        last_arduino_state = "off"
        logger.info("Initialized control file to OFF")
    except Exception as e:
        logger.warning(f"Could not initialize control file: {e}")

    while True:
        try:
            logger.info(f"Connecting to SSE: {SSE_ENDPOINT}")
            
            async with httpx.AsyncClient(timeout=None) as client:
                async with client.stream("GET", SSE_ENDPOINT, headers={"x-api-key": API_KEY}) as response:
                    if response.status_code != 200:
                        logger.error(f"SSE connection failed: {response.status_code}")
                        await asyncio.sleep(10)
                        continue

                    logger.info("SSE connected successfully")
                    
                    async for line in response.aiter_lines():
                        if not line.strip():
                            continue

                        if line.startswith("data:"):
                            try:
                                raw_data = line[5:].strip()
                                devices_data = json.loads(raw_data)
                                if isinstance(devices_data, list):
                                    logger.info(f"Received {len(devices_data)} device states")
                                    changes = process_device_status(devices_data)
                                    if changes:
                                        logger.info(f"Actions: {'; '.join(changes)}")
                            except json.JSONDecodeError as e:
                                logger.error(f"JSON decode error: {e}")
                            except Exception as e:
                                logger.error(f"Processing error: {e}")

        except httpx.RequestError as e:
            logger.error(f"SSE connection error: {e}. Retrying in 10s")
            await asyncio.sleep(10)
        except Exception as e:
            logger.error(f"Unexpected SSE error: {e}. Retrying in 10s")
            await asyncio.sleep(10)

# API Endpoints
@app.get("/sse-subscribe")
async def sse_subscribe():
    """Check SSE connection status."""
    global sse_background_task
    
    if sse_background_task is None or sse_background_task.done():
        return {
            "status": "SSE background task not running",
            "note": "The app will attempt to restart automatically"
        }
    
    return {
        "status": "SSE background task running",
        "endpoint": SSE_ENDPOINT,
        "arduino_device_id": ARDUINO_DEVICE_ID,
        "last_arduino_state": last_arduino_state
    }

@app.get("/active-jobs")
async def get_active_jobs():
    """List active simulation jobs."""
    result = {}
    jobs_to_remove = []
    
    for device_id, job_info in active_cron_jobs.items():
        is_alive = job_info["thread"].is_alive()
        if not is_alive:
            jobs_to_remove.append(device_id)

        result[device_id] = {
            "device_name": job_info.get("device_name", "Unknown"),
            "started_at": job_info["started_at"],
            "running": is_alive,
            "elapsed_seconds": round(
                (datetime.now() - datetime.fromisoformat(job_info["started_at"])).total_seconds()
            )
        }

    # Clean up finished jobs
    for device_id in jobs_to_remove:
        del active_cron_jobs[device_id]

    return result

@app.get("/health")
async def health_check():
    """Health status endpoint."""
    global sse_background_task
    
    active_jobs_list = []
    for d_id, j_info in active_cron_jobs.items():
        if j_info["thread"].is_alive():
            active_jobs_list.append({
                "device_id": d_id,
                "device_name": j_info.get("device_name", "Unknown"),
                "running": True,
                "started_at": j_info["started_at"]
            })

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

async def make_authenticated_request(url: str, method="GET", **kwargs):
    """Make authenticated request to production backend."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                headers = kwargs.pop("headers", {})
                headers["x-api-key"] = API_KEY
                response = await client.request(method, url, headers=headers, **kwargs)
                response.raise_for_status()
                return response
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            if attempt == max_retries - 1:
                raise HTTPException(status_code=502, detail=f"Request failed: {e}")
            await asyncio.sleep(2 ** attempt)

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=5001,
        log_level="info",
        reload=False
    )