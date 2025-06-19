# Energy Optimization and Forecasting System

This project is a comprehensive backend system designed for real-time energy data monitoring, processing, control, and forecasting. It integrates hardware data acquisition, simulated data generation, a cloud data pipeline, a central control API, and a machine learning model for power consumption prediction.

## System Architecture

The system is composed of several interconnected components that work together:

1.  **Data Ingestion**:
    *   **Hardware**: `serial_to_db.py` reads real-time energy data from an Arduino-connected sensor (like a PZEM-004T) via a serial port.
    *   **Simulation**: `cron_job.py` generates realistic, simulated energy data for development and testing purposes.
    *   Both ingestion scripts write data to a local PostgreSQL database.

2.  **Data Processing & Cloud Sync**:
    *   `batch_process.py` runs periodically to aggregate the raw data from the local database into minute-by-minute averages.
    *   It then transfers this aggregated data to a production-ready Neon (cloud PostgreSQL) database, ensuring the cloud data is clean and efficient to query.

3.  **Control & Management Backend**:
    *   `main.py` is the core FastAPI application that acts as the system's brain.
    *   It listens to a remote Server-Sent Events (SSE) stream to get the on/off status of all registered devices.
    *   Based on the SSE data, it controls the physical Arduino device by writing to a control file (`arduino_control.txt`).
    *   It also manages the lifecycle of simulated data jobs, starting or stopping them as commanded by the SSE stream.

4.  **Machine Learning Forecasting**:
    *   The `LSTM/` directory contains the machine learning pipeline.
    *   `train.py`: This script trains a Long Short-Term Memory (LSTM) neural network on historical power data to predict future energy consumption.
    *   `predict.py`: This script uses the trained model to generate future power predictions, which are then stored in both the local and Neon databases for use by frontend applications.

## Features

- **Real-time Data Acquisition**: Captures live energy metrics from hardware sensors.
- **Scalable Data Simulation**: Generates data for multiple virtual devices.
- **Robust Data Pipeline**: Aggregates and synchronizes data with a cloud database using batch processing.
- **Centralized Control**: A FastAPI backend manages device states and simulation jobs.
- **Remote Command & Control**: Listens to SSE from a production server to control local hardware and software.
- **Predictive Forecasting**: Utilizes an LSTM model to forecast future energy usage.
- **Environment-based Configuration**: Securely manages all credentials and settings via a `.env` file.

## Setup and Installation

### Prerequisites

- Python 3.8+
- PostgreSQL (for local data storage)
- A Neon DB account (for cloud storage)
- An Arduino or similar microcontroller for hardware integration (optional)

### 1. Clone the Repository

```bash
git clone <your-repository-url>
cd <repository-directory>
```

### 2. Install Dependencies

It's recommended to use a virtual environment.

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

Key dependencies include `fastapi`, `uvicorn`, `psycopg2-binary`, `pyserial`, `python-dotenv`, `tensorflow`, `scikit-learn`, `pandas`.

### 3. Configure Environment Variables

Create a `.env` file in the root directory (`d:\HPE`) by copying the example below. Fill in your specific details.

```env
# .env.example

# --- Local PostgreSQL Database ---
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_local_db_name
DB_USER=your_local_db_user
DB_PASSWORD=your_local_db_password

# --- Neon Cloud Database ---
NEON_DB_URL="your_neon_db_connection_string"

# --- Serial Port for Arduino ---
SERIAL_PORT=COM3 # or /dev/ttyUSB0 on Linux
BAUD_RATE=115200

# --- Backend API Configuration ---
API_KEY="your_secret_api_key"
PRODUCTION_URL="https://energy-optimisation-backend.onrender.com"
```

## Running the System

Each major component should be run in a separate terminal.

### Terminal 1: Run the Backend API

This is the central controller and must be running for the system to be managed.

```bash
python main.py
```

### Terminal 2: Run the Hardware Data Collector (Optional)

If you have an Arduino connected, run this script to start collecting real-world data.

```bash
python serial_to_db.py
```

### Terminal 3: Run the Data Transfer Process

This script will periodically aggregate and upload data to the cloud.

```bash
python batch_process.py
```

### Terminal 4: Run the Prediction Generator

This script can be run on a schedule (e.g., via a cron job) to update forecasts.

```bash
python LSTM/predict.py
```

## API Endpoints

The `main.py` application exposes several endpoints for monitoring and control:

- `GET /health`: Provides a detailed health check of the entire system.
- `GET /active-jobs`: Lists all active simulated data generation jobs.
- `POST /force-start-job/{device_id}`: Manually starts a simulation job.
- `POST /force-stop-job/{device_id}`: Manually stops a simulation job.
- `GET /sse-subscribe`: Checks the status of the background SSE listener.
