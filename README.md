# Pocket Pitwall Data Service

**Pocket Pitwall Data Service** is a Python-based middleware designed to bridge the OpenF1 API with the **M5Stack Cardputer**. It handles the heavy lifting of fetching, processing, and streaming F1 telemetry data so the handheld device can render a smooth, real-time race replay.

## Purpose

The primary goal of this service is to act as a data aggregator and WebSocket server. Since microcontrollers like the ESP32 have limited processing power and memory, this service pre-calculates track bounds, formats driver metadata, and streams high-frequency position data in a lightweight format optimized for the Cardputer’s display.

## How It Works

### 1. Data Collection (`openF1SessionBuilder.py`)

This component communicates directly with the OpenF1 API to gather all necessary data for a specific race session.

* **Session Metadata**: Fetches details about the circuit, country, and session type.
* **Driver Information**: Collects driver names, acronyms, and team colors.
* **Track Layout**: Downloads the X/Y coordinates for both the main track and the pit lane.
* **Live/Historic Data**: Retrieves lap times, intervals, stints, and high-frequency position telemetry.

### 2. The Middleware Server (`server.py`)

The server provides a unified interface for the Cardputer hardware to consume.

* **REST API**: Serves static session data like `drivers.json`, `track_layout.json`, and `race_metadata.json` via HTTP GET requests.
* **WebSocket Stream**: Opens a real-time connection (`/ws/{session_id}`) to stream driver positions.
* **Coordinate Processing**: The server identifies the track boundaries (min/max X and Y) so the Cardputer can instantly scale the map to its screen.

### 3. Data Formatting

To keep the network payload small, the server transforms verbose JSON telemetry into a compact, pipe-delimited string format (e.g., `Timestamp|DriverID,X,Y,Position|...`). This allows the Cardputer to parse dozens of car movements every 100ms with minimal overhead.

## Project Structure

* **`race_data_{id}/`**: Local cache of processed F1 data, including CSV telemetry and JSON metadata.
* **`openF1SessionBuilder.py`**: The engine for fetching and building local session databases from OpenF1.
* **`server.py`**: The FastAPI-based server that hosts the data and manages WebSocket connections.
* **`test.py`**: A utility script for validating server responses and data integrity.

## Technical Requirements

* **Python 3.x**
* **FastAPI**: For the web server and WebSocket implementation.
* **Pandas**: Used for efficient processing of large telemetry CSV files.
* **Requests**: To interface with the OpenF1 API.

## License

This project is licensed under the **MIT License**.
