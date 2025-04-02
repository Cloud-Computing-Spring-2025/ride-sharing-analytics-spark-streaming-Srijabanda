# Ride-Sharing Streaming Analytics with PySpark

## Overview
This project demonstrates how to build a **real-time ride-sharing analytics pipeline** using **Apache Spark Structured Streaming** and **PySpark**. It simulates streaming JSON ride events, processes them in Spark, and performs driver-level and time-based aggregations — storing the output as CSV files.

## Prerequisites

Make sure you have the following installed on your system:

- Python 3.7+
- Netcat (for simulating socket stream)
- Java 8 or 11 (required for Spark)
- Apache Spark 3.x


### Install Python Dependencies
```bash
pip install faker
pip install pyspark
```

## How to Run the Streaming Pipeline

### Step 1: Start the Netcat Server (Simulated Data Source)
In a terminal window, run:

```bash
nc -lk 9999
```

This opens a socket listener on port `9999`. Data sent here will be picked up by Spark.

### Step 2: Run Each Spark Streaming Task

> Open a separate terminal window for each of the following commands:

#### Task 1: Ingest & Parse Ride Data

```bash
spark-submit task1_streaming_ingestion.py
```

- Reads streaming JSON data from the socket.
- Parses it into structured DataFrame.
- Writes to `data/task1_output/` as CSV.

#### Task 2: Driver-Level Aggregation (Total Fare & Average Distance)

```bash
spark-submit task2_driver_aggregations.py
```

- Groups data by driver and time window.
- Calculates total fare and average trip distance.
- Writes to `data/task2_driver_windowed/` as CSV.

#### ▶️ Task 3: Time-Windowed Fare Analysis

```bash
spark-submit task3_windowed_analytics.py
```

- Performs 5-minute sliding window aggregation (sliding every 1 min).
- Calculates total fare per window.
- Writes to `data/task3_windowed_fare/` as CSV.

### Step 3: Send Sample Data to the Stream

In a **new terminal**, run your data generator or manually send data:

#### Option 1: Use the Generator
```bash
python data_generator.py
```

#### Option 2: Send manually via Netcat
```bash
{"trip_id":"T1001", "driver_id":"D10", "distance_km":5.2, "fare_amount":12.5, "timestamp":"2025-04-01 10:05:00"}
{"trip_id":"T1002", "driver_id":"D11", "distance_km":3.8, "fare_amount":8.9, "timestamp":"2025-04-01 10:06:00"}
```

## Output Examples

### Task 1 Output (Raw Ride Events)
CSV files in `data/task1_output/`
```csv
trip_id,driver_id,distance_km,fare_amount,timestamp
T1001,D10,5.2,12.5,2025-04-01 10:05:00
T1002,D11,3.8,8.9,2025-04-01 10:06:00
```

### Task 2 Output (Driver Aggregation)
CSV files in `data/task2_driver_windowed/`
```csv
window_start,window_end,driver_id,total_fare,avg_distance
2025-04-01 10:00:00,2025-04-01 10:05:00,D10,12.5,5.2
```

### Task 3 Output (Windowed Fare Aggregation)
CSV files in `data/task3_windowed_fare/`
```csv
window_start,window_end,total_fare
2025-04-01 10:00:00,2025-04-01 10:05:00,21.4
```

## Notes
- **Checkpoint directories** are automatically created inside `checkpoints/` for each task.
- Scripts use socket port `9999`. Change if needed in each script.
- Make sure Netcat is running **before Spark jobs**, or you’ll get connection errors.
- All output CSVs are saved inside the `data/` folder.

## Directory Structure

```
ride-sharing-analytics-spark-streaming/
├── task1_streaming_ingestion.py
├── task2_driver_aggregations.py
├── task3_windowed_analytics.py
├── data_generator.py
├── data/
│   ├── task1_output/
│   ├── task2_driver_windowed/
│   └── task3_windowed_fare/
└── checkpoints/
```

---

