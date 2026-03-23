SDA PHASE 3 : Generic Concurrent Real-Time Pipeline


MAIN FILE    : main.py
DATA FOLDER  : data/
CONFIG FILE  : config.json

----------------------------------------------------------------
HOW TO RUN
----------------------------------------------------------------
Step 1: Install dependencies:
   pip install matplotlib pandas

Step 2: Generate sample data (first time only):
   python generate_data.py

Step 3: Run the pipeline:
   python main.py

Step 4: A live dashboard window opens automatically
   Close the window to stop the pipeline

----------------------------------------------------------------
CONFIGURATION
----------------------------------------------------------------
To use a different dataset, update config.json:
   - dataset_path     : path to your CSV file
   - schema_mapping   : map your column names to internal names
   - core_parallelism : number of parallel workers
   - secret_key       : key used for signature verification

No code changes needed - only config.json needs updating

----------------------------------------------------------------
PROJECT STRUCTURE
----------------------------------------------------------------
main.py              - Orchestrator, starts all processes
config.json          - All settings and schema mapping
generate_data.py     - Generates sample sensor data with signatures
readme.txt           - This file

core/
   contracts.py      - Protocols (DataSink, PipelineService)
   engine.py         - SignatureWorker + AggregatorWorker

plugins/
   inputs.py         - Generic CSV reader with schema mapping
   outputs.py        - Live real-time dashboard

telemetry/
   monitor.py        - Queue health monitor (Observer pattern)

data/
   sample_sensor_data.csv  - Place your dataset here

----------------------------------------------------------------
PIPELINE FLOW
----------------------------------------------------------------
CSV File
   ↓
Input (CSVReader) → Queue 1 (Raw Data)
   ↓
4x SignatureWorker (PARALLEL) → Queue 2 (Verified Data)
   ↓
AggregatorWorker (SEQUENTIAL) → Queue 3 (Processed Data)
   ↓
LiveDashboard (shows live charts + telemetry bars)

----------------------------------------------------------------
Team Members:
Amna Dilshad       - main.py, config.json, contracts.py,
                      inputs.py, generate_data.py
Fiza Mubbsher      - engine.py, outputs.py, monitor.py
