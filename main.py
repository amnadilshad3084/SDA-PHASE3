"""
main.py
orchestrator - connects everything together
1. loads config
2. creates 3 queues
3. creates all workers
4. starts everything simultaneously
5. opens live dashboard
"""
import json
import multiprocessing
import sys
import os
import os
os.environ["OPENBLAS_NUM_THREADS"] = "1"

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from plugins.inputs  import CSVReader
from plugins.outputs import LiveDashboard
from core.engine     import SignatureWorker, AggregatorWorker
from telemetry.monitor import PipelineTelemetry


def load_config(filepath: str = 'config.json') -> dict:
    try:
        with open(filepath, 'r') as f:
            config = json.load(f)
        print("config loaded successfully")
        return config
    except FileNotFoundError:
        print(f"error config not found {filepath}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"error invalid json {e}")
        sys.exit(1)


def bootstrap():
    print("=" * 60)
    print("  PIPELINE SYSTEM PHASE 3")
    print("=" * 60)

    # 1. load config
    config      = load_config()
    parallelism = config['pipeline_dynamics']['core_parallelism']
    max_size    = config['pipeline_dynamics']['stream_queue_max_size']

    print(f"workers     : {parallelism}")
    print(f"queue size  : {max_size}")
    print(f"dataset     : {config['dataset_path']}")

    # 2. create 3 bounded queues
    raw_queue       = multiprocessing.Queue(maxsize=max_size)
    verified_queue  = multiprocessing.Queue(maxsize=max_size)
    processed_queue = multiprocessing.Queue(maxsize=max_size)

    print("queues created")

    # 3. create worker objects
    csv_reader  = CSVReader(raw_queue, config)
    sig_workers = [
        SignatureWorker(i, raw_queue, verified_queue, config)
        for i in range(parallelism)
    ]
    aggregator  = AggregatorWorker(
        verified_queue, processed_queue, config, parallelism
    )
    dashboard   = LiveDashboard(
        processed_queue, raw_queue, verified_queue, config
    )

    # 4. start telemetry monitor
    telemetry = PipelineTelemetry(
        raw_queue, verified_queue, processed_queue, max_size
    )
    telemetry.start()

    # 5. wrap in processes and start
    processes = []

    input_process = multiprocessing.Process(
        target=csv_reader.run, name="InputReader"
    )
    processes.append(input_process)

    for i, worker in enumerate(sig_workers):
        p = multiprocessing.Process(
            target=worker.run, name=f"SigWorker-{i}"
        )
        processes.append(p)

    agg_process = multiprocessing.Process(
        target=aggregator.run, name="Aggregator"
    )
    processes.append(agg_process)

    print("\nstarting all processes...")
    for p in processes:
        p.start()
        print(f"  started {p.name}")

    print("\ndashboard opening...")
    print("close dashboard window to stop pipeline")
    print("=" * 60)

    # 6. run dashboard in main process
    dashboard.run()

    # 7. cleanup
    print("\ncleaning up...")
    telemetry.stop()
    for p in processes:
        p.terminate()
        p.join()
    print("pipeline complete")


if __name__ == "__main__":
    multiprocessing.freeze_support()
    bootstrap()
