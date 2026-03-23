
import hashlib
from typing import Dict, Any, List
import multiprocessing


# ─── Pure Functions (Functional Core) ────────────────────────────────────────

def generate_signature(raw_value_str: str, key: str, iterations: int) -> str:
    password_bytes = key.encode('utf-8')
    salt_bytes     = raw_value_str.encode('utf-8')
    hash_bytes     = hashlib.pbkdf2_hmac(
        hash_name  = 'sha256',
        password   = password_bytes,
        salt       = salt_bytes,
        iterations = iterations
    )
    return hash_bytes.hex()

def verify_packet_signature(packet: Dict[str, Any], secret_key: str, iterations: int) -> bool:
    raw_value     = packet.get('metric_value', 0)
    attached_sig  = packet.get('security_hash', '')
    raw_value_str = f"{round(float(raw_value), 2)}"
    computed_sig  = generate_signature(raw_value_str, secret_key, iterations)
    return computed_sig == attached_sig


def calculate_sliding_average(window: List[float]) -> float:
    if not window:
        return 0.0
    return sum(window) / len(window)


def update_window(window: List[float], new_value: float, window_size: int) -> List[float]:
    new_window = list(window) + [new_value]
    return new_window[-window_size:]


# ─── SignatureWorker (PARALLEL) ───────────────────────────────────────────────

class SignatureWorker:
    def __init__(self, worker_id: int,
                 raw_queue: multiprocessing.Queue,
                 verified_queue: multiprocessing.Queue,
                 config: dict):
        self.worker_id      = worker_id
        self.raw_queue      = raw_queue
        self.verified_queue = verified_queue
        self.config         = config

    def run(self):
        secret_key = self.config['processing']['stateless_tasks']['secret_key']
        iterations = self.config['processing']['stateless_tasks']['iterations']
        print(f"signature worker {self.worker_id} started")

        while True:
            try:
                packet = self.raw_queue.get(timeout=5)
                if packet is None:
                    self.verified_queue.put(None)
                    print(f"signature worker {self.worker_id} stopped")
                    break
                is_real = verify_packet_signature(packet, secret_key, iterations)
                if is_real:
                    self.verified_queue.put(packet)
                else:
                    print(f"worker {self.worker_id} dropped fake: {packet.get('entity_name')}")
            except Exception:
                break


# ─── AggregatorWorker (SEQUENTIAL) ───────────────────────────────────────────

class AggregatorWorker:
    def __init__(self, verified_queue: multiprocessing.Queue,
                 processed_queue: multiprocessing.Queue,
                 config: dict, num_workers: int):
        self.verified_queue  = verified_queue
        self.processed_queue = processed_queue
        self.config          = config
        self.num_workers     = num_workers

    def run(self):
        window_size = self.config['processing']['stateful_tasks']['running_average_window_size']
        windows: Dict[str, List[float]] = {}
        stop_signals = 0
        print('aggregator worker started')

        while True:
            try:
                packet = self.verified_queue.get(timeout=5)
                if packet is None:
                    stop_signals += 1
                    if stop_signals >= self.num_workers:
                        self.processed_queue.put(None)
                        print('aggregator worker stopped')
                        break
                    continue
                entity          = packet.get('entity_name', 'unknown')
                metric          = float(packet.get('metric_value', 0.0))
                current_window  = windows.get(entity, [])
                new_window      = update_window(current_window, metric, window_size)
                average         = calculate_sliding_average(new_window)
                windows[entity] = new_window
                result = {
                    'entity_name':     packet.get('entity_name'),
                    'time_period':     packet.get('time_period'),
                    'metric_value':    metric,
                    'computed_metric': round(average, 4)
                }
                self.processed_queue.put(result)
            except Exception:
                break
              

import hashlib
from typing import Dict, Any, List
import multiprocessing


# ─── Pure Functions (Functional Core) ────────────────────────────────────────

def generate_signature(raw_value_str: str, key: str, iterations: int) -> str:
    password_bytes = key.encode('utf-8')
    salt_bytes     = raw_value_str.encode('utf-8')
    hash_bytes     = hashlib.pbkdf2_hmac(
        hash_name  = 'sha256',
        password   = password_bytes,
        salt       = salt_bytes,
        iterations = iterations
    )
    return hash_bytes.hex()


def verify_packet_signature(packet: Dict[str, Any], secret_key: str, iterations: int) -> bool:
    raw_value     = packet.get('metric_value', 0)
    attached_sig  = packet.get('security_hash', '')
    raw_value_str = f"{round(float(raw_value), 2)}"
    computed_sig  = generate_signature(raw_value_str, secret_key, iterations)
    return computed_sig == attached_sig


def calculate_sliding_average(window: List[float]) -> float:
    if not window:
        return 0.0
    return sum(window) / len(window)


def update_window(window: List[float], new_value: float, window_size: int) -> List[float]:
    new_window = list(window) + [new_value]
    return new_window[-window_size:]


# ─── SignatureWorker (PARALLEL) ───────────────────────────────────────────────

class SignatureWorker:
    def __init__(self, worker_id: int,
                 raw_queue: multiprocessing.Queue,
                 verified_queue: multiprocessing.Queue,
                 config: dict):
        self.worker_id      = worker_id
        self.raw_queue      = raw_queue
        self.verified_queue = verified_queue
        self.config         = config

    def run(self):
        secret_key = self.config['processing']['stateless_tasks']['secret_key']
        iterations = self.config['processing']['stateless_tasks']['iterations']
        print(f"signature worker {self.worker_id} started")

        while True:
            try:
                packet = self.raw_queue.get(timeout=5)
                if packet is None:
                    self.verified_queue.put(None)
                    print(f"signature worker {self.worker_id} stopped")
                    break
                is_real = verify_packet_signature(packet, secret_key, iterations)
                if is_real:
                    self.verified_queue.put(packet)
                else:
                    print(f"worker {self.worker_id} dropped fake: {packet.get('entity_name')}")
            except Exception:
                break


# ─── AggregatorWorker (SEQUENTIAL) ───────────────────────────────────────────

class AggregatorWorker:
    def __init__(self, verified_queue: multiprocessing.Queue,
                 processed_queue: multiprocessing.Queue,
                 config: dict, num_workers: int):
        self.verified_queue  = verified_queue
        self.processed_queue = processed_queue
        self.config          = config
        self.num_workers     = num_workers

    def run(self):
        window_size = self.config['processing']['stateful_tasks']['running_average_window_size']
        windows: Dict[str, List[float]] = {}
        stop_signals = 0
        print('aggregator worker started')

        while True:
            try:
                packet = self.verified_queue.get(timeout=5)
                if packet is None:
                    stop_signals += 1
                    if stop_signals >= self.num_workers:
                        self.processed_queue.put(None)
                        print('aggregator worker stopped')
                        break
                    continue
                entity          = packet.get('entity_name', 'unknown')
                metric          = float(packet.get('metric_value', 0.0))
                current_window  = windows.get(entity, [])
                new_window      = update_window(current_window, metric, window_size)
                average         = calculate_sliding_average(new_window)
                windows[entity] = new_window
                result = {
                    'entity_name':     packet.get('entity_name'),
                    'time_period':     packet.get('time_period'),
                    'metric_value':    metric,
                    'computed_metric': round(average, 4)
                }
                self.processed_queue.put(result)
            except Exception:
                break