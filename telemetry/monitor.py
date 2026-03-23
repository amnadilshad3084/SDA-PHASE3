import multiprocessing
import threading
import time
from typing import List, Dict, Any
from core.contracts import TelemetryObserver


class PipelineTelemetry:
    def __init__(self, raw_queue: multiprocessing.Queue,
                 verified_queue: multiprocessing.Queue,
                 processed_queue: multiprocessing.Queue,
                 max_size: int):
        self.raw_queue       = raw_queue
        self.verified_queue  = verified_queue
        self.processed_queue = processed_queue
        self.max_size        = max_size
        self.observers: List[TelemetryObserver] = []
        self.running = False

    def subscribe(self, observer: TelemetryObserver):
        self.observers.append(observer)

    def _get_fill_percent(self, queue: multiprocessing.Queue) -> float:
        try:
            return min((queue.qsize() / self.max_size) * 100, 100)
        except Exception:
            return 0.0

    def _notify_observers(self, telemetry: Dict[str, Any]):
        for observer in self.observers:
            observer.on_telemetry_update(telemetry)

    def _poll_loop(self):
        while self.running:
            telemetry = {
                'raw_fill_percent':       self._get_fill_percent(self.raw_queue),
                'verified_fill_percent':  self._get_fill_percent(self.verified_queue),
                'processed_fill_percent': self._get_fill_percent(self.processed_queue),
            }
            self._notify_observers(telemetry)
            time.sleep(0.5)

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._poll_loop, daemon=True)
        self.thread.start()
        print('telemetry monitor started')

    def stop(self):
        self.running = False
        print('telemetry monitor stopped')
      
