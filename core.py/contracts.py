"""
Core owns all protocols. Every module depends on these.
Nothing outside core is imported here.
"""
from typing import Protocol, List, Dict, Any, runtime_checkable


@runtime_checkable
class DataSink(Protocol):
    """anything that receives processed results must have write()"""
    def write(self, data: List[Dict[str, Any]]) -> None: ...


@runtime_checkable
class PipelineService(Protocol):
    """core engine must have execute() so input can call it"""
    def execute(self, raw_data: Dict[str, Any]) -> None: ...


@runtime_checkable
class TelemetryObserver(Protocol):
    """dashboard subscribes to telemetry using this"""
    def on_telemetry_update(self, telemetry: Dict[str, Any]) -> None: ...