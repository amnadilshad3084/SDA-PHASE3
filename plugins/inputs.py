"""
generic file reader - reads ANY csv file
maps column names to internal names using config schema_mapping
never knows what the data actually means
"""
import csv
import time
import multiprocessing
from typing import Dict, Any, List


# type casting functions
TYPE_CASTERS = {
    'string':  str,
    'integer': int,
    'float':   float,
}


def cast_value(value: str, data_type: str) -> Any:
    """pure function - casts string value to correct type"""
    caster = TYPE_CASTERS.get(data_type, str)
    try:
        return caster(value)
    except (ValueError, TypeError):
        return value


def map_row_to_packet(row: Dict[str, str], schema: List[Dict]) -> Dict[str, Any]:
    """
    pure function - maps raw column names to internal generic names
    uses schema from config
    """
    packet = {}
    for col_def in schema:
        source_name   = col_def['source_name']
        internal_name = col_def['internal_mapping']
        data_type     = col_def['data_type']
        raw_value     = row.get(source_name, '')
        packet[internal_name] = cast_value(raw_value, data_type)
    return packet


class CSVReader:
    """
    reads any csv file row by row
    maps columns using config schema
    puts each packet into raw_queue
    completely generic - no domain knowledge
    """

    def __init__(self, raw_queue: multiprocessing.Queue, config: dict):
        self.raw_queue = raw_queue
        self.config    = config

    def run(self):
        """reads file slowly and puts packets in queue"""
        filepath    = self.config['dataset_path']
        delay       = self.config['pipeline_dynamics']['input_delay_seconds']
        schema      = self.config['schema_mapping']['columns']
        parallelism = self.config['pipeline_dynamics']['core_parallelism']

        print(f"csv reader started reading {filepath}")

        try:
            with open(filepath, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    packet = map_row_to_packet(row, schema)
                    self.raw_queue.put(packet)
                    time.sleep(delay)

        except FileNotFoundError:
            print(f"error file not found {filepath}")
        except Exception as e:
            print(f"csv reader error {e}")

        # send stop signal for each worker
        for _ in range(parallelism):
            self.raw_queue.put(None)

        print("csv reader finished")