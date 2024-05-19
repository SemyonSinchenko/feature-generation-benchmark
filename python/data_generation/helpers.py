import json
import time
from pathlib import Path

from . import DatasetSizes


class BenchmarkWriter:
    def __init__(self, name: str, size: DatasetSizes, results_prefix: Path) -> None:
        json_result = None
        if size == DatasetSizes.TINY:
            json_result = results_prefix.joinpath("results_tiny.json")
        elif size == DatasetSizes.SMALL:
            json_result = results_prefix.joinpath("results_small.json")
        elif size == DatasetSizes.MEDIUM:
            json_result = results_prefix.joinpath("results_medium.json")
        else:
            json_result = results_prefix.joinpath("results_big.json")

        self.name = name
        self.json_results_file = json_result
        self.start_time = None

    def before(self) -> None:
        if self.json_results_file.exists():
            current = json.load(self.json_results_file.open("r"))
            current[self.name] = -1
        else:
            current = {self.name: -1}
        with self.json_results_file.open("w") as file_:
            json.dump(current, file_, indent=2)
        self.start_time = time.time()

    def after(self) -> float:
        end_time = time.time()
        assert self.start_time is not None
        total_time = end_time - self.start_time
        current = json.load(self.json_results_file.open("r"))
        current[self.name] = total_time

        with self.json_results_file.open("w") as file_:
            json.dump(current, file_, indent=2)

        return total_time
