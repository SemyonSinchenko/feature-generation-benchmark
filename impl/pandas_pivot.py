import shutil
import sys
from functools import reduce
from pathlib import Path

import pandas as pd
from data_generation.helpers import BenchmarkWriter, DatasetSizes
from rich import print

# See lib.rs for details about constants
CARD_TYPES = ("DC", "CC")
TRANSACTION_TYPES = (
    "food-and-household",
    "home",
    "uncategorized",
    "leisure-and-lifestyle",
    "health-and-beauty",
    "shopping-and-services",
    "children",
    "vacation-and-travel",
    "education",
    "insurance",
    "investments-and-savings",
    "expenses-and-other",
    "cars-and-transportation",
)
CHANNELS = ("mobile", "web")


# Required time windows
WINDOWS_IN_DAYS = (
    7,  # week
    14,  # two weeks
    21,  # three weeks
    30,  # month
    90,  # three months
    180,  # half of the year
    360,  # year
    720,  # two years
)


# Information for result
NAME = "Pandas pivot"

def generate_pivoted_batch(data: pd.DataFrame, t_minus: int, groups: list[str]) -> pd.DataFrame:
    pre_agg = (
        data.loc[data["t_minus"] <= t_minus]
        .groupby(["customer_id"] + groups, as_index=False)["trx_amnt"]
        .agg(["count", "mean", "sum", "min", "max"])
    )
    pivoted = pre_agg.pivot(
        columns=groups,
        index="customer_id",
        values=["count", "mean", "sum", "min", "max"],
    )
    pivoted.columns = ["_".join(a[1:]) + f"_{t_minus}d_{a[0]}" for a in pivoted.columns.to_flat_index()]
    return pivoted


if __name__ == "__main__":
    path = sys.argv[1]

    if "tiny" in path:
        task_size = DatasetSizes.TINY
    elif "small" in path:
        task_size = DatasetSizes.SMALL
    elif "medium" in path:
        task_size = DatasetSizes.MEDIUM
    else:
        task_size = DatasetSizes.BIG

    shutil.rmtree("tmp_out", ignore_errors=True)

    results_prefix = Path(__file__).parent.parent.joinpath("results")
    helper = BenchmarkWriter(NAME, task_size, results_prefix)

    # Start the work
    helper.before()

    data = pd.read_parquet(path)

    #  #   Column       Dtype
    # ---  ------       -----
    #  0   customer_id  int64
    #  1   card_type    object
    #  2   trx_type     object
    #  3   channel      object
    #  4   trx_amnt     float64
    #  5   t_minus      int64
    #  6   part_col     category

    dfs_list = []

    for win in WINDOWS_IN_DAYS:
        # Iterate over combination card_type + trx_type
        dfs_list.append(generate_pivoted_batch(data, win, ["card_type", "trx_type"]))

        # Iterate over combination channel + trx_type
        dfs_list.append(generate_pivoted_batch(data, win, ["channel", "trx_type"]))

    (
        reduce(lambda a, b: pd.merge(a, b, left_index=True, right_index=True), dfs_list)
        .reset_index(drop=False)
        .to_parquet("../tmp_out")
    )

    total_time = helper.after()

    print(f"[italic green]Total time: {total_time} seconds[/italic green]")
    sys.exit(0)
