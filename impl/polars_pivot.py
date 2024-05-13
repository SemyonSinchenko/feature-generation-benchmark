import json
import shutil
import sys
import time
from pathlib import Path

import polars as pl
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
    180, # half of the year
    360, # year
    720, # two years
)


# Information for result
ENGINE_NAME = "Polars"
APPROACH_NAME = "PivotEager"


def generate_pivoted_batch(data: pl.DataFrame, t_minus: int, groups: list[str]) -> pl.DataFrame:

    pivoted = (
        data.filter(pl.col("t_minus") <= t_minus)
        .group_by(["customer_id"] + groups)
        .agg(
            [
                pl.count("trx_amnt").alias("count"),
                pl.mean("trx_amnt").alias("mean"),
                pl.sum("trx_amnt").alias("sum"),
                pl.min("trx_amnt").alias("min"),
                pl.max("trx_amnt").alias("max"),
            ]
        )
        .pivot(
            index="customer_id",
            columns=groups,
            values=[
                "count", 
                "mean", 
                "sum", 
                "min", 
                "max"
            ]
        )
    )

    # Polars make some nasty looking column names when pivoting.
    pivoted = pivoted.rename(
        {
            column: (
                column.split("_")[-1]
                .replace('{', '')
                .replace('}', '')
                .replace('"', '')
                .replace(",","_") 
                + f"_{t_minus}d_" + column.split("_")[0]
            ) for column in pivoted.columns
            if column != "customer_id"
        }
    )
    return pivoted


if __name__ == "__main__":
    path = sys.argv[1]
    json_results_out_file = None

    if "tiny" in path:
        json_results_out_file = "results_tiny.json"
    elif "small" in path:
        json_results_out_file = "results_small.json"
    elif "medium" in path:
        json_results_out_file = "results_medium.json"
    else:
        json_results_out_file = "results_big.json"

    shutil.rmtree("tmp_out", ignore_errors=True)

    # Before we go we save the information for the case of OOM
    json_results = Path(__file__).parent.parent.joinpath("results").joinpath(json_results_out_file)

    if json_results.exists():
        results_dict = json.load(json_results.open("r"))
    else:
        results_dict = {}

    results_dict[ENGINE_NAME] = {
        "dataset": path,
        "approach": APPROACH_NAME,
        "total_time": -1,  # Indicator of OOM/Error; we will overwrite it in the case of success
    }

    with json_results.open("w") as file_:
        json.dump(obj=results_dict, fp=file_, indent=1)

    # Start the work
    start_time = time.time()

    data = pl.read_parquet(path + "/**/*.parquet")

    #  #   Column       Dtype
    # ---  ------       -----
    #  0   customer_id  int64
    #  1   card_type    object
    #  2   trx_type     object
    #  3   channel      object
    #  4   trx_amnt     float64
    #  5   t_minus      int64
    #  6   part_col     category

    dfs_list: list[pl.DataFrame] = []

    for win in WINDOWS_IN_DAYS:
        # Iterate over combination card_type + trx_type
        dfs_list.append(generate_pivoted_batch(data, win, ["card_type", "trx_type"]))

        # Iterate over combination channel + trx_type
        dfs_list.append(generate_pivoted_batch(data, win, ["channel", "trx_type"]))

    (
        pl.concat(dfs_list, how="align")
        .write_parquet("../tmp_out")
    )

    end_time = time.time()
    total_time = end_time - start_time

    print(f"[italic green]Total time: {total_time} seconds[/italic green]")

    # Write results
    print("[italic green]Dump results to JSON...[/italic green]")
    if json_results.exists():
        results_dict = json.load(json_results.open("r"))
    else:
        results_dict = {}

    results_dict[ENGINE_NAME] = {
        "dataset": path,
        "approach": APPROACH_NAME,
        "total_time": total_time,
    }

    with json_results.open("w") as file_:
        json.dump(obj=results_dict, fp=file_, indent=1)

    print("[italic green]Done.[/italic green]")
    sys.exit(0)
