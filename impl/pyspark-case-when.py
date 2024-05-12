import json
import shutil
import sys
import time
from pathlib import Path

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F
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
    360,  # two years
)

# Information for result
ENGINE_NAME = "PySpark"
APPROACH_NAME = "Case-When"


def get_all_aggregations(col_prefix: str, cond: Column, cols_list: list[Column]) -> None:
    # Count over group
    cols_list.append(F.sum(F.when(cond, F.lit(1)).otherwise(F.lit(0))).alias(f"{col_prefix}_count"))
    # Average over group
    cols_list.append(F.mean(F.when(cond, F.col("trx_amnt")).otherwise(F.lit(None))).alias(f"{col_prefix}_avg"))
    # Sum over group
    cols_list.append(F.sum(F.when(cond, F.col("trx_amnt")).otherwise(F.lit(0))).alias(f"{col_prefix}_sum"))
    # Min over group
    cols_list.append(F.min(F.when(cond, F.col("trx_amnt")).otherwise(F.lit(None))).alias(f"{col_prefix}_min"))
    # Max over group
    cols_list.append(F.max(F.when(cond, F.col("trx_amnt")).otherwise(F.lit(None))).alias(f"{col_prefix}_max"))


if __name__ == "__main__":
    path = sys.argv[1]
    shutil.rmtree("tmp_out", ignore_errors=True)

    # Before we go we save the information for the case of OOM
    json_results = Path(__file__).parent.parent.joinpath("results").joinpath("results.json")

    if json_results.exists():
        results_dict = json.load(json_results.open("r"))
    else:
        results_dict = {}

    results_dict[ENGINE_NAME] = {
        "dataset": path,
        "approach": APPROACH_NAME,
        "total_time": -1, # Indicator of OOM/Error; we will overwrite it in the case of success
    }

    with json_results.open("w") as file_:
        json.dump(obj=results_dict, fp=file_, indent=1)

    # Start the work
    start_time = time.time()

    # Partially inspired by:
    # 1. https://github.com/h2oai/db-benchmark/blob/master/spark/groupby-spark.py
    # 2. https://github.com/MrPowers/quinn/issues/143
    spark = (
        SparkSession.builder.master("local[*]")
        .config("spark.driver.memory", "16g")
        .config("spark.executor.memory", "16g")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.log.level", "ERROR")
        .getOrCreate()
    )
    # root
    # |-- customer_id: long (nullable = true)
    # |-- card_type: string (nullable = true)
    # |-- trx_type: string (nullable = true)
    # |-- channel: string (nullable = true)
    # |-- trx_amnt: double (nullable = true)
    # |-- t_minus: long (nullable = true)
    # |-- part_col: string (nullable = true)

    data = spark.read.parquet(path)

    cols_list = []
    for win in WINDOWS_IN_DAYS:
        # Iterate over combination card_type + trx_type
        for card_type in CARD_TYPES:
            for trx_type in TRANSACTION_TYPES:
                cond = F.lit(True)
                cond &= F.col("t_minus") <= F.lit(win)  # Is row in the window?
                cond &= F.col("card_type") == F.lit(card_type)  # Does row have needed card type?
                cond &= F.col("trx_type") == F.lit(trx_type)  # Does row have needed trx type?

                # Colname prefix
                col_prefix = f"{card_type}_{trx_type}_{win}d"

                get_all_aggregations(col_prefix, cond, cols_list)

        # Iterate over combination channel + trx_type
        for ch_type in CHANNELS:
            for trx_type in TRANSACTION_TYPES:
                cond = F.lit(True)
                cond &= F.col("t_minus") <= win  # Is row in the window?
                cond &= F.col("channel") == F.lit(ch_type)  # Does row have needed channel type?
                cond &= F.col("trx_type") == F.lit(trx_type)  # Does row have needed trx type?

                # Colname prefix
                col_prefix = f"{ch_type}_{trx_type}_{win}d"

                get_all_aggregations(col_prefix, cond, cols_list)

    result = data.groupBy("customer_id").agg(*cols_list)
    result.write.mode("overwrite").parquet("tmp_out")

    end_time = time.time()
    total_time = end_time - start_time
    print(f"[italic green]Total time: {total_time} seconds[/italic green]")

    # Write results
    print("[italic green]Dump results to JSON...[/italic green]")
    json_results = Path(__file__).parent.parent.joinpath("results").joinpath("results.json")
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
