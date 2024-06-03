import contextlib
import multiprocessing
import shutil
import sys
from pathlib import Path

from data_generation.helpers import BenchmarkWriter, DatasetSizes
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
    360,  # year
    720,  # two years
)

# Information for result
NAME = "PySpark Comet case-when"


def get_raw_cols(col_prefix: str, cond: Column, raw_cols: list[Column]) -> None:
    raw_cols.append(F.when(cond, F.lit(1)).otherwise(F.lit(0)).alias(f"{col_prefix}_flag"))
    raw_cols.append(F.when(cond, F.col("trx_amnt")).otherwise(F.lit(None)).alias(f"{col_prefix}_or_none"))
    raw_cols.append(F.when(cond, F.col("trx_amnt")).otherwise(F.lit(0)).alias(f"{col_prefix}_or_zero"))


def get_all_aggregations(col_prefix: str, cols_list: list[Column]) -> None:
    # Count over group
    cols_list.append(F.sum(F.col(f"{col_prefix}_flag")).alias(f"{col_prefix}_count"))
    # Average over group
    cols_list.append(F.mean(F.col(f"{col_prefix}_or_none")).alias(f"{col_prefix}_avg"))
    # Sum over group
    cols_list.append(F.sum(F.col(f"{col_prefix}_or_zero")).alias(f"{col_prefix}_sum"))
    # Min over group
    cols_list.append(F.min(F.col(f"{col_prefix}_or_none")).alias(f"{col_prefix}_min"))
    # Max over group
    cols_list.append(F.max(F.col(f"{col_prefix}_or_none")).alias(f"{col_prefix}_max"))


if __name__ == "__main__":
    path = sys.argv[1]
    results_prefix = Path(__file__).parent.parent.joinpath("results")
    shutil.rmtree("tmp_out", ignore_errors=True)

    if "tiny" in path:
        task_size = DatasetSizes.TINY
    elif "small" in path:
        task_size = DatasetSizes.SMALL
    elif "medium" in path:
        task_size = DatasetSizes.MEDIUM
    else:
        task_size = DatasetSizes.BIG

    # Before we go we save the information for the case of OOM
    helper = BenchmarkWriter(NAME, task_size, results_prefix)
    # Start the work
    helper.before()

    # Partially inspired by:
    # 1. https://github.com/h2oai/db-benchmark/blob/master/spark/groupby-spark.py
    # 2. https://github.com/MrPowers/quinn/issues/143
    available_cores = multiprocessing.cpu_count()
    spark = (
        SparkSession.builder.master("local[*]")
        .config("spark.driver.memory", "60g")
        .config("spark.executor.memory", "60g")
        .config("spark.sql.shuffle.partitions", f"{available_cores}")
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

    cols_list = [F.lit("customer_id")]
    agg_cols_list = []
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

                get_raw_cols(col_prefix, cond, cols_list)
                get_all_aggregations(col_prefix, agg_cols_list)

        # Iterate over combination channel + trx_type
        for ch_type in CHANNELS:
            for trx_type in TRANSACTION_TYPES:
                cond = F.lit(True)
                cond &= F.col("t_minus") <= win  # Is row in the window?
                cond &= F.col("channel") == F.lit(ch_type)  # Does row have needed channel type?
                cond &= F.col("trx_type") == F.lit(trx_type)  # Does row have needed trx type?

                # Colname prefix
                col_prefix = f"{ch_type}_{trx_type}_{win}d"

                get_raw_cols(col_prefix, cond, cols_list)
                get_all_aggregations(col_prefix, agg_cols_list)

    result = data.select(*cols_list).groupBy("customer_id").agg(*agg_cols_list)
    result.write.mode("overwrite").parquet("tmp_out")

    total_time = helper.after()
    print(f"[italic green]Total time: {total_time} seconds[/italic green]")
    sys.exit(0)
