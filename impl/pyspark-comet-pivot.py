import multiprocessing
import shutil
import sys
from pathlib import Path

from data_generation.helpers import BenchmarkWriter, DatasetSizes
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column
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
NAME = "PySpark Comet pivot"


def transform_col(col: str, all_cols: set[str]) -> Column:
    cols_to_process = [col]
    if "_2w_" in col:
        cols_to_process.append(col.replace("_2w_", "_1w_"))
    elif "_3w_" in col:
        cols_to_process.append(col.replace("_3w_", "_1w_"))
        cols_to_process.append(col.replace("_3w_", "_2w_"))
    elif "_1m_" in col:
        cols_to_process.append(col.replace("_1m_", "_1w_"))
        cols_to_process.append(col.replace("_1m_", "_2w_"))
        cols_to_process.append(col.replace("_1m_", "_3w_"))
    elif "_3m_" in col:
        cols_to_process.append(col.replace("_3m_", "_1w_"))
        cols_to_process.append(col.replace("_3m_", "_2w_"))
        cols_to_process.append(col.replace("_3m_", "_3w_"))
        cols_to_process.append(col.replace("_3m_", "_1m_"))
    elif "_6m_" in col:
        cols_to_process.append(col.replace("_6m_", "_1w_"))
        cols_to_process.append(col.replace("_6m_", "_2w_"))
        cols_to_process.append(col.replace("_6m_", "_3w_"))
        cols_to_process.append(col.replace("_6m_", "_1m_"))
        cols_to_process.append(col.replace("_6m_", "_3m_"))
    elif "_1y_" in col:
        cols_to_process.append(col.replace("_1y_", "_1w_"))
        cols_to_process.append(col.replace("_1y_", "_2w_"))
        cols_to_process.append(col.replace("_1y_", "_3w_"))
        cols_to_process.append(col.replace("_1y_", "_1m_"))
        cols_to_process.append(col.replace("_1y_", "_3m_"))
        cols_to_process.append(col.replace("_1y_", "_6m_"))
    elif "_2y_" in col:
        cols_to_process.append(col.replace("_2y_", "_1w_"))
        cols_to_process.append(col.replace("_2y_", "_2w_"))
        cols_to_process.append(col.replace("_2y_", "_3w_"))
        cols_to_process.append(col.replace("_2y_", "_1m_"))
        cols_to_process.append(col.replace("_2y_", "_3m_"))
        cols_to_process.append(col.replace("_2y_", "_6m_"))
        cols_to_process.append(col.replace("_2y_", "_1y_"))

    cols_to_process = [c for c in cols_to_process if c in all_cols]
    if len(cols_to_process) == 1:
        return F.col(col).alias(col.replace("(trx_amnt)", ""))

    if ("_sum(" in col) or ("_count(" in col):
        return (sum([F.col(x) for x in cols_to_process])).alias(col.replace("(trx_amnt)", ""))
    elif "_max(" in col:
        return F.greatest(*cols_to_process).alias(col.replace("(trx_amnt)", ""))
    elif "_min(" in col:
        return F.least(*cols_to_process).alias(col.replace("(trx_amnt)", ""))
    else:
        return (sum([F.col(x) for x in cols_to_process]) / len(cols_to_process)).alias(col.replace("(trx_amnt)", ""))


def generate_pivoted_batch(df: DataFrame, groups: list[str]) -> DataFrame:
    # Partially inpsired by https://stackoverflow.com/a/73850575

    t_groups = groups + ["_win"]
    pivot_col = F.concat_ws("_", *t_groups)
    win_cols = ["1w", "2w", "3w", "1m", "3m", "6m", "1y", "2y"]
    if groups[0] == "card_type":
        pivot_values = [f"{ct}_{tt}_{ww}" for ct in CARD_TYPES for tt in TRANSACTION_TYPES for ww in win_cols]
    else:
        pivot_values = [f"{ch}_{tt}_{ww}" for ch in CHANNELS for tt in TRANSACTION_TYPES for ww in win_cols]

    tdf = (
        df.withColumn(
            "_win",
            F.when(F.col("t_minus") <= F.lit(7), F.lit("1w"))
            .when(F.col("t_minus") <= F.lit(14), F.lit("2w"))
            .when(F.col("t_minus") <= F.lit(21), F.lit("3w"))
            .when(F.col("t_minus") <= F.lit(30), F.lit("1m"))
            .when(F.col("t_minus") <= F.lit(90), F.lit("3m"))
            .when(F.col("t_minus") <= F.lit(180), F.lit("6m"))
            .when(F.col("t_minus") <= F.lit(360), F.lit("1y"))
            .when(F.col("t_minus") <= F.lit(720), F.lit("2y")),
        )
        .withColumn("_pivot", pivot_col)
        .groupBy("customer_id")
        .pivot("_pivot", pivot_values)
        .agg(
            F.count("trx_amnt"),
            F.sum("trx_amnt"),
            F.min("trx_amnt"),
            F.max("trx_amnt"),
            F.mean("trx_amnt"),
        )
    )
    columns_to_select = [F.col("customer_id")]
    all_cols = set(tdf.columns)
    for col in tdf.columns:
        if col == "customer_id":
            continue
        else:
            columns_to_select.append(transform_col(col, all_cols))

    return tdf.select(*columns_to_select)


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

    helper = BenchmarkWriter(NAME, task_size, results_prefix)
    # Start the work

    helper.before()

    available_cores = multiprocessing.cpu_count()
    spark = (
        SparkSession.builder.master("local[*]")
        .config("spark.driver.memory", "55g")
        .config("spark.executor.memory", "55g")
        .config("spark.driver.memoryOverheadFactor", "0.5")
        .config("spark.executor.memoryOverheadFactor", "0.5")
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
    part1 = generate_pivoted_batch(data, ["card_type", "trx_type"])
    part2 = generate_pivoted_batch(data, ["channel", "trx_type"])

    part1.join(part2, on=["customer_id"], how="inner").write.mode("overwrite").parquet("tmp_out")
    total_time = helper.after()
    print(f"[italic green]Total time: {total_time} seconds[/italic green]")
    sys.exit(0)
