import multiprocessing
import shutil
import sys
from pathlib import Path

import polars as pl
import pyarrow as pa
from data_generation.helpers import BenchmarkWriter, DatasetSizes
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StructField, StructType, LongType
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
NAME = "PySpark-4 polars-udf"


def generate_pivoted_batch(data: pl.DataFrame, t_minus: int, groups: list[str]) -> pl.DataFrame:
    pivoted = (
        data.filter(pl.col("t_minus") <= t_minus)
        .group_by(["customer_id"] + groups)
        .agg(
            [
                pl.count("trx_amnt").cast(pl.Int64).alias("count"),
                pl.mean("trx_amnt").alias("mean"),
                pl.sum("trx_amnt").alias("sum"),
                pl.min("trx_amnt").alias("min"),
                pl.max("trx_amnt").alias("max"),
            ]
        )
        .pivot(index="customer_id", columns=groups, values=["count", "mean", "sum", "min", "max"])
    )

    # Polars make some nasty looking column names when pivoting.
    pivoted = pivoted.rename(
        {
            column: (
                column.split("_")[-1].replace("{", "").replace("}", "").replace('"', "").replace(",", "_")
                + f"_{t_minus}d_"
                + column.split("_")[0]
            )
            for column in pivoted.columns
            if column != "customer_id"
        }
    )
    return pivoted


def get_processing_func(expected_cols: list[str]):
    def generate_all_aggregations(data: pa.Table) -> pa.Table:
        pl_df = pl.from_arrow(data)
        assert isinstance(pl_df, pl.DataFrame)
        dfs_list: list[pl.DataFrame] = []

        for win in WINDOWS_IN_DAYS:
            # Iterate over combination card_type + trx_type
            dfs_list.append(generate_pivoted_batch(pl_df, win, ["card_type", "trx_type"]))

            # Iterate over combination channel + trx_type
            dfs_list.append(generate_pivoted_batch(pl_df, win, ["channel", "trx_type"]))

        out_df = pl.concat(dfs_list, how="align")
        if len(out_df.columns) < len(expected_cols):
            for field in expected_cols:
                if field not in out_df.columns:
                    out_df[field] = pl.Series(dtype=pl.Float64)

        return out_df.to_arrow()

    return generate_all_aggregations


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

    # Generating the output schema definition
    fields = [StructField("customer_id", LongType())]

    for win in WINDOWS_IN_DAYS:
        for first_key in CARD_TYPES:
            for second_key in TRANSACTION_TYPES:
                fields.append(StructField(f"{first_key}_{second_key}_{win}d_count", LongType()))
                fields.append(StructField(f"{first_key}_{second_key}_{win}d_mean", DoubleType()))
                fields.append(StructField(f"{first_key}_{second_key}_{win}d_min", DoubleType()))
                fields.append(StructField(f"{first_key}_{second_key}_{win}d_max", DoubleType()))
                fields.append(StructField(f"{first_key}_{second_key}_{win}d_sum", DoubleType()))

        for first_key in CHANNELS:
            for second_key in TRANSACTION_TYPES:
                fields.append(StructField(f"{first_key}_{second_key}_{win}d_count", LongType()))
                fields.append(StructField(f"{first_key}_{second_key}_{win}d_mean", DoubleType()))
                fields.append(StructField(f"{first_key}_{second_key}_{win}d_min", DoubleType()))
                fields.append(StructField(f"{first_key}_{second_key}_{win}d_max", DoubleType()))
                fields.append(StructField(f"{first_key}_{second_key}_{win}d_sum", DoubleType()))

    schema = StructType(fields)
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

    processing_fun = get_processing_func(schema.fieldNames())
    result = data.groupBy("customer_id").applyInArrow(processing_fun, schema)
    result.write.mode("overwrite").parquet("tmp_out")

    total_time = helper.after()
    print(f"[italic green]Total time: {total_time} seconds[/italic green]")
    sys.exit(0)
