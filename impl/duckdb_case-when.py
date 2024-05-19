import json
import shutil
import sys
import time
from pathlib import Path

import duckdb
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
NAME = "DuckDB case-when"


def generate_sql_query(col_prefix: str, sql_condition: str) -> str:
    sql_columns = f"""
        sum(case when {sql_condition} then 1 else 0 end) as '{col_prefix}_count',
        mean(case when {sql_condition} then trx_amnt else null end) as '{col_prefix}_mean',
        sum(case when {sql_condition} then trx_amnt else null end) as '{col_prefix}_sum',
        min(case when {sql_condition} then trx_amnt else null end) as '{col_prefix}_min',
        max(case when {sql_condition} then trx_amnt else null end) as '{col_prefix}_max'
    """
    return sql_columns


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
    shutil.rmtree("tmp_spill", ignore_errors=True)

    results_prefix = Path(__file__).parent.parent.joinpath("results")
    helper = BenchmarkWriter(NAME, task_size, results_prefix)

    #  #   Column       Dtype
    # ---  ------       -----
    #  0   customer_id  int64
    #  1   card_type    object
    #  2   trx_type     object
    #  3   channel      object
    #  4   trx_amnt     float64
    #  5   t_minus      int64
    #  6   part_col     category

    cols_list: list[str] = []

    for win in WINDOWS_IN_DAYS:
        for card_type in CARD_TYPES:
            for trx_type in TRANSACTION_TYPES:
                sql_condition = f"t_minus <= {win} "
                sql_condition += f"and card_type = '{card_type}' "
                sql_condition += f"and trx_type = '{trx_type}' "

                col_prefix = f"{card_type}_{trx_type}_{win}d"

                cols_list.append(generate_sql_query(col_prefix, sql_condition))

        for ch_type in CHANNELS:
            for trx_type in TRANSACTION_TYPES:
                sql_condition = f"t_minus <= {win} "
                sql_condition += f"and channel = '{ch_type}' "
                sql_condition += f"and trx_type = '{trx_type}' "

                col_prefix = f"{ch_type}_{trx_type}_{win}d"

                cols_list.append(generate_sql_query(col_prefix, sql_condition))
        # Iterate over combination card_type + trx_type
        # sql_list.append(generate_pivot_sql_query(win, ["card_type", "trx_type"]))
        # print(sql_list)

    sql = f"""
        set temp_directory = '../tmp_spill/';
        copy (
            select
                customer_id,
                {",".join(cols_list)}
            from read_parquet("./{path}/**/*.parquet")
            group by customer_id
        ) to '../tmp_out'
        (format 'parquet', compression 'zstd');
    """

    # Start the work
    helper.before()

    duckdb.connect(":memory:").execute(sql)

    total_time = helper.after()
    print(f"[italic green]Total time: {total_time} seconds[/italic green]")
    sys.exit(0)
