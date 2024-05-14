import shutil
import sys
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
NAME = "DuckDB pivot"


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

    sql = f"""
        with base_table as (
            select
                *
            from
                read_parquet("./{path}/**/*.parquet")
            ),
        all_windows as (
            {
                " union all ".join(
                    [
                        f"select *, '{win}' as t_minus from base_table where t_minus <= {win}" for win in WINDOWS_IN_DAYS
                    ]
                )
            }
        ),
        card_type_agg as (
            pivot all_windows
            on card_type, trx_type, t_minus_1
            using count(trx_amnt) as 'd_count', mean(trx_amnt) as 'd_mean', sum(trx_amnt) as 'd_sum', min(trx_amnt) as 'd_min', max(trx_amnt) as 'd_max'
            group by customer_id
        ),
        channel_type_agg as ( 
            pivot all_windows
            on channel, trx_type, t_minus_1
            using count(trx_amnt) as 'd_count', mean(trx_amnt) as 'd_mean', sum(trx_amnt) as 'd_sum', min(trx_amnt) as 'd_min', max(trx_amnt) as 'd_max'
            group by customer_id 
        )
        select
            a.*,
            b.* exclude(customer_id)
        from card_type_agg a
        full join channel_type_agg b
            on a.customer_id = b.customer_id
    """

    sql = f"""
        set temp_directory = '../tmp_spill/';
        copy (
            {sql}
        ) to '../tmp_out'
        (format 'parquet', compression 'zstd');
    """
    # Start the work
    helper.before()

    duckdb.connect(":memory:").execute(sql)

    total_time = helper.after()
    print(f"[italic green]Total time: {total_time} seconds[/italic green]")
    sys.exit(0)
