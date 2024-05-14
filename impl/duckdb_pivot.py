import json
import shutil
import sys
import time
from pathlib import Path

import duckdb
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
ENGINE_NAME = "DuckDB"
APPROACH_NAME = "Pivot"


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
    shutil.rmtree("tmp_spill", ignore_errors=True)

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
    start_time = time.time()

    duckdb.connect(":memory:").execute(sql)

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
