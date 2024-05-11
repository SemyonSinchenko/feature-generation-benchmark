from enum import StrEnum, auto

import numpy as np
import pyarrow as pa
import typer
from pyarrow import parquet as pq
from rich.progress import track
from typing_extensions import Annotated

from .native import generate_data_batch  # Import native batch-generator

# Random constants
BINOMIAL_N = 100
BINOMIAL_P = 0.25


class DatasetSizes(StrEnum):
    TINY = auto()
    SMALL = auto()
    MEDIUM = auto()
    BIG = auto()


def main_function(
    prefix: Annotated[str, typer.Option(help="Output files prefix")],
    size: Annotated[
        DatasetSizes,
        typer.Option(help="Size of the dataset: 1K, 10K, 100K or 100M of customers, default is tiny"),
    ] = DatasetSizes.TINY,
    seed: Annotated[int, typer.Option(help="Random seed value, default is 42")] = 42,
) -> None:
    dim = -1
    partitions = -1
    days_in_partition = -1

    if size == DatasetSizes.TINY:
        # For that case we have:
        # 1000 customers
        # one partition is ~4 months
        # amount of partitions is 6 (~2 years)
        dim = 1e3
        partitions = 6
        days_in_partition = 120
    elif size == DatasetSizes.SMALL:
        # For that case we have:
        # 10_000 customers
        # one partition is ~2 months
        # amount of partitions is 12 (~2 years)
        dim = 1e4
        partitions = 12
        days_in_partition = 60
    elif size == DatasetSizes.MEDIUM:
        # For that case we have:
        # 100_000 customers
        # one partition is ~1 week
        # amount of partitions is 102 (~2 years)
        dim = 1e5
        partitions = 102
        days_in_partition = 7
    else:
        # For that case we have:
        # 100_000_000 customers
        # one partition is ~1 day
        # amount of partitions is 730 (~2 years)
        dim = 1e8
        partitions = 730
        days_in_partition = 1

    dim = int(dim)

    # Customers are encoded by simple range
    customers = pa.array(np.arange(dim))

    # Expected amount of transactions per day is sampled from binomial distribution
    rng = np.random.default_rng(seed)
    expected_amounts = pa.array(rng.binomial(BINOMIAL_N, BINOMIAL_P, dim))

    # We will generate all the seed once; one seed per partition
    seeds = rng.integers(0, 1_000_000, partitions)
    for partition in track(range(partitions), description="Generate partitions..."):
        offset = partition * days_in_partition

        # For details of the implementation see lib.rs
        # Signature of generate_data_batch:
        # ids: PyArrowType<ArrayData>,
        # trx_per_day: PyArrowType<ArrayData>,
        # days_in_batch: i64,
        # offset: i64,
        # global_seed: u64,
        # partition_name: &str,
        batch: pa.RecordBatch = generate_data_batch(
            customers,
            expected_amounts,
            days_in_partition,
            offset,
            seeds[partition],
            f"partition_{partition}",
        )

        # Resulted schema:
        # let schema = Schema::new(vec![
        # Field::new("customer_id", DataType::Int64, false),
        # Field::new("card_type", DataType::Utf8, false),
        # Field::new("trx_type", DataType::Utf8, false),
        # Field::new("channel", DataType::Utf8, false),
        # Field::new("trx_amnt", DataType::Float64, false),
        # Field::new("t_minus", DataType::Int64, false),
        # Field::new("part_col", DataType::Utf8, false), // partition column
        # ]);
        pq.write_to_dataset(
            batch,
            prefix,
            partition_cols=["part_col"],
            existing_data_behavior="overwrite_or_ignore",
        )


def entry_point() -> None:
    typer.run(main_function)
