# Results of Benchmark

![Results](https://raw.githubusercontent.com/SemyonSinchenko/feature-generation-benchmark/main/docs/static/results_overview.png)

## Setup

**EC2 m5.4xlarge**

| Key                   | Value                    |
|-----------------------|--------------------------|
| vCPUs                 | 16                       |
| Memory (GiB)          | 64.0                     |
| Memory per vCPU (GiB) | 4.0                      |
| Physical Processor    | Intel Xeon Platinum 8175 |
| Clock Speed (GHz)     | 3.1                      |
| CPU Architecture      | x86_64                   |
| Disk space            | 256 Gb                   |

[Details about the instance type](https://instances.vantage.sh/aws/ec2/m5.4xlarge)

## Datasets

All the information provided for a default seed 42. Size on disk is a total size of compressed parquet files. An amount of rows depends of SEED that was used for generation of the data because an amount of transactions for each id (customer id) per day is sampled from binomial distribution.

See `src/lib.rs` for details of the implementation.

## Tiny Dataset

**Amount of rows:** 17,299,455

**Size on disk:** 178 Mb

**Unique IDs:** 1,000

| Tool | Time of processing in seconds |
| ---- | ----------------------------- |
| PySpark pandas-udf | 78.31 |
| PySpark case-when | 242.84 |
| Pandas pivot | 23.91 |
| Polars pivot | 4.54 |
| DuckDB pivot | 4.10 |
| DuckDB case-when | 36.59 |
| PySpark Comet case-when | 94.06 |
| PySpark-4 polars-udf | 53.06 |
| PySpark pivot | 104.21 |
| PySpark Comet pivot | 106.69 |


## Small Dataset

**Amount of rows:** 172,925,732

**Size on disk:** 1.8 Gb

**Unique IDs:** 10,000

| Tool | Time of processing in seconds |
| ---- | ----------------------------- |
| Pandas pivot | OOM |
| Polars pivot | OOM |
| DuckDB pivot | 2181.59 |
| PySpark pandas-udf | 5983.14 |
| PySpark case-when | 17653.46 |
| PySpark Comet case-when | 4873.54 |
| PySpark-4 polars-udf | 4704.73 |
| PySpark pivot | 455.49 |
| PySpark Comet pivot | 412.17 |



## Medium Dataset

**Amount of rows:** 1,717,414,863

**Size on disk:** 18 Gb

**Unique IDs:** 100,000

| Tool | Time of processing in seconds |
| ---- | ----------------------------- |
| Pandas pivot | OOM |
| Polars pivot | OOM |
| DuckDB pivot | 2181.59 |
| PySpark pandas-udf | 5983.14 |
| PySpark case-when | 17653.46 |
| PySpark Comet case-when | 4873.54 |
| PySpark-4 polars-udf | 4704.73 |
| PySpark pivot | 455.49 |
