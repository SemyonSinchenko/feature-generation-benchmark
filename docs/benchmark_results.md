# Results of Benchmark

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

[Details about the instance type](https://instances.vantage.sh/aws/ec2/m5.4xlarge)

## Datasets

All the information provided for a default seed 42. Size on disk is a total size of compressed parquet files. An amount of rows depends of SEED that was used for generation of the data because an amount of transactions for each id (customer id) per day is sampled from binomial distribution.

See `src/lib.rs` for details of the implementation.

## Tiny Dataset

**Amount of rows:** 17,299,455

**Size on disk:** 178 Mb

**Unique IDs:** 1,000

| Tool | Approach  | Time of processing in seconds |
| ---- | --------  | ----------------------------- |
| Pandas | Pivot | 24.12 |
| PySpark | Case-When | 233.97 |
| Polars | PivotEager | 4.61 |
| Duckdb | Case-When | 35.51 |


## Small Dataset

**Amount of rows:** 172,925,732

**Size on disk:** 1.8 Gb

**Unique IDs:** 10,000

| Tool | Approach  | Time of processing in seconds |
| ---- | --------- | ----------------------------- |
| Pandas | Pivot | 214.51 |
| PySpark | Case-When | 1782.17 |
| Polars | PivotEager | 52.61 |
| Duckdb | Case-When | 295.63 |



## Medium Dataset

**Amount of rows:** 1,717,414,863

**Size on disk:** 18 Gb

**Unique IDs:** 100,000

| Tool | Approach  | Time of processing in seconds |
| ---- | --------- | ----------------------------- |
| Pandas | Pivot | OOM |
| PySpark | Case-When | 17061.94 |
| Polars | PivotEager | OOM |
| Duckdb | Case-When | OOM |
