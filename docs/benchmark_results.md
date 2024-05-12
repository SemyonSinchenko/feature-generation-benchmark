# Results of Benchmark

## Setup

My private Laptop for now:

- 13th Gen Intel(R) Core(TM) i5-1335U;
- 16Gb of RAM;
- Fedora 40, Kernel 6.8

TODO: switch to GHA runners.

## Tiny Dataset

**Amount of rows:** 17,299,455

**Size on disk:** 178 Mb

**Unique IDs:** 1,000

| Tool | Approach  | Time of processing in seconds |
| ---- | --------  | ----------------------------- |
| Pandas | Pivot | 13.05 |
| PySpark | Case-When | 289.07 |


## Small Dataset

**Amount of rows:** 172,925,732

**Size on disk:** 1.8 Gb

**Unique IDs:** 10,000

| Tool | Approach  | Time of processing in seconds |
| ---- | --------- | ----------------------------- |
| Pandas | Pivot | OOM |
| PySpark | Case-When | 2206.98 |
