![](assets/logo.png)

## Introduction

![](https://img.shields.io/badge/license-GPLv3-brightgreen)

**Tencent Transaction Processing Testbed System (3TS)** that is jointly developed by Tencent's TDSQL team and the Key Laboratory of Data Engineering and Knowledge Engineering of the Ministry of Education of Renmin University of China. The system aims to design and construct a unified framework for transaction processing (including distributed transactions). It enables users to quickly build new concurrency control approaches via the access interface provided by the framework. Based on an comprehensive experiment study over the benchmarks, and the applications abstracted, users can select an optimal concurrency control approach. At present, 3TS has been integrated 13 mainstream concurrency control approaches, and provides common benchmarks such as TPC-C，PPS and YCSB. 3TS further provides a consistency level test benchmark, to address the issue of system selection difficulty caused by the blowout development of distributed database systems, and provides consistency level discrimination and performance test comparison.

If you want to better understand the aims of our project, please view [3TS opensource announcement](doc/en/announcement.md).

## Features

1. Support three ways to generate histories: Traversing Generating, Randomly Generating, Generating From the Text File.
2. Built-in multiple algorithms, including Serializable Algorithm, Conflict Serializable Algorithm, SSI, BOCC, FOCC, etc. 
3. Support evaluating algorithm cost from the execution time and rollback rate two perspectives.

## Dependence

### 3TS-Coo
A consistency verification system. Please check out the newest update on branch ['coo-consistency-check'](https://github.com/Tencent/3TS/tree/coo-consistency-check).

We update our result on [report webpage](https://axingguchen.github.io/3TS/).

### 3TS-DA
A static random history generator.  Please check out the newest update on branch ['dev'](https://github.com/Tencent/3TS/tree/dev).

- a compilter supporting C++17 or upper versions (recommend g++8)
- libconfig 1.7.2
- gflags 2.1.1
- gtest 1.6.0

### Deneva
A performance verification system. A consistency verification system. Please check out the newest update on branch ['dev'](https://github.com/Tencent/3TS/tree/dev).

- protobuf 3.9.1
- curl 7.29.0
- nanomsg 5.1.0

## Usage

- Run `make.sh` to compile the code. The `3TS` binary will be generated if compiling successfully.
- Run `cp config/config.cfg.template config.cfg` to copy the configuration file.
- Run `vi config.cfg` to modify the configuration file to determine the behavior of the testbed.
- Run `./3TS --conf_path=config.cfg` to execute test. The test result file will be generated when test is over.

## Principle

3TS framework can be divided into four parts:

- Runner: To determine the behavior of the testbed. The testbed now supports two runners. Please specify the runner behind the `Target` configuration item, e.g. `Target = ["FilterRun"]`.
  - `FilterRun`: To output the detection result from each algorithms with each history and the result can be filtered.
  - `BenchmarkRun`: To test performance by outputting the time cost of each algorithm detecting anomalies from the same number of histories in different transaction numbers and variable item numbers. 
- Generator: To generate histories.
- Algorithm: To detect anomalies in each history generated by Generator. The testbed supports following algorithms:
  - Serializable Algorithm (Judge whether the history is serializable or not based on the definition of serializable. But the standard to **check the consistency between the execution results of concurrent history and serialized history** and **the read strategy of each transaction** are different.):
    - The Serializable Algorithms based on standard that **every correspond transactions' read set** and **every correspond variable items' final version** are all same in the four read strategies.
      - `"SerializableAlgorithm_ALL_SAME_RU"` // uncommitted read strategy
      - `"SerializableAlgorithm_ALL_SAME_RC"` //  committed read strategy (read the latest committed version by which naturally avoid the unserializable cases caused by **dirty read** anomaly)
      - `"SerializableAlgorithm_ALL_SAME_RR"` // repeatable read strategy (read the previous version when second reading the same variable item in the same transaction by which naturally avoid the unserializable cases caused by **non-repeatable read anomaly**)
      - `"SerializableAlgorithm_ALL_SAME_SI"` // snapshot read strategy (naturally avoid the unserializable cases caused by **phantom anomaly**)
    - The Serializable Algorithms based on standard that **the every committed correspond transactions' read set** and **every correspond  variable items' final version** are all same in the four read strategies.
      - `"SerializableAlgorithm_COMMIT_SAME_RU"` // uncommitted read strategy
      - `"SerializableAlgorithm_COMMIT_SAME_RC"` // committed read strategy
      - `"SerializableAlgorithm_COMMIT_SAME_RR"` // repeatable read strategy
      - `"SerializableAlgorithm_COMMIT_SAME_SI"` // snapshot read strategy
    - The Serializable Algorithms based on standard that only **the every correspond  variable items' final version** are same in the four read strategies.
      - `"SerializableAlgorithm_FINAL_SAME_RU"` // uncommitted read strategy
      - `"SerializableAlgorithm_FINAL_SAME_RC"` // committed read strategy
      - `"SerializableAlgorithm_FINAL_SAME_RR"` // repeatable read strategy
      - `"SerializableAlgorithm_FINAL_SAME_SI"` // snapshot read strategy
  - Conflict Serializable Algorithm：`"ConflictSerializableAlgorithm"`
  - Dynamic Line Intersect (Identify Anomaly): `"DLI_IDENTIFY"`
  - Serializable Snapshot Isolation：`"SSI"`
  - Write-Snapshot Isolation：`"WSI"`
  - Backward Optimistic Concurrency Control：`"BOCC"`
  - Forward Optimistic Concurrency Control：`"FOCC"`

- Outputter: To statistics the results and output to the specific file.

**For explanations to other configuration items, please view the comments in configuration file.**

## License

GPLv3 @ Tencent
