![](assets/logo.png)

## 简介

![](https://img.shields.io/badge/license-GPLv3-brightgreen)

**Tencent Transaction Processing Testbed System（简称3TS）**，是腾讯公司CynosDB（TDSQL）团队与中国人民大学数据工程与知识工程教育部重点实验室，联合研制的面向数据库事务处理的验证系统。该系统旨在通过设计和构建事务（包括分布式事务）处理统一框架，并通过框架提供的访问接口，方便使用者快速构建新的并发控制算法；通过验证系统提供的测试床，可以方便用户根据应用场景的需要，对目前主流的并发控制算法在相同的测试环境下进行公平的性能比较，选择一种最佳的并发控制算法。目前，验证系统已集成13种主流的并发控制算法，提供了TPC-C、PPS、YCSB等常见基准测试。3TS还进一步提供了一致性级别的测试基准，针对现阶段分布式数据库系统的井喷式发展而造成的系统选择难问题，提供一致性级别判别与性能测试比较。

如需更详细理解本项目目的，请参阅[3TS开源声明](doc/zh/announcement.md)。

## 特性

1. 支持三种history的生成方式：遍历生成、随机生成、从文本读取
2. 内置多种算法，包括：可串行化、冲突可串行化、SSI、BOCC、FOCC等
3. 支持从执行时间和回滚率两个角度评估算法开销

## 环境依赖

### 3TS-DA

- 支持**C++17**或以上版本的编译器（建议使用g++8）
- libconfig 1.7.2
- gflags 2.1.1
- gtest 1.6.0

### Deneva

- protobuf 3.9.1
- curl 7.29.0
- nanomsg 5.1.0

## 使用方法

- 执行`make.sh`编译代码，编译成功后会生成`3ts`二进制文件
- 执行`cp config.cfg.template config.cfg`，复制一份配置文件
- 执行`vi config.cfg`，填写配置，决定测试的行为
- 执行`./3TS --conf_path=config.cfg`，即可执行测试，测试完成后会在本地生成测试结果文件

## 基本原理

3TS框架由四部分构成：

- 运行模式（runner）：负责控制框架的行为。目前框架提供两种运行方式，具体使用何种运行方式，请在配置文件中的`Target`配置项下指定，如`Target = ["FilterRun"]`。
    - `FilterRun`：输出各个算法对各个history的检测结果，同时可以对检测结果进行筛选
    - `BenchmarkRun`：用于测试性能，输出不同事务数量、变量数量场景下，各个算法检测指定数量的history所需要消耗的时间
- 生成器（generator）：负责生成history。
- 算法（algorithm）：对生成器所生成的history进行检测。目前框架提供如下算法：
    - 可串行化检测算法（基于可串行化的定义，判断history是否满足可串行化条件，但判定**并发序列和串行序列的执行结果是否一致**的标准，以及**各个事务所采取的读策略**有所不同）：
        - 四种读策略下，以**各事务读集相同**和**各变量终态一致**为标准的，可串行化检测算法
            - `"SerializableAlgorithm_ALL_SAME_RU"` // 未提交读策略
            - `"SerializableAlgorithm_ALL_SAME_RC"` // 已提交读策略（读取最新的已提交版本，天然避免了由**脏读**异常带来的不可串行化情况）
            - `"SerializableAlgorithm_ALL_SAME_RR"` // 可重复读策略（当事务第二次读到某一变量时，取第一次读到的版本，天然避免了由**不可重复读**异常带来的不可串行化情况）
            - `"SerializableAlgorithm_ALL_SAME_SI"` // 快照读策略（天然避免了由**幻读**异常带来的不可串行化情况）
        - 四种读策略下，以**各提交状态事务读集相同**和**各变量终态一致**为标准的，可串行化检测算法
            - `"SerializableAlgorithm_COMMIT_SAME_RU"` // 未提交读策略
            - `"SerializableAlgorithm_COMMIT_SAME_RC"` // 已提交读策略
            - `"SerializableAlgorithm_COMMIT_SAME_RR"` // 可重复读策略
            - `"SerializableAlgorithm_COMMIT_SAME_SI"` // 快照读策略
        - 四种读策略下，仅以**各变量终态一致**为标准的，可串行化检测算法
            - `"SerializableAlgorithm_FINAL_SAME_RU"` // 未提交读策略
            - `"SerializableAlgorithm_FINAL_SAME_RC"` // 已提交读策略
            - `"SerializableAlgorithm_FINAL_SAME_RR"` // 可重复读策略
            - `"SerializableAlgorithm_FINAL_SAME_SI"` // 快照读策略
    - 冲突可串行化检测算法：`"ConflictSerializableAlgorithm"`
    - SSI检测算法：`"SSI"`
    - WSI检测算法：`"WSI"`
    - BOCC检测算法：`"BOCC"`
    - FOCC检测算法：`"FOCC"`
- 输出器（outputter）：对算法的检测结果进行相应统计，并输出到指定文件中。

**其余配置项说明，请参考配置文件相关注释。**

## 许可证

GPLv3 @ Tencent
