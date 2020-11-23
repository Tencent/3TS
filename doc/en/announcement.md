![logo](../../assets/logo.png)

**[open source announcement] Tencent Transaction Processing Testbed System, 3TS**

## 1 Project Description

Tencent Transaction Processing Testbed System (3TS) that is jointly developed by Tencent's CynosDB (TDSQL) team and the Key Laboratory of Data Engineering and Knowledge Engineering of the Ministry of Education of Renmin University of China. The system aims to design and construct a unified framework for transaction processing (including distributed transactions). It enables users to quickly build new concurrency control approaches via the access interface provided by the framework. Based on an comprehensive experiment study over the benchmarks, and the applications abstracted, users can select an optimal concurrency control approach. At present, 3TS has been integrated 13 mainstream concurrency control approaches, and provides common benchmarks such as TPC-C、PPS and YCSB.. 3TS further provides a consistency level test benchmark, to address the issue of system selection difficulty caused by the blowout development of distributed database systems, and provides consistency level discrimination and performance test comparison.

In the near future, 3TS will deeply explore the theory and implementation technology of database transaction processing, and its core concepts are: OPEN, DEPTH and EVOLUTION. Open, adhering to the heart of open source, sharing knowledge and technology; Depth, practicing the spirit of systematic researching the essence of transaction processing technology, "We will never return until we defeat the enemy.”. Evolution, keep moving forward, “Although the road is endless and faraway, we still want to pursue the truth in the world."

As a framework related to transaction processing technology, the essential issues that 3TS is committed to exploring mainly include:

1. What factors will affect the transaction processing technology of distributed database(Availability? Reliability? Security? Consistency? Scalability? Function? Performance? Architecture? New hardware？ AI？... )
2. How to establish the evaluation and evaluation system of distributed transactional database system?
3. How many kinds of data anomalies are there in the world? How to establish the systematic research method for data anomalies?
4. Why are there so many concurrent access control algorithms? Is there any essential relationship between various concurrent access control algorithms?

3TS conducts systematic research from the following seven aspects:

1. Exploration of data anomalies.
2. Concurrent access control algorithms (mainstream concurrent access control technologies and a variety of advanced concurrency technologies): evaluation, optimization and innovation.
3. Multi-level consistency: under the integration of shared object consistency (or distributed consistency) and transaction consistency, explore a variety of strong consistency and weak consistency. What is the essential relationship between the isolation levels of database transactions and the various consistency models of shared objects? Can they be effectively integrated (instead of simply using "strict serializability" to associate "serializability" with "Linearizability")?
4. Highly available transactions: We will discuss relationships between Multi-level consistency and CAP. In the face of the high availability requirements of 7*24, how should the transaction mechanism of distributed database be constructed? Can strong consistency and high availability really coexist in the context of network partitioning?
5. Transaction processing architecture: How to decoup transaction processing module and other modules under the background of storage level and calculation level separation? Is the transaction processing module correct and efficient when it is placed on the client side, or on the middleware side, or is it closely coupled with storage modules on the server side?
6. AI, new hardware and transactions: We will explore the relationship between AI technology, new hardware and transaction processing technology. 
7. Evaluation system of distributed transaction database: How to establish the index to evaluate the transaction processing technology of a database system? Is the existing architecture adequate? (the TPC-C standard cannot override data anomalies such as Write skew and thus cannot verify serializable isolation levels. YCSB? Sysbench? J-Meter? …)? Or is there a more better standards?

## 2 System Characteristics

Transaction processing performance testbed system. Data management is undergoing changes from SQL to NoSQL, from NoSQL to NewSQL. Although NoSQL has the characteristics of high availability and high scalability, it sacrifices transaction processing characteristics and cannot adapt to current mission-critical applications, even to the case under the most basic social network behaviors. In the latter case, user A and B are becoming mutual friends. This behavior involves two operations: adding A as a friend of B, and adding B as a friend of A. These two operations are considered as an inseparable whole. Without transaction, such a simple social network application cannot be well supported in NoSQL systems. Instead, NewSQL inherits the advantages of relational databases and retains the high availability and high scalability characteristics of NoSQL. We remark that NewSQL is the goal of the current development of distributed database technology. However, building a NewSQL system is extremely challenging. Among them, the construction of distributed transactions with high performance and high scalability is one of the most difficult problems. To address this problem in practice, 3TS provides a development framework for distributed transaction concurrency control approaches, including locking, optimistic concurrency control (OCC), timestamp (TO) and multiple versions to achieve. 3TS has been integrated with the current mainstream. 13 Concurrency control approaches, and provides three general benchmarks, including TPC-C, PPS, YCSB. Through the access interfaces provided by 3TS, users can develop new concurrency control approaches and compare their performance with the current mainstream concurrency control approaches. 3TS aims to provide a practical framework for database system kernel developers and university students who are interested in database research, and help to train key technical personnel for database field. 

The concurrency operation leads to different data anomalies in the fields of shared object (memory), database system, and distributed storage system, etc. Transaction technology have been studied systematically for decades, many sub-fields have achieved fruitful results. However, some contents have not been studied thoroughly, and there is a "case by case" research method, which makes the object of study lack of unity and integrity.

Among them, the most typical is the database transaction processing technology, which takes ACID as the technical feature, which aims to ensure the consistency of the data operated by the concurrent transaction, that is, **the concurrent transaction scheduling can be serialized (the data has no "data anomalies")**. However, the basic question of how many kinds of data anomalies there are in this world is not known. On this problem, the limitation of "case by case" research method is more significant, only individual data anomalies are found, but the relationship between known data anomalies is not known. However, the definition of isolation levels and design concurrent access control algorithms depends on the known limited data anomalies.

In addition, with the arrival of the era of big data, distributed database has moved from the research field to the engineering practice field. A logical system is divided into subsystems of N entities and loses the logic monotonicity of cohesion, which makes the consistency of distributed transaction databases generalize into C of ACID and C of CAP. The serializable technology does not take into account the causal relationship between concurrent transactions and is not perfect. This makes a good opportunity for the research of distributed transaction database, that is, what is the relationship between data anomalies and two kinds of consistency(C of ACID and C of CAP)? Is it enough for current concurrent access control algorithms? There are some consistent studies in the academia and industry, but there is not a unified understanding.

## 3 System Objective

3TS is not an engineering system, but a research system. In this system, there are many subsystems, and more subsystems will be extended according to the aforementioned research scope. At present, 3TS includes Tencent's self-developed transaction testing system and 3TS-Deneva subsystem. In the 3TS, many concurrent access control algorithms are supported, include serializability, conflict serializability, SSI, WSI, BOCC, FOCC, Sundial/Tictoc, Maat, OCC, Silo, TO, Locking (No Wait, Wait Die), Calvin and so on. More concurrent access control algorithms and other related technologies will be supported in the future.

We will provide more detailed documentation describing the technical content included in 3TS.

## 4 Future plans

We will update the follow-up R&D plan one after another, and give a detailed description of each item in the plan.

Short-term plans: 

1. Add more new CC algorithms into 3TS and optimize them continuously. 
2. Implement multiple isolation levels for 3TS's concurrency algorithm. 
3. Add the global clock function for 3TS to ensure distributed clock synchronization. 

## 5 Sincerely look forward to making open source contribution together

We expect that 3TS is a bridge that can help everyone in the academia and industry to master database transaction processing technology quickly, in depth and breadth. We also hope us to carry out research and make progress together. .

3TS is a young penguin, looking forward to growing up together and efforts of you and me.
