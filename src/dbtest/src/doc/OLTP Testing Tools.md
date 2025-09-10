# OLTP Testing Tools
## Summary
| Tool | Databases | Isolation | Data Model |
| --- | --- | --- | --- |
| [Cobra](https://www.usenix.org/system/files/osdi20-tan.pdf) | RocksDB, PostgreSQL, FaunaDB*| SER | KV |
| [PolySI](https://dl.acm.org/doi/10.14778/3583140.3583145) | PostgreSQL, Dgraph*, CockroachDB* | SI | KV |
| [Viper](https://dl.acm.org/doi/10.1145/3552326.3567492) | TiDB*, SQLServer, MongoDB* | SI | KV, list |
| [DBCop](https://dl.acm.org/doi/10.1145/3360591) | CockroachDB*, Galera*, AntidoteDB | SER, SI, TCC | KV |
| [Elle](https://www.vldb.org/pvldb/vol14/p268-alvaro.pdf) | YugabyteDB*, Dgraph*, FaunaDB* | SER, SI, RC | KV, list|
| [MTC](https://www.computer.org/csdl/proceedings-article/icde/2025/360300d998/26FZCeQ3uJW) | PostgreSQL*, MongoDB*, Cassandra* | SER, SI | KV |
| [Plume](https://dl.acm.org/doi/10.1145/3689742) | PostgreSQL, AntidoteDB*, MongoDB* | TCC, RA, RC | KV, list |
| [AWDIT](https://dl.acm.org/doi/10.1145/3742465) | PostgreSQL*, CockroachDB*, RocksDB | TCC, RA, RC | KV |
| [IsoVista](https://dl.acm.org/doi/10.14778/3685800.3685866) | PostgreSQL*, MySQL, MariaDB | SER, SI, TCC, RA, RC | KV, list |
| [Vbox](https://arxiv.org/abs/2503.05163) | PostgreSQL, MySQL* | SER | KV, predicate |
| [Leopard](https://ieeexplore.ieee.org/document/10184872) | OceanBase*, TiDB*, OpenGauss* | SER, SI, RR, RC | SQL |
| [Troubadour](https://dl.acm.org/doi/abs/10.1145/3720504) | PostgreSQL, MySQL*, TiDB* | SER, SI, RC | SQL |
| [GRAIL](https://link.springer.com/chapter/10.1007/978-3-031-64285-2_11) | ArangoDB, Neo4j* | SER, SI, PL-2, PL-1 | KV, list |
| [Emme](https://dl.acm.org/doi/10.1145/3627703.3650080) | CockroachDB*, TiDB, PostgreSQL* | SER, SI | KV, predicate |
| [Chronos & Aion](https://www.computer.org/csdl/proceedings-article/icde/2025/360300d738/26FZC0ImYsU) | Dgraph*, TiDB, YugabyteDB* | SER, SI | KV, list |

* databases are selected: not all databases are listed (no more than three)
* the star after a database means there is some kind of violation found in that database
* the violation can be either artificial (synthetic or injected) or read-world (existing or newly discovered)
* the listed databases may be runned elsewhere before checking its history (e.g. downloaded from a bug report)
* the tools can apply to these databases and check the histories produced by them
* the tools may apply to other databases
* variants of a popular isolation level are omitted

## Black-box History Checking Tools
### Cobra
link: https://www.usenix.org/system/files/osdi20-tan.pdf

databases: Google Cloud Datastore, RocksDB, PostgreSQL, YugabyteDB, CockroachDB, FaunaDB

isolation: Serializability

data model: key-value store

violation: G2 in YugabyteDB and CockroachDB, Disappearing writes in YugabyteDB, Read uncommitted in CockroachDB and Read skew in FaunaDB

description: Cobra uses polygraph to search cycles in a dependency graph. It introduces several optimizations including combining writes, coalescing and pruning constraints, garbage collection and GPU acceleration. It also supports checking continuous and ever-growing histories.

### PolySI
link: https://dl.acm.org/doi/10.14778/3583140.3583145

databases: PostgreSQL, Dgraph, MariaDB-Galera, YugabyteDB, CockroachDB, MySQL-Galera

isolation: Snapshot Isolation

data model: key-value store

violation: in Dgraph, MariaDB-Galera, YugabyteDB, CockroachDB, MySQL-Galera

description: PolySI introduces the concept of generalized polygraph by merging constraints in a polygraph. By computing the composition of some types of relations, it makes the SI checking problem to be a cycle-search problem, no need to match a cycle of some pattern.

### Viper
link: https://dl.acm.org/doi/10.1145/3552326.3567492

databases: TiDB, SQLServer, YugabyteDB, MongoDB

isolation: Snapshot Isolation

data model: key-value store, list, range query

violation: Lost update, Aborted read, G1c and Read future writes in MonogoDB and Read skew in TiDB

description: Like PolySI, Viper also makes the SI checking problem to be a cycle-search problem, by introduing BC-polygraph, which divides one transaction into two nodes (begin and commit). It uses heuristics to guess the order in which transactions occur to guide searching.

### DBCop
link: https://dl.acm.org/doi/10.1145/3360591

databases: CockroachDB, Galera, AntidoteDB

isolation: Serializability, Snapshot Isolation, Transactional Causal Consistency

data model: key-value store

violation: various violations from Read Atomic to Serializability in CockroachDB, various violations from Read Committed to Snapshot Isolation in Galera

description: The main contribution of this work is to give the complexity of checking SER, SI and TCC. DBCop is developed to show the complexity is correct following the checking algorithms introduced before.

### Elle
link: https://www.vldb.org/pvldb/vol14/p268-alvaro.pdf

databases: TiDB, YugabyteDB, Dgraph, FaunaDB

isolation: Serializability, Snapshot Isolation, Read Committed

data model: key-value store, list

violation: various violations in TiDB, YugabyteDB, Dgraph and FaunaDB

description: Elle infers an Adya-style dependency graph between transaction by selecting database objects and operations when generating histories, to ensure the results of read reveal information about their version. Lists with append can benefit this inferrence.

### MTC
link: https://www.computer.org/csdl/proceedings-article/icde/2025/360300d998/26FZCeQ3uJW

databases: PostgreSQL, MongoDB, MariaDB Galera, Dgraph, Apache Cassandra

isolation: Serializability, Snapshot Isolation

data model: key-value store

violation: Write skew and Long fork in PostgreSQL, Aborted read in MongoDB, Lost update in MariaDB Galera, Causality violation in Dgraph, Aborted read in Apache Cassandra

description: MTC uses mini-transactions and their read-modify-write pattern to verify SER and SI efficiently in linear or quadratic time relative to the number of transactions. With the help of mini-transaction, both transaction execution and verification can be faster.

### Plume
link: https://dl.acm.org/doi/10.1145/3689742

databases: PostgreSQL, AntidoteDB, MariaDB-Galera, YugabyteDB, MySQL-Galera, Dgraph, MongoDB, CockroachDB

isolation: Transactional Causal Consistency, Read Atomic, Read Committed

data model: key-value store, list

violation: in AntidoteDB, MariaDB-Galera, YugabyteDB, MySQL-Galera, Dgraph, MongoDB, CockroachDB

description: Plume efficiently checks weak isolation levels, including TCC, RA and RC, by identifying (part of) 14 types of transactional anomalous patterns. It uses vectors and tree clocks to acclerate isolation checking.

### AWDIT
link: https://dl.acm.org/doi/10.1145/3742465

databases: PostgreSQL, CockroachDB, RocksDB

isolation: Transactional Causal Consistency, Read Atomic, Read Committed

data model: key-value store

violation: Future read and Causality cycle in CockroachDB and PostgreSQL

description: 

### IsoVista
link: https://dl.acm.org/doi/10.14778/3685800.3685866

databases: PostgreSQL, MySQL, MariaDB

isolation: Serializability, Snapshot Isolation, Transactional Causal Consistency, Read Atomic, Read Committed

data model: key-value store, list

violation: in PostgreSQL

description: IsoVista integrates existing tools, including PolySI, Viper, Elle and Plume, plus a new SER checker, to support various isolation levels. It also has a frontend with GUI and a web server.

### Vbox
link: https://arxiv.org/abs/2503.05163

databases: PostgreSQL, MySQL

isolation: Serializability

data model: key-value store, predicate

violation: in MySQL

description: 

### Leopard
link: https://ieeexplore.ieee.org/document/10184872

databases: PostgreSQL, OceanBase, TiDB, MySQL, OpenGauss

isolation: Serializability, Snapshot Isolation, Repeatable Read, Read Committed

data model: SQL

violation: several bugs in TiDB, MySQL, PostgreSQL, OpenGauss and OceanBase

description: Leopard decomposes several concurrency control protocols used by 18 popular databases into consistent read, mutual exclusion, first updater wins and serialization certifier to support checking various isolation levels. It mirrors the internal states of databases, including version orders and lock table, in addition to denpendency graph.

### Troubadour
link: https://dl.acm.org/doi/abs/10.1145/3720504

databases: PostgreSQL, MySQL, TiDB

isolation: Serializability, Snapshot Isolation, Read Committed

data model: SQL

violation: in MySQL and TiDB

description: Troubadour not only checks isolation levels (following the client-centric definition), but also verifies the semantic correctness of transactions. It conjoins them to yield SMT formulas and introduces novel encoding techniques to facilitate solving.

### GRAIL
link: https://link.springer.com/chapter/10.1007/978-3-031-64285-2_11

databases: ArangoDB, Neo4j

isolation: Serializability, Snapshot Isolation, PL-2, PL-1

data model: key-value store, list

violation: write skew in Neo4j

description: GRAIL uses graph databases and queries to detect isolation violations expressed as anti-patterns in dependency graphs. It builds graphs through graph database schemas and uses algorithms based on shortest path and strongly connected components to detect specifc cycles.

## White-box History Checking Tools
### Emme
link: https://dl.acm.org/doi/10.1145/3627703.3650080

databases: CockroachDB, TiDB, PostgreSQL

isolation: Serializability, Snapshot Isolation

data model: key-value store, predicate

violation: in PostgreSQL and CockroachDB

description: 

### Chronos & Aion
link: https://www.computer.org/csdl/proceedings-article/icde/2025/360300d738/26FZC0ImYsU

databases: Dgraph, TiDB, YugabyteDB

isolation: Serializability, Snapshot Isolation

data model: key-value store, list

violation: in Dgraph and YugabyteDB

description: Chronos uses timestamps to efficiently and incrementally check SER and SI, and Aion further extends it to support online checking, successfully overcoming chanllenges due to asynchronicity. They also introduce the garbage collection mechanism to avoid unlimited memory usage, which is especially important to online checking.
