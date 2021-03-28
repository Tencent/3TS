DDBMS
=======

DDBMS is a testbed of an OLTP distributed database management system (DBMS). It supports 6 concurrency control algorithms.

This testbed is based on the DBx1000 system, whose concurrency control scalability study can be found in the following paper:

    Staring into the Abyss: An Evaluation of Concurrency Control with One Thousand Cores
    Xiangyao Yu, George Bezerra, Andrew Pavlo, Srinivas Devadas, Michael Stonebraker
    http://voltdb.com/downloads/datasheets_collateral/vdb_whitepaper_staring_into_the_abyss.pdf

Build & Test
------------

To build the database.

    make deps
    make -j

Configuration
-------------

DBMS configurations can be changed in the config.h file. Please refer to README for the meaning of each configuration. Here we only list several most important ones.

    NODE_CNT          : Number of server nodes in the database
    THREAD_CNT        : Number of worker threads running per server
    WORKLOAD          : Supported workloads include YCSB and TPCC
    CC_ALG            : Concurrency control algorithm. Twelve algorithms are supported
                        (NO_WAIT, WAIT_DIE, TIMESTAMP, MVCC, OCC, CALVIN, SUNDIAL, SILO, BOCC, FOCC, SSI, WSI)
    MAX_TXN_IN_FLIGHT  : Maximum number of active transactions at each server at a given time
    DONE_TIMER        : Amount of time to run experiment

Configurations can also be specified as command argument at runtime. Run the following command for a full list of program argument.

    ./rundb -h

Run
---

The DBMS can be run with

    ./rundb -nid[N]
    ./runcl -nid[M]

where N and M are the ID of a server and client, respectively


DA 
---
The DBMS can use DA workload. This workload will executes a given sequence of transaction operations and prints out the actual execution results.

To use this workload, you can only use a single node, a single worker thread, and a single messaging thread.
Here are some of the configurations that need to be modified in the `config.h` file

    #define NODE_CNT 1
    #define THREAD_CNT 1
    #define REM_THREAD_CNT 1
    #define SEND_THREAD_CNT 1

    #define CLIENT_NODE_CNT 1
    #define CLIENT_THREAD_CNT 1
    #define CLIENT_REM_THREAD_CNT 1
    #define CLIENT_SEND_THREAD_CNT 1

    #define WORKLOAD DA

In addition, the client and server need to be placed only on one machine!
Only two lines of the same IP address can be written in the `ifconfig.txt` file, and this IP address is the machine you want to test.
Here is an example of this file:

    10.77.110.148
    10.77.110.148

After modifying all the above parameters, the next step is to determine the sequence of transaction operations to be performed. This sequence needs to be written in the `input.txt` file. Examples are as follows:

    W0a R1b W1a R1c C1 W0b C0
    R2a R3b W2b W3a C2 C3

A row represents a sequence.

Now to test, you need to perform the following command on the machine which you want to test in:

    ./rundb -nid0
    ./runcl -nid1

Finally, check the results, which are output in the `commit_histroy.txt` file.
Compare whether the actual execution results in the file meet the logic of your concurrency control algorithm. If so, it is proved that the algorithm is implemented correctly.