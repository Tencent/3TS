# 3TS-DA Consistency Test


This manual helps to run 3TS-DA test properly. Users can obtain histories of consistent and inconsistent (anoamlies) executions by the cycle detection. Users can also use the consistent and inconsistent histories to test the false positive abort and correctness, respectively, for CC protocols.

##  Generate history

#### a. enter 3TS folder and generate configuration
 ```
    cp config.cfg.template config.cfg
 ```

Check it out an example configuration in config.cfg，which generates histories of 3-2-6 (#txn-#data_item-#operation) comfiguration.
 ```
    FilterRun = {
      thread_num = 10L; // number of threads
      generator = "TraversalGenerator"; // history generator
      outputters = ("CompareOutputter", "RollbackRateOutputter"); // result outputters
      algorithms = ( // concurrency control algorithms and filters
        { name = "DLI_IDENTIFY"; } 
      );
    };

    /* ========== history generators ========= */
    // Generate all histories meeting such conditions.
    TraversalGenerator = {
      trans_num = 3L; // number of transactions
            item_num = 2L; // number of variable items
            max_dml = 6L; // max number of DML operations
            subtask_num = 10L; // number of subtasks
            subtask_id = 0L; // the id of subtask to run
            with_abort = true; // generate history with Abort operation
      with_scan = "NONE_HAVE"; // generate history with ScanOdd operation ("NONE_HAVE", "ALL_HAVE", "NO_LIMIT")
      with_write = "NO_LIMIT"; // generate history with Write operation ("NONE_HAVE", "ALL_HAVE", "NO_LIMIT")
            tcl_position = "ANYWHERE"; // generate TCL operation in history position ("TAIL", "ANYWHERE", "NOWHERE")
      allow_empty_trans = false; // transactions generated can be without DML operations
      dynamic_history_len = false; // number of DML operation can be less than <max_dml>
      anomaly_rank = "RAT_WAT_IAT"; //IAT_RAT_WAT //WAT_RAT_IAT//RAT_WAT_IAT 
    };
``` 

#### b. complie the code
 ```
    ./make.sh
 ```
 
which will generate '3TS' compiled file.

#### c. run to generate histories
```
    ./3TS --conf_path=config.cfg
 ```




## Compute false postive abort

From above, we generate compare.txt history that including consistent and inconsistent histories by classfying into "NOT DLI_IDENTIFY OK" AND "DLI_IDENTIFY OK". We can obtain the consistent and inconsistent histories from it.

#### a. get consistent history into input.txt
 ```
    sed -n '1,/] DLI_IDENTIFY OK/!p' compare.txt > input.txt
 ```
 
#### b. Or we can also get the inconsistent history into input.txt
```
    sed -n '/NOT DLI_IDENTIFY OK/,/] DLI_IDENTIFY OK/ { /NOT DLI_IDENTIFY OK/! { /] DLI_IDENTIFY OK/! p } }' compare.txt > input.txt
```

#### c. Test the false potive by consistent histores or the correctness by inconsistent histories. Move input.txt to test folder （e.g., 3TS/contrib/deneva)
Firstly, configure the workload to DA and specify a CC protocol in config.h, e.g.,:
```
    #define WORKLOAD DA
    #define CC_ALG SSI
```

You can only use a single node, a single worker thread, and a single messaging thread.
Here are some of the configurations that need to be modified in the `config.h` file.
```
    #define NODE_CNT 1
    #define THREAD_CNT 1
    #define REM_THREAD_CNT 1
    #define SEND_THREAD_CNT 1

    #define CLIENT_NODE_CNT 1
    #define CLIENT_THREAD_CNT 1
    #define CLIENT_REM_THREAD_CNT 1
    #define CLIENT_SEND_THREAD_CNT 1
```

In addition, the client and server need to be placed only on one machine!
Only two lines of the same IP address can be written in the `ifconfig.txt` file, and this IP address is the machine you want to test.
Here is an example of this file:
```
    166.111.69.50
    166.111.69.50
```

After modifying all the above parameters, the next step is to determine the sequence of transaction operations to be performed. These sequences are ready in `input.txt` file from our previous step. The server run of protocols will decide whether to abort or commit of each sequence. Examples are as follows:
```
    W0a R1b W1a R1c C1 W0b C0
    R2a R3b W2b W3a C2 C3
```

A row represents an execution history.

Then run the deneva:
```
    ./rundb -nid0    # server side
    ./runcl -nid1    # client side
```

Finally, check the results, which are output in the `commit_histroy.txt` file.
Compare whether the actual execution results in the file meet the logic of your concurrency control algorithm. If so, it is proved that the algorithm is implemented correctly.
