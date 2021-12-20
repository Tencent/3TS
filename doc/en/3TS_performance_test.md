# 3TS performance example test 

This manual helps to run 3TS performance test properly. Users are expected to obtain the TPS of some CC protocols.

## Configure IP addresses
Single run need to configure virtual/real machines by 'ifconfig.txt' under folder '3TS/contribdeneva', e.g.:

#### a. setup virtual IPs:
```
ifconfig bond1:0 166.111.69.50 netmask 255.255.255.192 up
ifconfig bond1:1 166.111.69.51 netmask 255.255.255.192 up
```
These bind 166.111.69.50 and 166.111.69.51 to interface bond1.
#### b. edit virtual machines or real IPs in ifconfig.txt
```
166.111.69.50
166.111.69.51
```
Later, the arguement (e.g., -nid0, nid1) specified to server or client will read the corresponding IP in the specific row.

##  Passwordless login 

Put the public key (e.g., id_rsa.pub) to the remote hosts ~/. ssh/authorized_keys.

##  Configure paraemters
Configure paramters in file '3TS/contrib/deneva/config.h', The following are typical configurable parameters.
``` 
#define THREAD_CNT 4
#define WORKLOAD YCSB
#define CC_ALG  NO_WAIT
```

where these parameter represent for the number of threads, data set, and CC protocol.

##  Complie the code
```
make clean
make deps
make -j 10
```
##  Start to run
Copy complied 'rundb' and 'runcl' files to corresponding machines. (e.g., 3TS/output/)
#### a. Start a server (under 3TS/output/):
```
./rundb -nid0
```
#### b. Start a client (under 3TS/output/):
```
./runcl -nid1
```
