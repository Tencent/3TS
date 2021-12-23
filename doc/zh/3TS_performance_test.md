# 3TS 性能测试

本文档是测试说明文档，用户可以通过测试3TS里算法，得到一些性能数据如TPS。

## 配置可用IP
手动测试需要配置虚拟或者服务器IP，需要修改在'3TS/contribdeneva'文件下的'ifconfig.txt'文件：


#### a. 配置虚拟IP：
```
ifconfig bond1:0 166.111.69.50 netmask 255.255.255.192 up
ifconfig bond1:1 166.111.69.51 netmask 255.255.255.192 up
```
上面的命令会绑定166.111.69.50和166.111.69.51到bind1接口。

#### b. 在'ifconfig.txt'下配置IPs：
```
166.111.69.50
166.111.69.51
```
后面我们起server和client时会配备一些参数(e.g., -nid0, nid1)，就会指定到对应行的IP。

##  免密登录

需要把生成的公钥 (e.g., id_rsa.pub)放到需要免密登录的机器的'~/. ssh/authorized_keys'文件下。

##  配置参数
在'3TS/contrib/deneva/config.h'下配置参数，下面是一个常用的一个参数：
``` 
#define THREAD_CNT 4
#define WORKLOAD YCSB
#define CC_ALG  NO_WAIT
```
其中他们分别带别线程数，YCSB的工作负载，和NO_WAIT的并发控制算法。

##  编译代码
```
make clean
make deps
make -j 10
```
##  运行代码
首先需要把编译好的 'rundb' 和 'runcl' 文件放在相应的服务器上 (比如在 '3TS/output/' 目录下)。

#### a. 启动服务端程序 (在 '3TS/output/' 目录下)：
```
./rundb -nid0
```
#### b. 启动客户端程序 (在 '3TS/output/' 目录下)：
```
./runcl -nid1
```

