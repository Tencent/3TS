## Ubuntu 18.04 编译 3TS-Coo

### 所需依赖

首先，根据 3TS 官方文档，为了生成Makefile、编译代码并链接数据库，需要安装以下内容：

- GCC 编译套件， CMake
- ODBC 启动管理器
- 要测试的数据库（例如：MySQL）
- 与数据库对应的 ODBC 驱动（例如：mysql-connector-odbc）
- 3TS-Coo 以及 ODBC 运行的依赖库

 3TS 使用了 C++17 的特性，所以安装 GCC 套件的时候请确保它支持 C++17 (推荐 g++8)。具体命令如下。

```shell
sudo apt update
sudo apt install build-essential cmake git
apt install 
```

同样的，我们也要安装 ODBC 依赖，这个在下文会一一给出。



### 安装 ODBC 驱动管理器

ODBC 是 Open Database Connect 即开放数据库互连的简称，它是由微软提出的一个用于访问数据库的统一界面标准，是应用程序和数据库系统之间的中间件。

ODBC 主要由驱动程序 (ODBC Driver) 和驱动程序管理器 (ODBC Driver Manager) 组成。驱动程序是一个用以支持 ODBC 函数调用的模块，每个驱动程序对应于相应的数据库，源码由具体的数据库厂商实现。驱动程序管理器可链接到所有 ODBC 应用程序中，它负责管理应用程序中 ODBC 函数与 DLL 中函数的绑定。

#### 下载与解压缩

目前主要使用的驱动管理器有两个，分别是 [unixODBC](https://www.unixodbc.org/) 和 [iODBC](https://www.iodbc.org/dataspace/doc/iodbc/wiki/iodbcWiki/WelcomeVisitors)。 我们选择下载安装 `unixODBC`。首先，从官方下载 `unixODBC` 源码。具体版本请自行选择，这里选择下载 [2.3.12 ](https://www.unixodbc.org/unixODBC-2.3.12.tar.gz) 版本。可以直接从官网的 Downloads 界面下载，也可以借助 `curl` 或者 `wget` 下载。下载后使用 `tar` 工具进行解压缩。

```shell
curl -o unixODBC-2.3.12.tar.gz https://www.unixodbc.org/unixODBC-2.3.12.tar.gz
tar -zxvf unixODBC-2.3.12.tar.gz
```

#### 依赖下载

根据 `unixODBC` 官方网站， `unixODBC` 源码使用 `autoconf` 构建。编译安装之前，请确保本地环境已经有 `automake`，`autoconf`， `libtool` 工具。

```shell
apt install autoconf automake libtool
```

#### 编译安装

根据官方步骤，进行编译安装，编译安装命令如下。具体安装步骤和编译选项见 [unixODBC 官网](https://www.unixodbc.org/)

```shell
cd unixODBC-dir
./configure # 默认情况下，unixODBC将会安装到/usr/local中。与configure一样，可以通过更改configure的 --prefix 更改安装位置，这里使用默认位置安装。 
make -j4 && make install
```

#### 安装验证

安装后，使用 `odbc_config` 或 `odbcinst` 验证安装是否成功。

```shell
root@c7b71bf10157$ odbc_config --version
2.3.12
root@c7b71bf10157$ odbcinst -j
unixODBC 2.3.12
DRIVERS............: /usr/local/etc/odbcinst.ini
SYSTEM DATA SOURCES: /usr/local/etc/odbc.ini
FILE DATA SOURCES..: /usr/local/etc/ODBCDataSources
USER DATA SOURCES..: /root/.odbc.ini
SQLULEN Size.......: 8
SQLLEN Size........: 8
SQLSETPOSIROW Size.: 8
```



### 安装 ODBC 驱动

#### 安装 MySQL ODBC 驱动

以 `mysql-connector-odbc-8.0.19-linux-ubuntu18.04-x86-64` 为例

##### 前提条件

使用的是 `ubuntu 18.04`，MySQL ODBC 没有提供对应的 DEB 包。所以只能源码编译 ODBC。 首先根据 MySQL 官方网站，为了安装 MySQL ODBC Driver 我们需要先确保以下条件满足。

* **GNU 编译套件和 Cmake**: 要求 GCC 4.2.1 或更高版本

* **MySQL Client Library**。要获取 MySQL client 库和包含文件，请访问 [MySQL 下载页面](https://dev.mysql.com/downloads/)。

* **一个兼容的 ODBC 驱动管理器**。Connector/ODBC 已知可以与 iODBC 和 unixODBC 管理器一起工作。

* **字符集支持**。如果您使用的字符集未编译到 MySQL 客户端库中，请将 MySQL 字符定义从 `charsets` 目录安装到 `SHAREDIR`（默认情况下为 `/usr/local/mysql/share/mysql/charsets`）。如果您在同一台机器上安装了 MySQL 服务器，这些字符集应该已经就位。有关字符集支持的更多信息，请参见“字符集、排序规则、Unicode”。

##### MySQL Client Library 安装

MySQL 针对 Client Library 已经提供了 DEB 包，可以直接在 [对应网站](https://downloads.mysql.com/archives/community/)下载安装。这里以 MySQL 8.0.19 对应的 client library 为例。

首先下载对应的 DEB 包。主要涉及的 DEB 包下文列出

```shell
mysql-common_8.0.19-1ubuntu18.04_amd64.deb
libmysqlclient21_8.0.19-1ubuntu18.04_amd64.deb 
libmysqlclient-dev_8.0.19-1ubuntu18.04_amd64.deb
```

官网已经提供了对应的 **Bundle** 包，包含 MySQL 官方打包的所有 deb。下载后使用 `dpkg` 命令安装对应的 DEB 包。

```shell
dpkg -i mysql-common_8.0.19-1ubuntu18.04_amd64.deb
dpkg -i libmysqlclient21_8.0.19-1ubuntu18.04_amd64.deb 
dpkg -i libmysqlclient-dev_8.0.19-1ubuntu18.04_amd64.deb
```

注意，这三个包是层层依赖的，所以安装顺序不能调换。

##### MySQL ODBC Connector (Driver) 编译安装

首先，从 [MySQL ODBC 下载网站](https://downloads.mysql.com/archives/c-odbc/) 拉取对应版本的 MySQL ODBC Connector 源码到本地，使用 `tar` 进行解压。

```shell
tar -zxvf mysql-connector-odbc-8.0.19-src.tar.gz
```

MySQL ODBC Connector 源码使用 `cmake` 构建，所以安装之前，请确保本地已经有 `cmake`。进入 Connector 文件夹里，并创建 `build` 文件夹。

```shell
cd mysql-connector-odbc-8.0.19-src
mkdir build
```

使用 `cmake` 配置构建选项

```shell
cmake -G "Unix Makefiles" -DWITH_UNIXODBC=true -DDISABLE_GUI=true ..
```

* `-DWITH_UNIXODBC`: 使用 `unixODBC` 作为驱动管理器，MySQL ODBC 默认使用 `iODBC`。
* `-DDISABLE_GUI`: 不构建 GUI 图形界面，默认为 `false`, 构建 GUI。

使用 `make` 进行项目构建，并安装。默认安装到 `/usr/local` 路径。可以使用 `cmake` 编译选项 `-DCMAKE_INSTALL_PREFIX` 进行配置。

```shell
make -j4 && make install
```

编译过程中可以出现无法链接到 `ssl` 库的问题。可以使用 `apt` 安装后重新构建。

```shell
apt install libssl-dev
make -j4
```



### 3TS-Coo 编译

#### 下载

首先从  [3TS github 官网](https://github.com/Tencent/3TS.git) 上拉去 3TS 源码， 进入代码根目录，切换分支到 `coo-consistency-check`。

```shell
git clone https://github.com/Tencent/3TS.git
git checkout coo-consistency-check
```

#### 依赖

注意， 3Ts-Coo 项目依赖 pthread 库 和 gflags 库，编译前请确保已经安装相关依赖。 

#### 编译

进入 `src/dbtest` 文件夹，创建 `build` 目录并进入。

~~~shell
cd src/dbtest
mkdir build && cd build
~~~

这里不要直接 `cmake` 构建，否则会报下面错误

>  By not providing "FindODBC.cmake" in CMAKE_MODULE_PATH this project has
>   asked CMake to find a package configuration file provided by "ODBC", but
>   CMake did not find one.
>
>   Could not find a package configuration file provided by "ODBC" with any of
>   the following names:
>
>     ODBCConfig.cmake
>     odbc-config.cmake
>
> separate development package or SDK, be sure it has been
>   installed.

要解决该错误，需要执行

~~~shell
find / -name "FindODBC.cmake"

//filepath is obtained by "find"
cp filepath/FindODBC.cmake 3TS/src/dbtest
~~~

之后在 `build` 目录下进行构建与编译

~~~shell
cmake -S -DWITH_UNIXODBC=1 ../ #这里拷贝的是 mysql FindODBC.cmake 默认为 iODBC， 需要配置参数
make -j4
~~~

