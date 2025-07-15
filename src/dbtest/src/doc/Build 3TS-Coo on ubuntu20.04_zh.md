## Ubuntu 20.04 编译 3TS-Coo

# 一、准备环境

为了编译 3TS-Coo并使用它，需要安装这些：

1. [GCC](https://gcc.gnu.org/) ：GCC（GNU Compiler Collection）是一个开源的编译器套件，用于编译多种编程语言
2. [CMake](https://cmake.org/)：CMake是一个跨平台的构建工具，用于自动生成项目构建文件，简化编译过程
3. 待测试的数据库环境（例如： [MySQL](https://downloads.mysql.com/archives/community) )
4. [ODBC](http://www.unixodbc.org/)：ODBC是一种标准的API，让应用程序能连接多种数据库
5. 待测试数据库对应的 ODBC 驱动（例如：[mysql-connector-odbc](https://dev.mysql.com/downloads/connector/odbc/)）

## 1.1 安装构建工具

```bash
sudo apt update
sudo apt install -y gcc g++ cmake curl
```

## 1.2 安装数据库和驱动

*   **简单方法 (用 apt 默认版本):**
    ```bash
    sudo apt install -y mysql-server libmysqlclient-dev mysql-client
    ```

*   **手动安装特定版本 (如果需要):**
    1.  从 [MySQL 官网](https://downloads.mysql.com/archives/community/) 下载对应版本的 DEB Bundle 包。
    2.  按顺序安装：
        ```bash
        dpkg -i mysql-common_8.0.19-1ubuntu18.04_amd64.deb
        dpkg -i libmysqlclient21_8.0.19-1ubuntu18.04_amd64.deb
        dpkg -i libmysqlclient-dev_8.0.19-1ubuntu18.04_amd64.deb
        ```
    3.  设置数据库：
        ```bash
        sudo mysql
        CREATE database test;
        ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY '12345678';
        FLUSH PRIVILEGES;
        exit;
        ```
    4.  下载并编译 [MySQL ODBC Connector 驱动源码](https://downloads.mysql.com/archives/c-odbc/ ) ：
        ```bash
        tar -zxvf mysql-connector-odbc-8.0.42-src.tar.gz
        cd mysql-connector-odbc-8.0.42-src
        mkdir build && cd build
        cmake -G "Unix Makefiles" -DWITH_UNIXODBC=true -DDISABLE_GUI=true ..
        make -j4 && make install
        ```

## 1.3 安装和配置 ODBC

*   **简单方法 (用 apt 默认版本):**
    
    ```bash
    sudo apt install unixodbc unixodbc-dev -y
    ```
    
*   **手动编译 (如果需要):**
    ```bash
    curl -o unixODBC-2.3.12.tar.gz https://www.unixodbc.org/unixODBC-2.3.12.tar.gz
    tar -zxvf unixODBC-2.3.12.tar.gz
    sudo apt install -y autoconf automake libtool
    cd unixODBC-2.3.12/
    ./configure
    make -j4 && make install
    # 验证
    odbc_config --version
    odbcinst -j
    ```

*   **配置 ODBC 文件 (关键步骤!)**
    *   **`odbcinst.ini` 示例:**
        ```ini
        # cat /usr/local/etc/odbcinst.ini
        [MySQL]
        Driver = /home/infinity/Desktop/mysql-connector-odbc-8.0.42-src/build/lib/libmyodbc8w.so
        Description = Unicode Driver for connecting to MySQL database server
        Threading = 0
        ```
    *   **`odbc.ini` 示例:**
        ```ini
        # cat /usr/local/etc/odbc.ini
        [mysql]
        Server          = localhost
        Host            = localhost
        Port            = 3306
        User            = root
        Password        = 12345678
        Driver          = /home/infinity/Desktop/mysql-connector-odbc-8.0.42-src/build/lib/libmyodbc8a.so
        DATABASE        = test
        DESCRIPTION     = MySQL ODBC 8.0 ANSI Driver test
        UID             = root
        ```
*   **测试 ODBC 连接:**
    ```bash
    isql mysql
    ```

# 二、编译和测试 3TS-Coo

## 2.1 获取源码

```bash
git clone https://github.com/Tencent/3TS.git
cd 3TS
git checkout coo-consistency-check
```

## 2.2 安装依赖 (gflags)

```bash
# 假设你在 gflags 源码目录
mkdir build && cd build
cmake ..
make && make install
```

*   **可选：拷贝 `FindODBC.cmake` (如果编译报错)**
    ```bash
    find / -name "FindODBC.cmake"  # 找到文件路径
    cp <找到的路径>/FindODBC.cmake 3TS/src/dbtest/
    ```

## 2.3 编译和运行测试

```bash
cd src/dbtest
mkdir build && cd build
cmake -S ../ -DWITH_UNIXODBC=1  # 指定用 unixODBC
make -j4
```

*   **运行测试脚本:**
    1.  编辑 `auto_test.sh`，设置正确的数据库用户名和密码。
    2.  执行：
        ```bash
        ./auto_test.sh "mysql" "read-uncommitted"
        ```

*   **成功输出示例:**
    ```bash
    Test Result: Anomaly
    Reason: Data anomaly is not recognized by the database, resulting in data inconsistencies
    ```

