## Building 3TS-Coo on Ubuntu 20.04

# I. Environment Preparation

To generate Makefiles, compile code, and link databases, install the following:

1. [GCC](https://gcc.gnu.org/): GCC (GNU Compiler Collection) is an open-source compiler suite for multiple programming languages.
2. [CMake](https://cmake.org/): A cross-platform build tool that automates project configuration.
3. Target database environment (e.g., [MySQL](https://downloads.mysql.com/archives/community)).
4. [ODBC](http://www.unixodbc.org/): A standard API for database connectivity.
5. ODBC driver for the target database (e.g., [mysql-connector-odbc](https://dev.mysql.com/downloads/connector/odbc/)).

## 1.1 Install Build Tools
```bash
sudo apt update
sudo apt install -y gcc g++ cmake curl
```

## 1.2 Install DB & Driver
- **Quick method (default versions):**
  ```bash
  sudo apt install -y mysql-server libmysqlclient-dev mysql-client
  ```

- **Manual install (specific versions):**
  1. Download DEB bundle from [MySQL site](https://downloads.mysql.com/archives/community/)
  2. Install in order:
     ```bash
     dpkg -i mysql-common_8.0.19-1ubuntu18.04_amd64.deb
     dpkg -i libmysqlclient21_8.0.19-1ubuntu18.04_amd64.deb
     dpkg -i libmysqlclient-dev_8.0.19-1ubuntu18.04_amd64.deb
     ```
  3. Configure DB:
     ```bash
     sudo mysql
     CREATE database test;
     ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY '12345678';
     FLUSH PRIVILEGES;
     exit;
     ```
  4. Download  and Build [MySQL ODBC driver source](https://downloads.mysql.com/archives/c-odbc/ ) :
     ```bash
     tar -zxvf mysql-connector-odbc-8.0.42-src.tar.gz
     cd mysql-connector-odbc-8.0.42-src
     mkdir build && cd build
     cmake -G "Unix Makefiles" -DWITH_UNIXODBC=true -DDISABLE_GUI=true ..
     make -j4 && make install
     ```

## 1.3 Install & Configure ODBC
- **Quick method:**
  ```bash
  sudo apt-get install unixodbc-dev
  ```

- **Manual build:**
  ```bash
  curl -o unixODBC-2.3.12.tar.gz https://www.unixodbc.org/unixODBC-2.3.12.tar.gz
  tar -zxvf unixODBC-2.3.12.tar.gz
  sudo apt install -y autoconf automake libtool
  cd unixODBC-2.3.12/
  ./configure
  make -j4 && make install
  odbc_config --version  # Verify
  ```

- **Configure ODBC files:**
  - `odbcinst.ini`:
    ```ini
    # cat /usr/local/etc/odbcinst.ini
    [MySQL]
    Driver = /home/infinity/Desktop/mysql-connector-odbc-8.0.42-src/build/lib/libmyodbc8w.so
    Description = Unicode Driver for connecting to MySQL database server
    Threading = 0
    ```
  - `odbc.ini`:
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
- **Test connection:**
  ```bash
  isql mysql
  ```

# 2. Build & Test 3TS-Coo

## 2.1 Get Source Code
```bash
git clone https://github.com/Tencent/3TS.git
cd 3TS
git checkout coo-consistency-check
```

## 2.2 Install Dependencies [gflags](https://github.com/gflags/gflags) 
```bash
# In gflags source directory
mkdir build && cd build
cmake ..
make && make install
```

- **Optional: Copy FindODBC.cmake (if build fails)**
  ```bash
  find / -name "FindODBC.cmake"
  cp <found_path>/FindODBC.cmake 3TS/src/dbtest/
  ```

## 2.3 Compile & Run Tests
```bash
cd src/dbtest
mkdir build && cd build
cmake -S ../ -DWITH_UNIXODBC=1  # Use unixODBC
make -j4
```

- **Run test script:**
  1. Edit `auto_test.sh` with correct DB credentials
  2. Execute:
     ```bash
     ./auto_test.sh "mysql" "read-uncommitted"
     ```
- **Successful output:**
  ```bash
  Test Result: Anomaly
  Reason: Data anomaly is not recognized by the database, resulting in data inconsistencies
  ```