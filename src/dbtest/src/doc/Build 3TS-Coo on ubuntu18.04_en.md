## Compiling 3TS-Coo on Ubuntu 18.04

### Required Dependencies

First, according to the 3TS official documentation, to generate the Makefile, compile the code, and link to the database, the following tools and libraries need to be installed:

- GCC suite and CMake
- ODBC Driver Manager
- The database you want to test (e.g., MySQL)
- ODBC driver for the database (e.g., mysql-connector-odbc)
- Dependencies for 3TS-Coo and ODBC

3TS uses C++17 features, so when installing the GCC suite, make sure it supports C++17 (GCC 8 is recommended). The specific commands are as follows:

```shell
sudo apt update
sudo apt install build-essential cmake git
apt install 
```

We also need to install ODBC dependencies, which will be detailed below.

### Installing the ODBC Driver Manager

ODBC (Open Database Connectivity) is a standard API for accessing database management systems (DBMS). It was developed by Microsoft to make it easier for applications to communicate with databases. It acts as a middleware between applications and the DBMS.

ODBC mainly consists of the ODBC Driver and the ODBC Driver Manager. The driver is a module that supports ODBC function calls, and each driver corresponds to a specific database, provided by the database vendor. The driver manager links all ODBC applications to the drivers and handles the binding of ODBC function calls to the appropriate driver functions.

#### Download and Extract

The two main driver managers are [unixODBC](https://www.unixodbc.org/) and [iODBC](https://www.iodbc.org/dataspace/doc/iodbc/wiki/iodbcWiki/WelcomeVisitors). We will install `unixODBC`. Download the `unixODBC` source from the official site. Choose the version as needed; here we download version [2.3.12](https://www.unixodbc.org/unixODBC-2.3.12.tar.gz). You can download it directly from the Downloads page on the website or use `curl` or `wget`. After downloading, use `tar` to extract the files.

```shell
curl -o unixODBC-2.3.12.tar.gz https://www.unixodbc.org/unixODBC-2.3.12.tar.gz
tar -zxvf unixODBC-2.3.12.tar.gz
```

#### Installing Dependencies

According to the `unixODBC` website, the source uses `autoconf` to build. Ensure that `automake`, `autoconf`, and `libtool` are installed before compiling and installing.

```shell
apt install autoconf automake libtool
```

#### Compiling and Installing

Follow the official steps to compile and install. The commands are as follows. For detailed installation steps and build options, refer to the [unixODBC official site](https://www.unixodbc.org/).

```shell
cd unixODBC-dir
./configure # By default, unixODBC will be installed in /usr/local. You can change the installation location with the --prefix option. Here we use the default location.
make -j4 && make install
```

#### Installation Verification

After installation, use `odbc_config` or `odbcinst` to verify the installation.

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

### Installing the ODBC Driver

#### Installing MySQL ODBC Driver

Using `mysql-connector-odbc-8.0.19-linux-ubuntu18.04-x86-64` as an example.

##### Prerequisites

Using `Ubuntu 18.04`, MySQL ODBC does not provide a corresponding DEB package. Therefore, the ODBC driver must be compiled from source. According to the MySQL official site, to install the MySQL ODBC Driver, ensure the following prerequisites are met:

* **GNU Compiler Collection and CMake**: Requires GCC 4.2.1 or later.
* **MySQL Client Library**. To obtain the MySQL client library and header files, visit the [MySQL downloads page](https://dev.mysql.com/downloads/).
* **A compatible ODBC driver manager**. Connector/ODBC is known to work with both iODBC and unixODBC managers.
* **Character set support**. If you are using a character set not compiled into the MySQL client library, install the MySQL character definitions from the `charsets` directory to the `SHAREDIR` (default is `/usr/local/mysql/share/mysql/charsets`). If you have installed a MySQL server on the same machine, these character sets should already be in place. For more information on character set support, see "Character Sets, Collations, Unicode."

##### MySQL Client Library Installation

MySQL provides DEB packages for the Client Library, which can be downloaded and installed from the [MySQL downloads page](https://downloads.mysql.com/archives/community/). Here we use the MySQL 8.0.19 client library as an example.

First, download the corresponding DEB packages. The main DEB packages are listed below:

```shell
mysql-common_8.0.19-1ubuntu18.04_amd64.deb
libmysqlclient21_8.0.19-1ubuntu18.04_amd64.deb 
libmysqlclient-dev_8.0.19-1ubuntu18.04_amd64.deb
```

The official site provides a **Bundle** package that includes all the deb files. Download it and use the `dpkg` command to install the corresponding DEB packages.

```shell
dpkg -i mysql-common_8.0.19-1ubuntu18.04_amd64.deb
dpkg -i libmysqlclient21_8.0.19-1ubuntu18.04_amd64.deb 
dpkg -i libmysqlclient-dev_8.0.19-1ubuntu18.04_amd64.deb
```

Note that these packages depend on each other, so the installation order cannot be changed.

##### MySQL ODBC Connector (Driver) Compilation and Installation

First, download the MySQL ODBC Connector source code from the [MySQL ODBC downloads page](https://downloads.mysql.com/archives/c-odbc/), then use `tar` to extract the files.

```shell
tar -zxvf mysql-connector-odbc-8.0.19-src.tar.gz
```

The MySQL ODBC Connector source uses `cmake` to build, so ensure that `cmake` is installed before proceeding. Enter the Connector directory and create a `build` directory.

```shell
cd mysql-connector-odbc-8.0.19-src
mkdir build
```

Configure the build options using `cmake`.

```shell
cmake -G "Unix Makefiles" -DWITH_UNIXODBC=true -DDISABLE_GUI=true ..
```

* `-DWITH_UNIXODBC`: Use `unixODBC` as the driver manager. By default, MySQL ODBC uses `iODBC`.
* `-DDISABLE_GUI`: Do not build the GUI. The default is `false`, which builds the GUI.

Build the project using `make`, and install it. The default installation path is `/usr/local`, which can be changed using the `-DCMAKE_INSTALL_PREFIX` option.

```shell
make -j4 && make install
```

During compilation, if there are issues about missing dynamic library, install it using `apt` and rebuild.

```shell
apt install libssl-dev
make -j4
```

### Compiling 3TS-Coo

#### Download

First, clone the 3TS source code from the [3TS GitHub repository](https://github.com/Tencent/3TS.git), enter the source code root directory, and switch to the `coo-consistency-check` branch.

```shell
git clone https://github.com/Tencent/3TS.git
git checkout coo-consistency-check
```

#### Dependencies

Note that the 3TS-Coo project depends on the `pthread` library and the `gflags` library. Ensure these dependencies are installed before compiling.

~~~ shell

    

 ~~~

#### Compilation

Navigate to the `src/dbtest` directory, create a `build` directory, and enter it.

```shell
cd src/dbtest
mkdir build && cd build
```

Do not directly run `cmake` here; otherwise, you will encounter the following error:

> By not providing "FindODBC.cmake" in CMAKE_MODULE_PATH this project has
> asked CMake to find a package configuration file provided by "ODBC", but
> CMake did not find one.
>
> Could not find a package configuration file provided by "ODBC" with any of
> the following names:
>
> ODBCConfig.cmake
> odbc-config.cmake
>
> Make sure the installation of the ODBC driver manager is correct.

To resolve this error, run:

```shell
find / -name "FindODBC.cmake"

//filepath is obtained by "find

"
cp filepath/FindODBC.cmake 3TS/src/dbtest
```

Then, configure and build in the `build` directory.

```shell
cmake -S -DWITH_UNIXODBC=1 ../ # Copying the MySQL FindODBC.cmake, the default is iODBC, so we need to set the configuration parameter.
make -j4
```
