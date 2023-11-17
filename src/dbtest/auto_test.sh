#!/bin/bash
set -x
db=$1
isolation=$2
if [ $db == "sqlserver" ]
then
    ./3ts_dbtest -isolation=$2 -db_type="sqlserver" -user="username" -passwd="password" -case_dir="sqlserver"
elif [ $db == "mysql" ]
then
    ./3ts_dbtest -isolation=$2 -db_type="mysql" -user="username" -passwd="password" -case_dir="mysql"
elif [ $db == "myrocks" ]
then
    ./3ts_dbtest -isolation=$2 -db_type="myrocks" -user="username" -passwd="password" -case_dir="myrocks"
elif [ $db == "tidb" ]
then
    ./3ts_dbtest -isolation=$2 -db_type="tidb" -user="username" -passwd="password" -case_dir="tidb"
elif [ $db == "oracle" ]
then
    ./3ts_dbtest -isolation=$2 -db_type="oracle" -user="username" -passwd="password" -case_dir="oracle"
elif [ $db == "pg" ]
then
    #./3ts_dbtest -isolation=$2 -db_type="pg" -user="username" -passwd="password" -case_dir="pg"
    ./3ts_dbtest -isolation=$2 -db_type="pg" -user="username" -passwd="password" -case_dir="pg"
elif [ $db == "ob" ]
then
    #./3ts_dbtest -isolation=$2 -db_type="ob" -user="username" -passwd="" -case_dir="ob"
    #./3ts_dbtest -isolation=$2 -db_type="ob" -user="username" -passwd="" -case_dir="ob"
    #./3ts_dbtest -isolation=$2 -db_type="ob" -user="username" -passwd="" -case_dir="ob"
    ./3ts_dbtest -isolation=$2 -db_type="ob" -user="username" -passwd="password" -case_dir="ob"
elif [ $db == "crdb" ]
then
    ./3ts_dbtest -isolation=$2 -db_type="crdb" -user="username" -passwd="password" -case_dir="crdb"
elif [ $db == "mongodb" ]
then
    ./3ts_dbtest -isolation=$2 -db_type="mongodb" -user="username" -passwd="password" -case_dir="mongodb"
elif [ $db == "mariadb" ]
then
    ./3ts_dbtest -isolation=$2 -db_type="mariadb" -user="username" -passwd="password" -case_dir="mariadb"
elif [ $db == "cassandra" ]
then
    ./3ts_dbtest -isolation=$2 -db_type="cassandra" -user="username" -passwd="password" -case_dir="cassandra"
elif [ $db == "yugabyte" ]
then
    ./3ts_dbtest -isolation=$2 -db_type="yugabyte" -user="username" -passwd="password" -case_dir="yugabyte"
fi
