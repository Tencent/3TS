#!/bin/bash
set -x
db=$1
isolation=$2
if [ $db == "sqlserver" ]
then
    ./3ts_dbtest -isolation=$2 -db_type="sqlserver" -user="SA" -passwd="Ly.123456" -case_dir="sqlserver"
elif [ $db == "mysql" ]
then
    ./3ts_dbtest -isolation=$2 -db_type="mysql" -user="test123" -passwd="Ly.123456" -case_dir="mysql"
elif [ $db == "tidb" ]
then
    ./3ts_dbtest -isolation=$2 -db_type="tidb" -user="test123" -passwd="Ly.123456" -case_dir="tidb"
elif [ $db == "oracle" ]
then
    ./3ts_dbtest -isolation=$2 -db_type="oracle" -user="system" -passwd="oracle" -case_dir="oracle"
elif [ $db == "pg" ]
then
    ./3ts_dbtest -isolation=$2 -db_type="pg" -user="test123" -passwd="Ly.123456" -case_dir="pg"
elif [ $db == "ob" ]
then
    ./3ts_dbtest -isolation=$2 -db_type="ob" -user="sys@oracle" -passwd="" -case_dir="ob"
elif [ $db == "crdb" ]
then
    ./3ts_dbtest -isolation=$2 -db_type="crdb" -user="test123" -passwd="Ly.123456" -case_dir="crdb"
else [ $db == "mongodb" ]
    ./3ts_dbtest -isolation=$2 -db_type="mongodb" -user="test123" -passwd="Ly.123456" -case_dir="mysql"
fi


