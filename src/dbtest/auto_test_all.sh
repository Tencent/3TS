#!/bin/bash
set -x
./auto_test.sh "mysql" "read-uncommitted"
./auto_test.sh "mysql" "read-committed"
./auto_test.sh "mysql" "repeatable-read"
./auto_test.sh "mysql" "serializable"

./auto_test.sh "sqlserver" "read-uncommitted"
./auto_test.sh "sqlserver" "read-committed"
./auto_test.sh "sqlserver" "repeatable-read"
./auto_test.sh "sqlserver" "serializable"

./auto_test.sh "oracle" "read-committed"
./auto_test.sh "oracle" "serializable"

./auto_test.sh "pg" "read-uncommitted"
./auto_test.sh "pg" "read-committed"
./auto_test.sh "pg" "repeatable-read"
./auto_test.sh "pg" "serializable"

./auto_test.sh "tidb" "read-committed"
./auto_test.sh "tidb" "repeatable-read"

./auto_test.sh "crdb" "serializable"

./auto_test.sh "ob" "read-uncommitted"
./auto_test.sh "ob" "read-committed"
./auto_test.sh "ob" "repeatable-read"
./auto_test.sh "ob" "serializable"

./auto_test.sh "mariadb" "read-uncommitted"
./auto_test.sh "mariadb" "read-committed"
./auto_test.sh "mariadb" "repeatable-read"
./auto_test.sh "mariadb" "serializable"

./auto_test.sh "cassandra" "one"

./auto_test.sh "yugabyte" "serializable"
./auto_test.sh "yugabyte" "snapshot"