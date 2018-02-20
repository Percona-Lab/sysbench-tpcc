# sysbench-tpcc

TPCC-like workload for sysbench

prepare data and tables

```
./tpcc.lua --mysql-socket=/tmp/mysql.sock --mysql-user=root --mysql-db=sbt --time=300 --threads=64 --report-interval=1 --tables=10 --scale=100 prepare
```
