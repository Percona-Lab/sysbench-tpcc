# sysbench-tpcc

TPCC-like workload for sysbench 1.0.x.
**Make sure you are using sysbench 1.0.14 or better!**

# modified by digoal
1\. Use prepared statement for postgresql.

2\. Use Read committed by default
  
3\. modified file: ```tpcc_common.lua, tpcc.lua, tpcc_run.lua```

# PostgreSQL example (use modified prepared statement by digoal)
for exp.

```
unixsocket_dir='/tmp'
port=1921
user=postgres
dbname=postgres
```

## PostgreSQL: prepare data and tables
```
./tpcc.lua --pgsql-host=/tmp --pgsql-port=1921 --pgsql-user=postgres --pgsql-db=postgres --time=300 --threads=64 --report-interval=1 --tables=10 --scale=100 --trx_level=RC --db-ps-mode=auto --db-driver=pgsql prepare
```

## PostgreSQL: Run benchmark
```
./tpcc.lua --pgsql-host=/tmp --pgsql-port=1921 --pgsql-user=postgres --pgsql-db=postgres --time=300 --threads=64 --report-interval=1 --tables=10 --scale=100 --trx_level=RC --db-ps-mode=auto --db-driver=pgsql run
```

## PostgreSQL: Cleanup
```
./tpcc.lua --pgsql-host=/tmp --pgsql-port=1921 --pgsql-user=postgres --pgsql-db=postgres --time=300 --threads=64 --report-interval=1 --tables=10 --scale=100 --trx_level=RC --db-ps-mode=auto --db-driver=pgsql cleanup
```

# for MySQL
# prepare data and tables

`
./tpcc.lua --mysql-socket=/tmp/mysql.sock --mysql-user=root --mysql-db=sbt --time=300 --threads=64 --report-interval=1 --tables=10 --scale=100 --db-driver=mysql prepare
`

## prepare for RocksDB

`
./tpcc.lua --mysql-socket=/tmp/mysql.sock --mysql-user=root --mysql-db=sbr --time=3000 --threads=64 --report-interval=1 --tables=10 --scale=100 --use_fk=0 --mysql_storage_engine=rocksdb --mysql_table_options='COLLATE latin1_bin' --trx_level=RC --db-driver=mysql prepare
`

# Run benchmark

`
./tpcc.lua --mysql-socket=/tmp/mysql.sock --mysql-user=root --mysql-db=sbt --time=300 --threads=64 --report-interval=1 --tables=10 --scale=100 --db-driver=mysql run
`

# Cleanup 

`
./tpcc.lua --mysql-socket=/tmp/mysql.sock --mysql-user=root --mysql-db=sbt --time=300 --threads=64 --report-interval=1 --tables=10 --scale=100 --db-driver=mysql cleanup
`
