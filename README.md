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
./tpcc.lua --pgsql-host=/tmp --pgsql-port=1921 --pgsql-user=postgres --pgsql-db=postgres --threads=64 --tables=10 --scale=100 --trx_level=RC --db-ps-mode=auto --db-driver=pgsql prepare
```
  
or disable foreign key   
  
```
./tpcc.lua --pgsql-host=/tmp --pgsql-port=1921 --pgsql-user=postgres --pgsql-db=postgres --threads=64 --tables=10 --scale=100 --trx_level=RC --db-ps-mode=auto --db-driver=pgsql --use_fk=0 prepare
```
  
or use custom tablespace   
  
```
export pgsql_table_options="tablespace tbs1"
export pgsql_index_options="tablespace tbs2"

./tpcc.lua --pgsql-host=/tmp --pgsql-port=1921 --pgsql-user=postgres --pgsql-db=postgres --threads=64 --tables=10 --scale=100 --trx_level=RC --db-ps-mode=auto --db-driver=pgsql --use_fk=0 prepare
```
  
or use zheap custom storage_engine   
  
```
export pgsql_table_options="with (storage_engine='zheap') tablespace tbs1"
export pgsql_index_options="tablespace tbs2"

./tpcc.lua --pgsql-host=/tmp --pgsql-port=4001 --pgsql-user=postgres --pgsql-db=postgres --threads=64 --tables=20 --scale=100 --trx_level=RC --db-ps-mode=auto --db-driver=pgsql --use_fk=0 prepare
```
  
## PostgreSQL: Run benchmark
```
./tpcc.lua --pgsql-host=/tmp --pgsql-port=1921 --pgsql-user=postgres --pgsql-db=postgres --time=300 --threads=64 --report-interval=1 --tables=10 --scale=100 --trx_level=RC --db-ps-mode=auto --db-driver=pgsql --use_fk=0 --enable_purge=yes run
```

## PostgreSQL: Cleanup
```
./tpcc.lua --pgsql-host=/tmp --pgsql-port=1921 --pgsql-user=postgres --pgsql-db=postgres --time=300 --threads=64 --report-interval=1 --tables=10 --scale=100 --trx_level=RC --db-ps-mode=auto --db-driver=pgsql --use_fk=0 cleanup
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
  
# options
```
-- Command line options
sysbench.cmdline.options = {
   scale =
      {"Scale factor (warehouses)", 100},
   tables =
      {"Number of tables", 1},
   use_fk =
      {"Use foreign keys", 1},
   force_pk =
      {"Force using auto-inc PK on history table", 0},
   trx_level =
      {"Transaction isolation level (RC, RR or SER)", "RC"},
   enable_purge =
      {"Use purge transaction (yes, no)", "no"},
   report_csv =
      {"Report output in csv (yes, no)", "no"},
   mysql_storage_engine =
      {"Storage engine, if MySQL is used", "innodb"},
   mysql_table_options =
      {"Extra table options, if MySQL is used. e.g. 'COLLATE latin1_bin'", ""}
}
```
  
