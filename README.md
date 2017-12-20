# mysqlbinlog-rs
A MySQL binlog file (row format) parser in Rust

I built a parser in Python3 in the early of this year. Now in Rust instead, for high efficiency, and provided a Python3 binding for conveniency.

# Scenarios
- Sync MySQL data into Redis, MongoDB, Kafka, e.g. 
- Figure out the DB data row change history when fixing bugs
- Watch table's data changing for developer, for example, coding a PHP controller-action.
- ...

Examples for Rust developers
- You can see files in the dir `examples`


Examples for Python developers
```
Usage:
  python3 main.py --ignore=th%.%,an%.% -b /usr/local/var/mysql/mysql_binlog.000001

# Give argument --ignore with a db name, table name pattern to ignore the row events in that tables.
```



- 2017-12-13 Reduce times of reading binlog file. Release the content memory in Vec[] for unused content. Fix python binding's dylib loading path.
