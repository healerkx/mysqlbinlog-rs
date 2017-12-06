# mysqlbinlog-rs
A MySQL binlog file (row format) parser in Rust

I built a parser in Python3 in the early of this year. Now in Rust instead, for high efficiency, and provided a Python3 binding for conveniency.

```
Usage:
  python3 main.py --ignore=th%.%,an%.% -b /usr/local/var/mysql/mysql_binlog.000001

# Give argument --ignore with a db name, table name pattern to ignore the row events in that tables.
```
