
from mysqlbinlog import *

import platform

def main():
    reader = BinLogReader('/Users/healer/mysql_binlog.000001')
    
    while True:
        h = reader.read_event_header()
        if not h:
            print("Empty")
            break
        reader.read_event(h)
    
    reader.close()


if __name__ == '__main__':
    print(platform.architecture())
    main()