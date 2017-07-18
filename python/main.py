
from mysqlbinlog import *

import platform

def main():
    reader = BinLogReader('/Users/healer/mysql_binlog.000001')
    
    p = EventHeader()
        
    while True:
        h = reader.read_event_header(p)
        if not h:
            print("Empty")
            break
        
        reader.read_event(p)
    
    reader.close()


if __name__ == '__main__':
    
    main()