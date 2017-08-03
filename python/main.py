
from mysqlbinlog import *

import platform

def main():
    reader = BinLogReader('/Users/healer/mysql_binlog.000001')
    
    eh = EventHeader()
        
    while True:
        print('-' * 30)
        h = reader.read_event_header(eh)
        if not h:
            print("Empty")
            break
        
        event = reader.read_event(eh)
        
        reader.read_event_info(event)

        reader.free_event(event)
    
    reader.close()


if __name__ == '__main__':
    
    main()