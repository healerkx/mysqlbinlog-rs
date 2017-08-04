
from mysqlbinlog import *

import platform

def main():
    reader = BinLogReader('/Users/healer/mysql_binlog.000001')
    
    eh = EventHeader()
        
    count = 0
    while True:

        print('-' * 30)
        h = reader.read_event_header(eh)
        if not h:
            print("Empty")
            break
        
        event = reader.read_event(eh)
        
        info = EventInfo()
        
        reader.read_event_info(event, info)
        reader.free_event(event)
        count += 1
    
    reader.close()


if __name__ == '__main__':
    
    main()