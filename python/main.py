
from mysqlbinlog import *

import platform

def main():
    reader = BinLogReader('/Users/healer/mysql_binlog.000001')
    
    event_header = EventHeader()
        
    count = 0
    while True:

        print('-' * 30)
        h = reader.read_event_header(event_header)
        if not h:
            print("Empty")
            break
        
        event = reader.read_event(event_header)
        
        info = reader.read_event_info(event_header, event)
        if info.type_code == 19:
            
            db, table = reader.read_table_map_event(event, info)
            print(db, table)
        reader.free_event(event)
        count += 1
    
    reader.close()


if __name__ == '__main__':
    
    main()