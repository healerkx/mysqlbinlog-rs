import sys
sys.path.append('../pylib')
from mysqlbinlog import *
from ctypes import *
import platform, time

def main():
    reader = BinLogReader('/Users/healer/mysql_binlog.000001')
    
    event_header = EventHeader()
    
    count = 0
    while True:
        # print('-' * 30)
        h = reader.read_event_header(event_header)
        if not h:
            print("Empty")
            break
        
        event = reader.read_event(event_header)
        
        event_info = reader.read_event_info(event_header, event)
        if event_info.type_code == EventType.TABLE_MAP_EVENT:
            db, table = reader.read_table_map_event(event, event_info)
            print(db, table)
        elif event_info.type_code == EventType.DELETE_ROWS_EVENT2:
            rows = reader.read_delete_event_rows(event, event_info)
            print(rows)
        elif event_info.type_code == EventType.UPDATE_ROWS_EVENT2:
            old, new = reader.read_update_event_rows(event, event_info)
            print(old)
            print(new)   
        elif event_info.type_code == EventType.WRITE_ROWS_EVENT2:
            rows = reader.read_insert_event_rows(event, event_info)
            print(rows)
            
        reader.free_event(event)
        count += 1
    
    reader.close()


if __name__ == '__main__':
    b = time.clock()
    main()
    e = time.clock()
    print(e - b)