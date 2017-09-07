
from mysqlbinlog import *
from ctypes import *
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
        
        event_info = reader.read_event_info(event_header, event)
        if event_info.type_code == EventType.TABLE_MAP_EVENT:
            db, table = reader.read_table_map_event(event, event_info)
            print(db, table)
        elif event_info.type_code == EventType.DELETE_ROWS_EVENT2:
            reader.read_delete_event_rows(event, event_info)
        elif event_info.type_code == EventType.UPDATE_ROWS_EVENT2:
            old, new = reader.read_update_event_rows(event, event_info)

            for i in old[0]:
                print(i)
            for i in new[0]:
                print(i)
            
        elif event_info.type_code == EventType.WRITE_ROWS_EVENT2:
            reader.read_insert_event_rows(event, event_info)
            print('$$')
            
        reader.free_event(event)
        count += 1
    
    reader.close()


if __name__ == '__main__':
    
    main()