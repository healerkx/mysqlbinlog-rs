import sys
sys.path.append('../pylib')
from mysqlbinlog import *
from ctypes import *
import platform, time
from optparse import OptionParser

def main(options, args):
    reader = BinLogReader(options.binlog)
    # TODO: reader?
    quit_when_eof = options.quit_when_eof
    milliseconds = options.milliseconds

    event_header = EventHeader()

    count = 0
    while True:
        # print('-' * 30)
        h = reader.read_event_header(event_header)
        if not h:
            if quit_when_eof:
                break
            else:
                seconds = milliseconds / 1000
                time.sleep(seconds)
                continue
        
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
    parser = OptionParser()

    parser.add_option("-s", "--source", action="store", dest="source", help="Provide source database")
    parser.add_option("-b", "--binlog", action="store", dest="binlog", help="Provide binlog file name")
    parser.add_option("-l", "--highlight", action="store", dest="highlight", help="Highlights the differences")
    parser.add_option("-e", "--exclude", action="store", dest="exclude", help="Provide the excluded db and table pattern")
    parser.add_option("-q", "--quit-when-eof", action="store", dest="quit_when_eof", help="Quit the program when EOF?", default=False)
    parser.add_option("-m", "--milliseconds", action="store", dest="milliseconds", help="Provide sleep seconds", default=10)
    options, args = parser.parse_args()

    if not options.binlog:
        print("binlog filename is required")
        exit()
    
    b = time.clock()
    main(options, args)
    e = time.clock()
    print('\nCost', e - b, 'seconds')