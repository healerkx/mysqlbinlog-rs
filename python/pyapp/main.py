import sys
sys.path.append('../pylib')
from mysqlbinlog import *
from ctypes import *
import platform, time, re
from optparse import OptionParser


def print_row(row):
    for item in row:
        if item is None:
            print('<none>', end=', ')
        elif type(item) == bytes:
            print(str(item, 'utf8'), end=', ')
        else:
            print(item, end=', ')
    print()


def print_updates(old, new):
    count, index = len(old), 0
    while index < count:
        print_row(old[index])
        print_row(new[index])
        index += 1

def print_inserts(rows):
    for row in rows:
        print_row(row)

def print_deletes(rows):
    for row in rows:
        print_row(row)

def print_table_time_info(timestamp, db_table, operation):
    print("[%s] %s %s" % (formatted_time(timestamp), operation, db_table))

def main(options, args):
    reader = BinLogReader(options.binlog)
    # TODO: reader?
    quit_when_eof = options.quit_when_eof
    milliseconds = options.milliseconds

    reg_list = []
    if options.ignore:
        ignore = options.ignore.replace('%', '\\w*').replace('.', '\\.')
        patterns = ignore.split(',')
        for pattern in patterns:
            reg = re.compile(pattern)
            reg_list.append(reg)

    event_header = EventHeader()

    count = 0
    skip = False
    current_db_table = ''
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
        if not event:
            continue

        if skip:
            skip = False
            continue
        
        event_info = reader.read_event_info(event_header, event)
        if event_info.type_code == EventType.TABLE_MAP_EVENT:
            db, table = reader.read_table_map_event(event, event_info)
            full_name = db + '.' + table
            for reg in reg_list:
                if reg.match(full_name):
                    skip = True
                    # print('Skip', full_name)
                    break
            if skip:
                continue
            current_db_table = full_name
        elif event_info.type_code == EventType.DELETE_ROWS_EVENT2:
            rows = reader.read_delete_event_rows(event, event_info)
            print_table_time_info(event_header.timestamp, current_db_table, 'delete')
            print_deletes(rows)
        elif event_info.type_code == EventType.UPDATE_ROWS_EVENT2:
            old, new = reader.read_update_event_rows(event, event_info)
            print_table_time_info(event_header.timestamp, current_db_table, 'update')
            print_updates(old, new)
        elif event_info.type_code == EventType.WRITE_ROWS_EVENT2:
            rows = reader.read_insert_event_rows(event, event_info)
            print_table_time_info(event_header.timestamp, current_db_table, 'insert')
            print_inserts(rows)
            
        reader.free_event(event)
        count += 1
    
    reader.close()


if __name__ == '__main__':
    parser = OptionParser()

    parser.add_option("-s", "--source", action="store", dest="source", help="Provide source database")
    parser.add_option("-b", "--binlog", action="store", dest="binlog", help="Provide binlog file name")
    parser.add_option("-l", "--highlight", action="store", dest="highlight", help="Highlights the differences")
    parser.add_option("-i", "--ignore", action="store", dest="ignore", help="The db and table pattern to ignore")
    parser.add_option("-q", "--quit-when-eof", action="store", dest="quit_when_eof", help="Quit the program when EOF?", default=False)
    parser.add_option("-m", "--milliseconds", action="store", dest="milliseconds", help="Provide sleep seconds", default=100)
    options, args = parser.parse_args()
    
    if not options.binlog:
        print("binlog filename is required")
        exit()
    
    b = time.clock()
    main(options, args)
    e = time.clock()
    print('\nCost', e - b, 'seconds')