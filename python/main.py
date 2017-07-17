
from mysqlbinlog import *

import platform

def main():
    r = BinLogReader('/Users/healer/mysql_binlog.000001')
    
    h = r.read_event_header()
    print("#", h.contents.server_id, h.contents.timestamp)

    del(r)

if __name__ == '__main__':
    print(platform.architecture())
    main()