
from ctypes import *
import time, sys, os
from decimal import Decimal as D

# TODO: setup.py
class EventType:
    UNKNOWN_EVENT = 0
    START_EVENT_V3 = 1
    QUERY_EVENT = 2
    STOP_EVENT = 3
    ROTATE_EVENT = 4
    INTVAR_EVENT = 5
    LOAD_EVENT = 6
    SLAVE_EVENT = 7
    CREATE_FILE_EVENT = 8
    APPEND_BLOCK_EVENT = 9
    EXEC_LOAD_EVENT = 10
    DELETE_FILE_EVENT = 11
    NEW_LOAD_EVENT = 12
    RAND_EVENT = 13
    USER_VAR_EVENT = 14
    FORMAT_DESCRIPTION_EVENT = 15
    XID_EVENT = 16
    BEGIN_LOAD_QUERY_EVENT = 17
    EXECUTE_LOAD_QUERY_EVENT = 18
    TABLE_MAP_EVENT  = 19
    PRE_GA_WRITE_ROWS_EVENT  = 20
    PRE_GA_UPDATE_ROWS_EVENT  = 21
    PRE_GA_DELETE_ROWS_EVENT  = 22
    """
    From MySQL 5.1.18 events
    """    
    WRITE_ROWS_EVENT  = 23
    UPDATE_ROWS_EVENT  = 24
    DELETE_ROWS_EVENT  = 25
    # ----------------------------------

    INCIDENT_EVENT = 26
    HEARTBEAT_LOG_EVENT = 27

    """
    From MySQL 5.6.2 events
    """
    WRITE_ROWS_EVENT2 = 30
    UPDATE_ROWS_EVENT2 = 31
    DELETE_ROWS_EVENT2 = 32
    # ----------------------------------

def formatted_time(unixtime):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(unixtime))


class EventHeader(Structure):
    _fields_ = [
        ("timestamp", c_int),
        ("type_code", c_byte),
        ("server_id", c_int),
        ("event_len", c_int),
        ("next_pos", c_int),
        ("flags", c_short)
        ]

    def __str__(self):
        return "<HEADER: %s, %d>" % (formatted_time(self.timestamp), self.type_code)


class EventInfo(Structure):

    def __init__(self, event_header):
        self.type_code = event_header.type_code

    _fields_ = [
        ("type_code", c_byte),
        ("db_name_len", c_uint),
        ("table_name_len", c_uint),
        ("row_count", c_uint),
        ("col_count", c_uint)
    ]


def timestamp_datetime(value):
    format = '%Y-%m-%d %H:%M:%S'
    # value为传入的值为时间戳(整形)，如：1332888820
    value = time.localtime(value)
    ## 经过localtime转换后变成
    ## time.struct_time(tm_year=2012, tm_mon=3, tm_mday=28, tm_hour=6, tm_min=53, tm_sec=40, tm_wday=2, tm_yday=88, tm_isdst=0)
    # 最后再经过strftime函数转换为正常日期格式。
    dt = time.strftime(format, value)
    return dt

class FieldInfo(Structure):
    _fields_ = [
        ("field_type", c_uint32),
        ("field_len", c_uint32),
        ("field_value", c_int64)
    ]

    def value(self):
        if self.field_type in [1, 2, 3, 8]:
            return self.field_value
        elif self.field_type == 253:
            return string_at(self.field_value, self.field_len)
        elif self.field_type == 4 or self.field_type == 5:
            u = self.as_utf8_str()
            if u == '':
                return ''
            return float(u)
        elif self.field_type == 246:
            s = str(string_at(self.field_value, self.field_len), 'utf-8')
            return D(s)
        elif self.field_type == 6:
            return None
        elif self.field_type == 7 or self.field_type == 17:
            return timestamp_datetime(self.field_value)
        else:
            return '?'
    
    def as_utf8_str(self):
        b = string_at(self.field_value, self.field_len)
        return str(b, 'utf-8')

    def __str__(self):
        return "<%s: %s>" % (self.field_type, self.field_value)


class BinLogReader:
    """
    """

    def __init__(self, filename, debug=False):
        project_path = os.path.realpath(os.path.join(__file__, '../../../'))
        path = 'debug' if debug else 'release'
        self.d = cdll.LoadLibrary('%s/target/%s/libmysqlbinlog.dylib' % (project_path, path))
        
        self.d.binlog_reader_new.restype = c_void_p
        self.reader = self.d.binlog_reader_new(bytes(filename, 'utf8'))
        if not self.reader:
            print("Failed to open '%s'." % filename)
            exit()
        #
        self.d.binlog_reader_free.argtypes = [c_void_p]
        #
        self.d.binlog_reader_read_event_header.restype = c_byte
        self.d.binlog_reader_read_event_header.argtypes = [c_void_p, POINTER(EventHeader)]

        #
        self.d.binlog_reader_read_event.restype = c_void_p
        self.d.binlog_reader_read_event.argtypes = [c_void_p, POINTER(EventHeader)]

        #
        self.d.binlog_reader_read_event_info.restype = c_bool
        self.d.binlog_reader_read_event_info.argtypes = [c_void_p, POINTER(EventInfo)]
        
        #
        self.d.binlog_reader_read_table_map_event.restype = c_bool
        self.d.binlog_reader_read_table_map_event.argtypes = [c_void_p, POINTER(EventInfo), c_char_p, c_char_p]
        
        #                   
        self.d.binlog_reader_read_rows_event_content.restype = c_bool
        # content_t is dynamic
        # self.d.binlog_reader_read_rows_event_content.argtypes = [c_void_p, POINTER(EventInfo), POINTER(content_t), c_bool]

        #
        self.d.binlog_reader_free_event.restype = c_bool
        self.d.binlog_reader_free_event.argtypes = [c_void_p]

    #
    def close(self):
        self.d.binlog_reader_free(self.reader)

    def read_event_header(self, header):
        b = self.d.binlog_reader_read_event_header(self.reader, byref(header))
        return b

    def read_event(self, header):
        """
        """
        event = self.d.binlog_reader_read_event(self.reader, byref(header))
        return event


    def read_event_info(self, event_header, event):
        """
        """
        event_info = EventInfo(event_header)
        self.d.binlog_reader_read_event_info(event, byref(event_info))
        return event_info

    def read_table_map_event(self, event, event_info):
        db_name_t = c_char * event_info.db_name_len
        table_name_t = c_char * event_info.table_name_len

        db_name = db_name_t()
        table_name = table_name_t()

        self.d.binlog_reader_read_table_map_event(event, byref(event_info), db_name, table_name)
        db_name = str(db_name.value, 'utf-8')
        table_name = str(table_name.value, 'utf-8')
        return db_name, table_name

    #
    def __parse_content(self, content, row_count, col_count):
        i, j = 0, 0
        rows = []
        for i in range(0, row_count):
            row = []
            for j in range(0, col_count):
                index = i * col_count + j
                f = content[index].value()
                row.append(f)
            rows.append(row)
        return rows
    
    # new_entry is for update rows event
    def read_rows_event_content(self, event, event_info, new_entry=True):
        '''
        '''
        count = event_info.row_count * event_info.col_count
        content_t = FieldInfo * count
        content = content_t()
        # TODO: Cache the restype and argtypes
        # self.d.binlog_reader_read_rows_event_content.restype = c_bool
        self.d.binlog_reader_read_rows_event_content.argtypes = [c_void_p, POINTER(EventInfo), POINTER(content_t), c_bool]
        self.d.binlog_reader_read_rows_event_content(event, byref(event_info), byref(content), new_entry)
        
        items = self.__parse_content(content, event_info.row_count, event_info.col_count)

        self.d.binlog_reader_free_rows_event_content.argtypes = [c_void_p, POINTER(EventInfo), POINTER(content_t)]
        self.d.binlog_reader_free_rows_event_content(event, byref(event_info), byref(content))

        return items


    def read_insert_event_rows(self, event, event_info):
        return self.read_rows_event_content(event, event_info, True)

    def read_update_event_rows(self, event, event_info):
        old = self.read_rows_event_content(event, event_info, False)
        new = self.read_rows_event_content(event, event_info, True)
        return old, new
    #
    def read_delete_event_rows(self, event, event_info):
        return self.read_rows_event_content(event, event_info, True)


    def free_event(self, event):
        """
        """
        return self.d.binlog_reader_free_event(event)
