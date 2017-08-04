

from ctypes import *
import time

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




class BinLogReader:
    """
    """

    def __init__(self, filename):
        self.d = cdll.LoadLibrary('/Users/healer/Projects/privates/mysqlbinlog-rs/target/debug/libmysqlbinlog.dylib')
        self.d.binlog_reader_new.restype = c_void_p
        self.reader = self.d.binlog_reader_new(bytes(filename, 'utf8'))


    def close(self):
        self.d.binlog_reader_free.argtypes = [c_void_p]
        self.d.binlog_reader_free(self.reader)

    def read_event_header(self, header):
        self.d.binlog_reader_read_event_header.restype = c_byte
        self.d.binlog_reader_read_event_header.argtypes = [c_void_p, POINTER(EventHeader)]
        b = self.d.binlog_reader_read_event_header(self.reader, byref(header))
        return b

    def read_event(self, header):
        """
        """        
        self.d.binlog_reader_read_event.restype = c_void_p
        self.d.binlog_reader_read_event.argtypes = [c_void_p, POINTER(EventHeader)]
        
        event = self.d.binlog_reader_read_event(self.reader, byref(header))
        
        return event
        """
        event_type = header.type_code
        if event_type == EventType.WRITE_ROWS_EVENT2:
            pass
        elif event_type == EventType.UPDATE_ROWS_EVENT2:
            pass
        elif event_type == EventType.DELETE_ROWS_EVENT2:
            pass
        elif event_type == EventType.XID_EVENT:
            pass
        elif event_type == EventType.TABLE_MAP_EVENT:
            pass
        elif event_type == EventType.ROTATE_EVENT:
            pass
        elif event_type == EventType.STOP_EVENT:
            pass                                             
        elif event_type == EventType.UNKNOWN_EVENT:
            pass
        else:
            pass
        """

    def read_event_info(self, event_header, event):
        """
        """
        self.d.binlog_reader_read_event_info.restype = c_bool
        self.d.binlog_reader_read_event_info.argtypes = [c_void_p, POINTER(EventInfo)]
        event_info = EventInfo(event_header)
        self.d.binlog_reader_read_event_info(event, byref(event_info))
        return event_info

    def read_table_map_event(self, event, event_info):
        db_name_t = c_char * event_info.db_name_len
        table_name_t = c_char * event_info.table_name_len

        db_name = db_name_t()
        table_name = table_name_t()
        self.d.binlog_reader_read_table_map_event.restype = c_bool
        self.d.binlog_reader_read_table_map_event.argtypes = [c_void_p, POINTER(EventInfo), c_char_p, c_char_p]
        self.d.binlog_reader_read_table_map_event(event, byref(event_info), db_name, table_name)
        db_name = str(db_name.value, 'utf-8')
        table_name = str(table_name.value, 'utf-8')
        return db_name, table_name

    def free_event(self, event):
        """
        """        
        self.d.binlog_reader_free_event.restype = c_bool
        self.d.binlog_reader_free_event.argtypes = [c_void_p]
        return self.d.binlog_reader_free_event(event)