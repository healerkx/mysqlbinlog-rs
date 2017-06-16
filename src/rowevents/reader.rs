
use rowevents::parser::Parser;
use rowevents::stream::Stream;
use rowevents::event_header::EventHeader;
use rowevents::events::*;
use std::io::Result;
use std::io::{Error, ErrorKind};

pub struct Reader {
    filename: String,
    parser: Parser,
    skip_next_event: bool
}

impl Reader {
    
    pub fn new(filename: &str) -> Result<Reader> {
        
        if let Some(stream) = Stream::from_file(filename) {
            let mut parser = Parser::new(stream);
            parser.read_binlog_file_header();
            Ok(Reader{
                filename: filename.to_string(),
                parser: parser,
                skip_next_event: false
                })
        } else {
            Err(Error::new(ErrorKind::Other, "oh no!"))
        }
    }

    pub fn is_concerned_event(&mut self, eh: &EventHeader) -> bool  {
        true
    }

    pub fn read_event(&mut self) -> Result<(EventHeader, Event)> {
        if let Ok(eh) = self.read_event_header() {
            if self.skip_next_event || !self.is_concerned_event(&eh) {
                
                if let Ok(e) = self.read_unknown_event(&eh) {
                    // Recover from skip
                    self.set_skip_next_event(false);
                    Ok((eh, e))
                } else {
                    Err(Error::new(ErrorKind::Other, "oh no!"))
                }
            } else if let Ok(e) = self.read_event_detail(&eh) {
                match e {
                    // Event::Xid(e) => println!("{:?}", e),
                    Event::TableMap(ref e) => {
                        if e.table_name == "table_2" {
                            println!("{}={}", e.table_name.len(), e.table_name);
                            self.set_skip_next_event(true);
                        }
                    },
                    _ => ()
                }
                Ok((eh, e))
                
            } else {
                Err(Error::new(ErrorKind::Other, "oh no!"))
            }
        } else {
            Err(Error::new(ErrorKind::Other, "oh no!"))
        }
    }

    pub fn read_event_header(&mut self) -> Result<EventHeader> {
        self.parser.read_event_header()
    }

    pub fn read_event_detail(&mut self, eh: &EventHeader) -> Result<Event> {
        
        match eh.get_event_type() {
            QUERY_EVENT => self.parser.read_query_event(eh),

            STOP_EVENT | ROTATE_EVENT => self.parser.read_rotate_event(eh),

            FORMAT_DESCRIPTION_EVENT => self.parser.read_format_descriptor_event(eh),
            XID_EVENT => self.parser.read_xid_event(eh),

            TABLE_MAP_EVENT  => self.parser.read_table_map_event(eh),

            // WRITE_ROWS_EVENT  => self.parser.read_event(eh),
            // UPDATE_ROWS_EVENT  => self.parser.read_event(eh),
            // DELETE_ROWS_EVENT  => self.parser.read_event(eh),

            WRITE_ROWS_EVENT2 => self.parser.read_write_event(eh),
            UPDATE_ROWS_EVENT2 => self.parser.read_update_event(eh),
            DELETE_ROWS_EVENT2 => self.parser.read_delete_event(eh),

            _ => self.parser.read_unknown_event(eh)
        }
    }

    pub fn read_unknown_event(&mut self, eh: &EventHeader) -> Result<Event> {
        self.parser.read_unknown_event(eh)
    }

    pub fn set_skip_next_event(&mut self, skip: bool) {
        self.skip_next_event = skip;
    }
}

impl Iterator for Reader {
    type Item = usize;

    // next() is the only required method
    fn next(&mut self) -> Option<usize> {
        // increment our count. This is why we started at zero.
        Some(0)
    }
}