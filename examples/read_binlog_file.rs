
extern crate mysqlbinlog;

use mysqlbinlog::rowevents::reader;
use mysqlbinlog::rowevents::events::Event;

fn main() {
    let reader = reader::Reader::new("/Users/healer/multi.log");
    if let Ok(mut r) = reader {
        
        while let Ok(e1) = r.read_event_header() {
            print!("[{}] ", e1.get_time());
            let event = r.read_event(&e1);
            match event {
                Ok(Event::Xid(e)) => println!("{:?}", e),
                Ok(Event::Insert(e)) => println!("INSERT {:?}", e),
                Ok(Event::Update(e)) => println!("UPDATE {:?}", e),
                Ok(Event::Delete(e)) => println!("DELETE {:?}", e),
                _ => println!("{:?}", e1)
            }
        }
        
        print!("End");
    }
    
}