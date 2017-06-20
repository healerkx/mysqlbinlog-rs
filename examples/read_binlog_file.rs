
extern crate mysqlbinlog;

use mysqlbinlog::rowevents::reader;
use mysqlbinlog::rowevents::events::Event;
use std::io::Result;

fn main() {
    let reader = reader::Reader::new("/Users/healer/multi.log");
    if let Ok(mut r) = reader {
        
        

        while let Some((eh, e)) = r.next() {
            print!("[{}] ", eh.get_time());
            
            match e {
                Event::Xid(e) => println!("{:?}", e),
                Event::TableMap(e) => { println!("{:?}", e); },
                Event::Insert(e) => println!("INSERT {:?}", e),
                Event::Update(e) => println!("UPDATE {:?}", e),
                Event::Delete(e) => println!("DELETE {:?}", e),
                _ => println!("{:?}", eh)
            }
        }
        
        print!("End");
    }
    
}