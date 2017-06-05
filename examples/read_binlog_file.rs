
extern crate mysqlbinlog;

use mysqlbinlog::rowevents::reader;
use mysqlbinlog::rowevents::events::Event;

fn main() {
    let reader = reader::Reader::new("/Users/healer/data.log");
    if let Ok(mut r) = reader {
        println!("{:?}", 1);
        while let Ok(e1) = r.read_event_header() {
            println!("-------------------------------------------");
            println!("{}", e1.get_time());
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