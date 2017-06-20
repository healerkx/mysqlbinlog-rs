
extern crate mysqlbinlog;

use mysqlbinlog::rowevents::reader;
use mysqlbinlog::rowevents::events::Event;

fn main() {
    let reader = reader::Reader::new("/Users/healer/mysql_binlog.000002");
    if let Ok(mut r) = reader {
        r.add_excluded_db_name("antares".to_string());
        
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