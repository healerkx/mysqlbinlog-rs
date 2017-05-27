
extern crate mysqlbinlog;

use mysqlbinlog::rowevents::reader;

fn main() {
    let reader = reader::Reader::new("/Users/healer/data.000001");
    if let Ok(mut r) = reader {
        println!("{:?}", 1);
        while let Ok(e1) = r.read_event_header() {
            println!("{:?}", 2);
            r.read_event(&e1);
            print!("{}", e1.get_time());
        }
        
        //let e2 = r.read_event_header()?;
        print!("End");
    }
    
}