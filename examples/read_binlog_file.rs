
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde;
extern crate mysqlbinlog;
extern crate toml;

use std::io::Result;
use std::env;
use std::fs::File;
use std::io::prelude::*;

use mysqlbinlog::rowevents::reader;
use mysqlbinlog::rowevents::events::Event;
use mysqlbinlog::rowevents::event_header::EventHeader;

#[derive(Deserialize, Serialize, Debug)]
struct Filter {
    excluded: Vec<String>
}

#[derive(Deserialize, Serialize, Debug)]
struct Config {
    filter: Filter
}

fn parse_config(config_file: &str) -> Result<Config> {
    let mut file = File::open(config_file)?;
    let mut content = String::new();
    file.read_to_string(&mut content);
    let config: Config = toml::from_str(&content).unwrap();
    Ok(config)
}

fn print_event(eh: &EventHeader, e: &Event) {
    println!("[{}] {:?}", eh.get_time(), e);
}

fn main() {

    let mut u: Vec<_> = env::args().collect::<Vec<_>>().clone();
    let u: Vec<_> = u.drain(1..).collect();
    let mut binlog_file = "".to_string();
    let mut config_file = "".to_string();
    for argument in u {
        let pair = argument.split("=").collect::<Vec<_>>();
        if pair[0] == "--binlog" {
            binlog_file = pair[1].to_string();
        } else if pair[0] == "--config" {
            config_file = pair[1].to_string();
        }
    }

    let config = parse_config(&config_file).unwrap();
    println!("{:?}", config.filter);

    let reader = reader::Reader::new(&binlog_file);
    if let Ok(mut r) = reader {
        r.add_excluded_db_table("antares.*");
        
        while let Some((eh, e)) = r.next() {
            if r.skip_next_event() {
                continue;
            }
            match e {
                Event::Insert(e) => print_event(&eh, &Event::Insert(e)),
                Event::Delete(e) => print_event(&eh, &Event::Delete(e)),
                Event::Update(e) => print_event(&eh, &Event::Update(e)),
                Event::Unknown | _ => {  },
            }
        }
        
        print!("End");
    }
    
}