



use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use std::option::Option;

pub struct Stream {
    file: File,
    content: Vec<u8>,
    offset: usize
}


impl Stream {

    pub fn from_file(filename: &str) -> Option<Stream> {
        let mut result = File::open(filename);
        if let Ok(mut file) = result {
            let mut content: Vec<u8> = vec![];
            file.read_to_end(&mut content);
            print!("{:?}", &content[0 .. 40]);
            Some(Stream {file: file, content: content, offset: 0})
        } else {
            None
        }
    }
     
    pub fn read(&mut self, size: usize) -> &[u8] {
        let from = self.offset;
        self.offset += size;
        // println!("({:?},{}, {:?})", from, size, &self.content[from .. from + size]);
        &self.content[from .. from + size]
    }

    pub fn get_content(&self) -> &Vec<u8> {
        &self.content
    }
}