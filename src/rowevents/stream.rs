
use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use std::option::Option;
use std::process;
use std::io::Result;

pub struct Stream {
    file: File,
    content: Vec<u8>,
    offset: usize
}


impl Stream {

    pub fn from_file(filename: &str) -> Option<Stream> {
        let mut result = File::open(filename);
        if let Ok(mut file) = result {
            Some(Stream {file: file, content: vec![], offset: 0})
        } else {
            None
        }
    }
     
    pub fn read(&mut self, size: usize) -> &[u8] {
        let from = self.offset;
        self.offset += size;

        if from + size >= self.content.len() {
            match self.read_file(size) {
                Ok(0) => {
                    // TODO: Wait or Quit?    
                    println!("Reach the end of this binlog file");
                    process::exit(0x0000);
                },
                _ => {}
            }
        }
        
        &self.content[from .. from + size]
    }

    // try! Read size * 2 bytes from file
    pub fn read_file(&mut self, size: usize) -> Result<usize> {
        let mut buffer = Vec::with_capacity(size * 2);
        buffer.resize(size * 2, 0);
        let read = self.file.read(&mut buffer)?;
        self.content.extend_from_slice(&buffer[0..read]);
        Ok(read)
    }
}