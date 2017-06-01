
pub trait BaseDescriptor {

    fn handle_metadata(&self);
    
}

pub struct IntDescriptor {

}

impl IntDescriptor {

}

impl BaseDescriptor for IntDescriptor {

    fn handle_metadata(&self) {

    }
}


pub enum Descriptor {
    Int(IntDescriptor)
}