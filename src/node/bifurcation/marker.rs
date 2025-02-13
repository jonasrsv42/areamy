use crate::connect::marker::Multiplicity;

pub struct Left {}
pub struct Right {}
impl Multiplicity for Left {}
impl Multiplicity for Right {}
