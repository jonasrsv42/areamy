use crate::marker::Multiplicity;

pub struct Left;
pub struct Right;
impl Multiplicity for Left {}
impl Multiplicity for Right {}

/// Marker trait for biunion sides. Used to dispatch `.parent::<Side>(node)`.
pub trait Side: Multiplicity {}
impl Side for Left {}
impl Side for Right {}
