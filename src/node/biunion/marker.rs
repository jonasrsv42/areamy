use crate::graph::marker::Multiplicity;

pub struct Left;
pub struct Right;
impl Multiplicity for Left {}
impl Multiplicity for Right {}

/// Marker trait for biunion sides. Used to dispatch `.parent::<Side>(node)`.
pub trait Side: Multiplicity {
    /// This side's one of a left/right pair.
    fn pick<'a, T>(left: &'a mut T, right: &'a mut T) -> &'a mut T;
}

impl Side for Left {
    fn pick<'a, T>(left: &'a mut T, _right: &'a mut T) -> &'a mut T {
        left
    }
}

impl Side for Right {
    fn pick<'a, T>(_left: &'a mut T, right: &'a mut T) -> &'a mut T {
        right
    }
}
