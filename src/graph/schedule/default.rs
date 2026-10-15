use crate::graph::Schedule;
use crate::graph::marker::Multiplicity;
use crate::wake;

impl<T, MultiplicityType> Schedule<MultiplicityType> for Box<T>
where
    T: ?Sized + Schedule<MultiplicityType>,
    MultiplicityType: Multiplicity,
{
    fn schedule(&mut self, waker: wake::Waker) {
        T::schedule(self.as_mut(), waker);
    }
}
