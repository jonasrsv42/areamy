use crate::error::Error;
use crate::poll::Room;
use crate::poll::waker::Waker;

impl<RoomType: ?Sized + Room> Room for Box<RoomType> {
    fn poll(&mut self, waker: &mut Waker) -> Result<core::task::Poll<()>, Error> {
        RoomType::poll(self.as_mut(), waker)
    }
}
