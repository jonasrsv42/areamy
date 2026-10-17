//! Bifurcation: Flush then close reaches both output sides in order.

use super::mock::{HoldBifurcation, RIGHT, drain, drive_work};
use crate::error::Error;
use crate::work;
use crate::{At, Closeable, DefaultThread, Message, Pushable, Tees, Workable, Writes, bifurcation};

#[test]
fn work_bifurcation_flush_then_close() -> Result<(), Error> {
    let mut node = work::Bifurcation::of(HoldBifurcation::new());
    let mut writer = node.writer()?;
    let left = node.at_mut::<bifurcation::Left>().tee()?;
    let right = node.at_mut::<bifurcation::Right>().tee()?;

    writer.push(Message::Data(1))?;
    writer.push(Message::Flush("f".into()))?;
    writer.close()?;

    let mut workable: Box<dyn Workable<ThreadId = DefaultThread>> = Box::new(node);
    drive_work(&mut *workable)?;
    assert_eq!(
        drain(left)?,
        vec![Message::Data(1), Message::Flush("f".into())]
    );
    assert_eq!(
        drain(right)?,
        vec![Message::Data(1 + RIGHT), Message::Flush("f".into())]
    );
    Ok(())
}
