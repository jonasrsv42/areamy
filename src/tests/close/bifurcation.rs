//! Bifurcation: Flush then close reaches both output sides in order.

use super::mock::{HoldBifurcation, RIGHT, drain, drive_work};
use crate::error::Error;
use crate::reader::work::tee;
use crate::work::{Writer, make_bifurcation};
use crate::{Closeable, DefaultThread, Message, Pushable, Workable, bifurcation};

#[test]
fn work_bifurcation_flush_then_close() -> Result<(), Error> {
    let mut node = make_bifurcation(HoldBifurcation::new());
    let mut writer = Writer::new(&node)?;
    let mut left = tee::Reader::new::<bifurcation::Left>(&mut node)?;
    let mut right = tee::Reader::new::<bifurcation::Right>(&mut node)?;

    writer.push(Message::Data(1))?;
    writer.push(Message::Flush("f".into()))?;
    writer.close()?;

    let mut workable: Box<dyn Workable<ThreadId = DefaultThread>> = node;
    drive_work(&mut *workable)?;
    assert_eq!(
        drain(&mut left)?,
        vec![Message::Data(1), Message::Flush("f".into())]
    );
    assert_eq!(
        drain(&mut right)?,
        vec![Message::Data(1 + RIGHT), Message::Flush("f".into())]
    );
    Ok(())
}
