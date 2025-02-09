use crate::{marker::Connection, Pullable, Pushable, Workable};
pub struct Bridge<PullableType, PushableType>
where
    PullableType: Pullable,
    PushableType: Pushable<Message = PullableType::Message>,
{
    pullable: PullableType,
    pushable: PushableType,
}

impl<PullableType, PushableType> Connection for Bridge<PullableType, PushableType>
where
    PullableType: Pullable,
    PushableType: Pushable<Message = PullableType::Message>,
{
}

impl<PullableType, PushableType> Workable for Bridge<PullableType, PushableType>
where
    PullableType: Pullable,
    PushableType: Pushable<Message = PullableType::Message>,
{
    type ThreadId = PullableType::ThreadId;

    fn work(&mut self) -> Result<(), crate::error::Error> {
        let msg = self.pullable.pull()?;
        self.pushable.push(msg)
    }
}

impl<PullableType, PushableType> Bridge<PullableType, PushableType>
where
    PullableType: Pullable,
    PushableType: Pushable<Message = PullableType::Message>,
{
    pub fn new(pullable: PullableType, pushable: PushableType) -> Self {
        Self { pullable, pushable }
    }
}
