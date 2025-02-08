use crate::error::Error;
use crate::Pullable;

pub fn read_until<ThreadIdType, MessageType>(
    pullable: &mut impl Pullable<ThreadId = ThreadIdType, Message = MessageType>,
    condition: MessageType,
) -> Result<Vec<MessageType>, Error>
where
    MessageType: PartialEq,
{
    let mut a = Vec::new();
    loop {
        let message = pullable.pull()?;

        if message == condition {
            return Ok(a);
        }

        a.push(message)
    }
}
