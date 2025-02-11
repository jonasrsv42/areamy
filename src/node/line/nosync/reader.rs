use crate::error::Error;
use crate::Pullable;

/// [`read_until`] will [Pullable::pull] until it recieves the target [Pullable::Message]
///
/// * `pullable` - the [Pullable] to [Pullable::pull].
/// * `condition` - a [Pullable::Message]
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
