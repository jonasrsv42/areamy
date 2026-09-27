use crate::error::Error;
use crate::graph::marker::{Connection, Multiplicity, Unary};

mod default;

/// [`Get`] trait is used for retrieving connection(s) from a node that can be added to others..
///
/// This allows to [Get] connections from a node and then [crate::graph::Add] it to another.
/// Such as getting one of its input queues and adding as output queue to a different node.
/// E.g. for recieving outgoing or ingoing connections.
///
/// [Get] is generic over [Connection] to allow [Get::get] different types, for example
/// [crate::graph::Pushable]. It is also ?Sized because we make liberal use of
/// dynamic dispatch, hence a common pattern is to [Get::get] a `dyn Trait`.
///
/// [Get] is generic over [Multiplicity] to allow [Get::get] multiple inbound
/// [Connection] for a single node. Per default all implementations are [Unary] unless
/// otherwise stated to avoid specifying [Multiplicity] where not necessary.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot provide this connection",
    label = "has no `{ConnectionType}`"
)]
pub trait Get<ConnectionType: Connection + ?Sized, MultiplicityType: Multiplicity = Unary> {
    fn get(&self) -> Result<Box<ConnectionType>, Error>;
}
