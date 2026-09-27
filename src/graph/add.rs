use crate::error::Error;
use crate::graph::marker::{Connection, Multiplicity, Unary};

mod default;

/// [`Add`] trait is used to create [Connection]s between our nodes.
/// it also lets us declare the type of [Connection]s that nodes are
/// expected to have through generics with dynamic dispatch.
///
/// [Add] is generic over [Connection] to allow [Add::add] different types, for example
/// [crate::work::Workable] or [crate::graph::Pushable]. It is also ?Sized because we make liberal use of
/// dynamic dispatch, hence a common pattern is to [Add::add] `dyn Trait`.
///
/// [Add] is generic over [Multiplicity] to allow [Add::add] multiple inbound and outbound
/// [Connection] for a single node. Per default all implementations are [Unary] unless
/// otherwise stated to avoid specifying [Multiplicity] where not necessary.
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot take this connection",
    label = "does not accept `{ConnectionType}`"
)]
pub trait Add<ConnectionType: Connection + ?Sized, MultiplicityType: Multiplicity = Unary> {
    fn add(&mut self, connection: Box<ConnectionType>) -> Result<(), Error>;
}
