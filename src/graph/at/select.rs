use crate::graph::marker::Connection;
use core::marker::PhantomData;

/// [`Select`] is a node moved at one side. See [crate::At::at].
#[must_use = "a moved side only feeds `via` / `reader`; borrow with `at_mut` / `at_ref` for the rest"]
pub struct Select<NodeType, MultiplicityType> {
    node: NodeType,
    multiplicity: PhantomData<MultiplicityType>,
}

impl<NodeType: Connection, MultiplicityType> Connection for Select<NodeType, MultiplicityType> {}

impl<NodeType, MultiplicityType> Select<NodeType, MultiplicityType> {
    pub(super) fn new(node: NodeType) -> Self {
        Self {
            node,
            multiplicity: PhantomData,
        }
    }

    pub(crate) fn into_node(self) -> NodeType {
        self.node
    }
}
