/// One per input side of a biunion node: its edges, parents, phases or wakers.
#[derive(Default)]
pub struct Inputs<Left, Right> {
    pub left: Left,
    pub right: Right,
}
