/// One per input side of a biunion node: its edges, phases or wakers.
pub struct Inputs<Left, Right> {
    pub left: Left,
    pub right: Right,
}
