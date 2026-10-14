/// One per output side of a bifurcation node: its fan-outs or pending signals.
#[derive(Default)]
pub struct Sides<Left, Right> {
    pub left: Left,
    pub right: Right,
}
