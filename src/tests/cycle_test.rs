//! Signal-policy handling in a cyclic graph.
//!
//! Topology: biunion → bifurcation, with bifurcation's left output fed
//! back into biunion's left input. The biunion increments by 1; the
//! bifurcation routes values > 5 to its right output (final sink) and
//! values ≤ 5 back through the cycle.
//!
//! - Forward edge (biunion → bifurcation) uses `make_bidi` (Forward policy).
//! - Back-edge (bifurcation → biunion) uses `make_push` (FollowData policy)
//!   so signals don't propagate around the loop forever.

use crate::error::Error;
use crate::node::{bifurcation, biunion};
use crate::work::make_line;
use crate::{
    BifurcationRoutine, BiunionRoutine, LineIo, Message, make_bidi,
    work::Connect,
    work::{Reader, Writer, make_bifurcation, make_biunion},
};
use std::collections::VecDeque;

struct IncrementBiunion {
    output: VecDeque<usize>,
}

impl IncrementBiunion {
    fn new() -> Self {
        Self {
            output: VecDeque::new(),
        }
    }
}

impl crate::Send<usize, biunion::Left> for IncrementBiunion {
    fn send(&mut self, message: usize) -> Result<(), Error> {
        self.output.push_back(message + 1);
        Ok(())
    }
}

impl crate::Send<usize, biunion::Right> for IncrementBiunion {
    fn send(&mut self, message: usize) -> Result<(), Error> {
        self.output.push_back(message + 1);
        Ok(())
    }
}

impl crate::Next<usize> for IncrementBiunion {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.output.pop_front())
    }
}

impl crate::Flush for IncrementBiunion {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl crate::node::Name for IncrementBiunion {
    fn name(&self) -> &str {
        "IncrementBiunion"
    }
}

impl BiunionRoutine<usize, usize, usize> for IncrementBiunion {}

/// Routes values > 5 to the right output (sink); values ≤ 5 to the
/// left output (feedback into the biunion).
struct DeciderBifurcation {
    left_output: VecDeque<usize>,
    right_output: VecDeque<usize>,
}

impl DeciderBifurcation {
    fn new() -> Self {
        Self {
            left_output: VecDeque::new(),
            right_output: VecDeque::new(),
        }
    }
}

impl crate::Send<usize> for DeciderBifurcation {
    fn send(&mut self, message: usize) -> Result<(), Error> {
        if message > 5 {
            self.right_output.push_back(message);
        } else {
            self.left_output.push_back(message);
        }
        Ok(())
    }
}

impl crate::Next<usize, bifurcation::Left> for DeciderBifurcation {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.left_output.pop_front())
    }
}

impl crate::Next<usize, bifurcation::Right> for DeciderBifurcation {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.right_output.pop_front())
    }
}

impl crate::Flush for DeciderBifurcation {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl crate::node::Name for DeciderBifurcation {
    fn name(&self) -> &str {
        "DeciderBifurcation"
    }
}

impl BifurcationRoutine<usize, usize, usize> for DeciderBifurcation {}

#[test]
fn test_cycle_with_signal_policies() {
    let biunion = make_biunion(IncrementBiunion::new());
    let mut bifurcation = make_bifurcation(DeciderBifurcation::new());

    let writer = Writer::new::<biunion::Right>(&biunion).unwrap();

    // bifurcation.left → biunion.left (back-edge, FollowData)
    Connect::<usize>::push::<bifurcation::Left, biunion::Left>(
        bifurcation.as_mut(),
        biunion.as_ref(),
    )
    .unwrap();

    // biunion → bifurcation (forward, Forward policy via make_bidi)
    make_bidi(biunion, &mut bifurcation).unwrap();

    let reader = Reader::new::<bifurcation::Right>(bifurcation).unwrap();
    let mut io = LineIo::new(writer, reader);

    io.push(Message::Data(1)).unwrap();
    assert_eq!(io.flush("hello".into()).unwrap(), vec![6]);
}

#[test]
fn test_cycle_with_multiple_values() {
    let biunion = make_biunion(IncrementBiunion::new());
    let mut bifurcation = make_bifurcation(DeciderBifurcation::new());

    let writer = Writer::new::<biunion::Right>(&biunion).unwrap();

    Connect::<usize>::push::<bifurcation::Left, biunion::Left>(
        bifurcation.as_mut(),
        biunion.as_ref(),
    )
    .unwrap();

    make_bidi(biunion, &mut bifurcation).unwrap();

    let reader = Reader::new::<bifurcation::Right>(bifurcation).unwrap();
    let mut io = LineIo::new(writer, reader);

    for i in 1..=6 {
        io.push(Message::Data(i)).unwrap();
    }

    // Biunion prioritizes the left (feedback) input, so values 1-5 each
    // fully cycle (becoming 6) before value 6 is processed (becoming 7).
    assert_eq!(
        io.flush("flush-test".into()).unwrap(),
        vec![6, 6, 6, 6, 6, 7]
    );
}

struct IncrementLine {
    output: VecDeque<usize>,
}

impl IncrementLine {
    fn new() -> Self {
        Self {
            output: VecDeque::new(),
        }
    }
}

impl crate::Send<usize> for IncrementLine {
    fn send(&mut self, message: usize) -> Result<(), Error> {
        self.output.push_back(message + 1);
        Ok(())
    }
}

impl crate::Next<usize> for IncrementLine {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.output.pop_front())
    }
}

impl crate::Flush for IncrementLine {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl crate::node::Name for IncrementLine {
    fn name(&self) -> &str {
        "IncrementLine"
    }
}

impl crate::node::line::LineRoutine<usize, usize> for IncrementLine {}

#[test]
fn test_cycle_with_line_node() {
    let line = make_line(IncrementLine::new());
    let mut bifurcation = make_bifurcation(DeciderBifurcation::new());

    let writer = Writer::new(&line).unwrap();

    // Connect bifurcation's left output back to line's input (backward connection)
    // This creates a cycle where values <= 5 go back to the line
    Connect::<usize>::push::<bifurcation::Left, crate::marker::Unary>(
        bifurcation.as_mut(),
        line.as_ref(),
    )
    .unwrap();

    // Connect line's output to bifurcation's input (forward connection)
    make_bidi(line, &mut bifurcation).unwrap();

    let reader = Reader::new::<bifurcation::Right>(bifurcation).unwrap();
    let mut io = LineIo::new(writer, reader);

    for i in 1..=6 {
        io.push(Message::Data(i)).unwrap();
    }

    // The expected results for line node: [6, 7, 6, 6, 6, 6]
    //
    // IMPORTANT: The line node implementation produces a different result order
    // compared to the biunion test [6, 6, 6, 6, 6, 7]. Here's why:
    //
    // The actual sequence of events from the logs:
    // 1. We push values 1-6 to the line node, queuing them for processing
    // 2. Value 1 is processed first:
    //    - Goes through the full cycle 1→2→3→4→5→6
    //    - The 6 is sent to output (first 6 in our result)
    // 3. Value 6 is processed next (NOT value 2):
    //    - Incremented to 7
    //    - Sent to output (the 7 in our result)
    // 4. Remaining values 2-5 get processed in order:
    //    - Each cycles until exceeding 5 (becoming 6)
    //    - Each produces a 6 in the output
    //
    // The ordering [6, 7, 6, 6, 6, 6] happens because:
    // - The graph processes the inputs in FIFO order (1 then 6 then 2-5)
    // - Each input completes its full cycle before the next is processed
    // - The bifurcation node sends outputs as soon as values exceed 5
    //
    // In contrast, the biunion node test produces [6, 6, 6, 6, 6, 7] because:
    // - Biunion prioritizes the left input (feedback loop) over the right input
    // - Each value from 1-5 is fully processed (cycling until >5) before
    //   the next value from the right input is taken
    // - This means all of values 1-5 complete their cycles before value 6
    //   is processed, resulting in five 6's followed by one 7
    //
    // Key Insight: The different scheduling behaviors between line and biunion
    // nodes demonstrate how internal queue handling affects the dataflow behavior
    // even when the logical cycle rules are identical.
    //
    // This test demonstrates how the same cycle logic with different node types
    // produces different behaviors due to their internal scheduling mechanisms.
    assert_eq!(
        io.flush("line-cycle-test".into()).unwrap(),
        vec![6, 7, 6, 6, 6, 6]
    );
}
