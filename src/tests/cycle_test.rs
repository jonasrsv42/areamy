//! Test to demonstrate signal policy handling in a cyclic graph.
//!
//! This test creates a cycle between a biunion and bifurcation:
//! - Biunion takes inputs from right side and increments by 1, forwarding to bifurcation
//! - Bifurcation decides if output value > 5:
//!   - If > 5: outputs to the sink
//!   - If <= 5: sends back to biunion's left input for further processing
//!
//! The forward connection (biunion -> bifurcation) uses make_bidi with Forward policy
//! The backward connection (bifurcation -> biunion) uses make_push with FollowData policy
//!
//! This demonstrates how our signal policies prevent infinite cycles of signals
//! while allowing data to flow correctly.

use crate::error::Error;
use crate::node::{bifurcation, biunion};
use crate::{
    make_bidi,
    work::Connect,
    work::{make_bifurcation, make_biunion, Sink, Source},
    BifurcationRoutine, BiunionRoutine, LineReader, Message,
};
use std::collections::VecDeque;

// Biunion routine that increments values by 1
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

// Implementation for biunion's left input (from feedback)
impl crate::Send<usize, biunion::Left> for IncrementBiunion {
    fn send(&mut self, message: usize) -> Result<(), Error> {
        println!(
            "Biunion received {} on left input, incrementing to {}",
            message,
            message + 1
        );
        self.output.push_back(message + 1);
        Ok(())
    }
}

// Implementation for biunion's right input (from initial input)
impl crate::Send<usize, biunion::Right> for IncrementBiunion {
    fn send(&mut self, message: usize) -> Result<(), Error> {
        println!(
            "Biunion received {} on right input, incrementing to {}",
            message,
            message + 1
        );
        self.output.push_back(message + 1);
        Ok(())
    }
}

// Output implementation
impl crate::Next<usize> for IncrementBiunion {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        let result = self.output.pop_front();
        if result.is_some() {
            println!("Biunion outputting {}", result.unwrap());
        }
        Ok(result)
    }
}

impl crate::Flush for IncrementBiunion {
    fn flush(&mut self) -> Result<(), Error> {
        println!("Biunion flushed");
        Ok(())
    }
}

impl crate::node::Name for IncrementBiunion {
    fn name<'a>(&'a self) -> &'a str {
        "IncrementBiunion"
    }
}

impl BiunionRoutine<usize, usize, usize> for IncrementBiunion {}

// Bifurcation routine that decides based on value > 5
struct DeciderBifurcation {
    left_output: VecDeque<usize>,  // For values <= 5, going back to biunion
    right_output: VecDeque<usize>, // For values > 5, going to final output
}

impl DeciderBifurcation {
    fn new() -> Self {
        Self {
            left_output: VecDeque::new(),
            right_output: VecDeque::new(),
        }
    }
}

// Input implementation
impl crate::Send<usize> for DeciderBifurcation {
    fn send(&mut self, message: usize) -> Result<(), Error> {
        println!("Bifurcation received {}, deciding...", message);

        if message > 5 {
            println!("Value {} > 5, sending to right output", message);
            self.right_output.push_back(message);
        } else {
            println!("Value {} <= 5, sending to left output (feedback)", message);
            self.left_output.push_back(message);
        }

        Ok(())
    }
}

// Left output implementation (feedback to biunion)
impl crate::Next<usize, bifurcation::Left> for DeciderBifurcation {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        let result = self.left_output.pop_front();
        if result.is_some() {
            println!("Bifurcation left output: {}", result.unwrap());
        }
        Ok(result)
    }
}

// Right output implementation (final output)
impl crate::Next<usize, bifurcation::Right> for DeciderBifurcation {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        let result = self.right_output.pop_front();
        if result.is_some() {
            println!("Bifurcation right output: {}", result.unwrap());
        }
        Ok(result)
    }
}

impl crate::Flush for DeciderBifurcation {
    fn flush(&mut self) -> Result<(), Error> {
        println!("Bifurcation flushed");
        Ok(())
    }
}

impl crate::node::Name for DeciderBifurcation {
    fn name<'a>(&'a self) -> &'a str {
        "DeciderBifurcation"
    }
}

impl BifurcationRoutine<usize, usize, usize> for DeciderBifurcation {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cycle_with_signal_policies() {
        // Create our nodes
        let biunion = make_biunion(Ok(IncrementBiunion::new())).unwrap();
        let mut bifurcation = make_bifurcation(Ok(DeciderBifurcation::new())).unwrap();

        // Create a source to input to biunion's right side
        let source = Source::new::<biunion::Right>(&biunion).unwrap();

        // Connect bifurcation's left output back to biunion's left input (backward connection)
        // This uses SignalPolicy::FollowData by default in make_push
        Connect::<usize>::push::<bifurcation::Left, biunion::Left>(
            bifurcation.as_mut(),
            biunion.as_ref(),
        )
        .unwrap();

        // Connect biunion's output to bifurcation's input (forward connection)
        // This uses SignalPolicy::Forward by default in make_bidi
        make_bidi(biunion, &mut bifurcation).unwrap();

        // Create a sink for the bifurcation's right output
        let sink = Sink::new::<bifurcation::Right>(bifurcation).unwrap();

        let mut reader = LineReader::new(source, sink);
        println!("=== Starting cycle test with value 1 ===");

        // Input value 1 to start the cycle
        reader.push(Message::Data(1)).unwrap();

        assert_eq!(reader.flush("hello".into()).unwrap(), vec![6]);

        println!("=== Test completed successfully ===");
    }
}

