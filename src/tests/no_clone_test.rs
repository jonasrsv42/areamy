//! Test to demonstrate that non-cloneable objects (Box<usize>) can be passed through a pipeline of pullable nodes and pollables nodes.
//!
//! This test creates a chain of three BoxIncrementer nodes:
//! - Each node receives a Box<usize>, increments the value inside, and forwards it
//! - We pass Box<1> through the pipeline and expect Box<4> at the end
//! - Box<T> is not Clone, so this demonstrates the ability to use non-cloneable objects in the pipeline

use crate::Origin;
use crate::connect::graph::{Pullable, Pushable}; // Add Pushable and Pullable traits
use crate::error::Error;
use crate::node::line::LineRoutine;
use crate::pull::Sink;
use crate::pull::{SourceBuffer, make_pull};
use crate::signal::Trackable; // Add Trackable for signal types
use crate::work::Source;
use crate::{DefaultThread, Message, Next, Send}; // Remove unused Origin import
use std::collections::VecDeque;

// Custom LineRoutine that increments the value in a Box<usize>
struct BoxIncrementer {
    output: VecDeque<Box<usize>>,
}

impl BoxIncrementer {
    fn new() -> Self {
        Self {
            output: VecDeque::new(),
        }
    }
}

// Implementation for receiving input
impl Send<Box<usize>> for BoxIncrementer {
    fn send(&mut self, message: Box<usize>) -> Result<(), Error> {
        // Increment the value inside the box
        let incremented_value = Box::new(*message + 1);
        println!(
            "BoxIncrementer: Incrementing {} to {}",
            *message, *incremented_value
        );
        self.output.push_back(incremented_value);
        Ok(())
    }
}

// Output implementation
impl Next<Box<usize>> for BoxIncrementer {
    fn next(&mut self) -> Result<Option<Box<usize>>, Error> {
        let result = self.output.pop_front();
        if let Some(ref value) = result {
            println!("BoxIncrementer: Outputting {}", **value);
        }
        Ok(result)
    }
}

impl crate::Flush for BoxIncrementer {
    fn flush(&mut self) -> Result<(), Error> {
        println!("BoxIncrementer: Flushed");
        Ok(())
    }
}

impl crate::node::Name for BoxIncrementer {
    fn name<'a>(&'a self) -> &'a str {
        "BoxIncrementer"
    }
}

// Implement LineRoutine for BoxIncrementer
impl LineRoutine<Box<usize>, Box<usize>> for BoxIncrementer {}

// Helper function to format messages in test output rather than implementing Debug
// This avoids conflicting with the derive(Debug) implementation
fn format_message(msg: &Message<Box<usize>, Trackable<&'static str>>) -> String {
    match msg {
        Message::Data(boxed) => format!("Message::Data(Box({}))", **boxed),
        Message::Flush(s) => format!("Message::Flush({:?})", s),
        Message::Marker(s) => format!("Message::Marker({:?})", s),
    }
}

// Helper function to format messages with boxed signals
fn format_boxed_message(msg: &Message<Box<usize>, BoxedOrigin>) -> String {
    match msg {
        Message::Data(boxed) => format!("Message::Data(Box({}))", **boxed),
        Message::Flush(signal) => format!("Message::Flush(Box({}))", *signal.0),
        Message::Marker(signal) => format!("Message::Marker(Box({}))", *signal.0),
    }
}

// This struct implements Origin for Box<usize> so it can be used as a signal type
#[derive(Debug, PartialEq, Eq)]
struct BoxedOrigin(Box<usize>);

// Origin is a marker trait with Debug + Eq + Sync + Send bounds
impl Origin for BoxedOrigin {}

impl From<Box<usize>> for BoxedOrigin {
    fn from(boxed: Box<usize>) -> Self {
        BoxedOrigin(boxed)
    }
}

#[test]
fn test_non_cloneable_pull_pipeline() {
    // Create a chain of three BoxIncrementer nodes
    // Box<1> -> BoxIncrementer1 -> Box<2> -> BoxIncrementer2 -> Box<3> -> BoxIncrementer3 -> Box<4>

    // Create the source buffer with the correct signal type
    let buffer = SourceBuffer::<Box<usize>, Trackable<&'static str>, DefaultThread>::new();
    let mut source = Source::of(&buffer).unwrap();

    // Create three BoxIncrementer nodes in a chain
    let line1 = make_pull(buffer, BoxIncrementer::new());
    let line2 = make_pull(line1, BoxIncrementer::new());
    let line3 = make_pull(line2, BoxIncrementer::new());

    // Create a sink to read the output
    let mut sink = Sink::new(line3);

    println!("=== Starting non-cloneable pipeline test ===");

    // Push a boxed value to start the pipeline
    let input_value = Box::new(1);
    println!("Pushing input value: Box({})", *input_value);
    source.push(Message::Data(input_value)).unwrap();

    // Pull the result from the pipeline using the Pullable trait
    let result = Pullable::pull(&mut sink).unwrap();
    println!("Result from pipeline: {}", format_message(&result));

    assert!(matches!(result.data().map(|b| *b), Some(4)));
    // The value should have been incremented three times

    println!("=== Non-cloneable pipeline test completed successfully ===");
}

#[test]
fn test_non_cloneable_pull_signal() {
    // Create a source buffer using our BoxedOrigin type as the signal type
    let buffer = SourceBuffer::<Box<usize>, BoxedOrigin, DefaultThread>::new();
    let mut source = Source::of(&buffer).unwrap();

    // Create a pullable pipeline
    let line1 = make_pull(buffer, BoxIncrementer::new());
    let line2 = make_pull(line1, BoxIncrementer::new());

    // Create a sink to read the output
    let mut sink = Sink::new(line2);

    println!("=== Starting non-cloneable signal test ===");

    // Push a boxed value as data
    let input_value = Box::new(42);
    println!("Pushing data: Box({})", *input_value);
    source.push(Message::Data(input_value)).unwrap();

    // Pull the result
    let result = Pullable::pull(&mut sink).unwrap();
    println!("Result from pipeline: {}", format_boxed_message(&result));

    assert!(matches!(result.data().map(|b| *b), Some(44)));
}
//}
