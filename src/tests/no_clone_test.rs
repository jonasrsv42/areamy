//! Test to demonstrate that non-cloneable objects (Box<usize>) can be passed through a pipeline of pullable nodes.
//!
//! This test creates a chain of three BoxIncrementer nodes:
//! - Each node receives a Box<usize>, increments the value inside, and forwards it
//! - We pass Box<1> through the pipeline and expect Box<4> at the end
//! - Box<T> is not Clone, so this demonstrates the ability to use non-cloneable objects in the pipeline

use crate::error::Error;
use crate::node::line::LineRoutine;
use crate::pull::{make_pull, Root};
use crate::pull::Sink;
use crate::work::Source;
use crate::{DefaultThread, Message, Next, Send}; // Remove unused Origin import
use crate::connect::graph::{Pushable, Pullable}; // Add Pushable and Pullable traits
use crate::signal::Trackable; // Add Trackable for signal types
use std::collections::VecDeque;

// Custom LineRoutine that increments the value in a Box<usize>
struct BoxIncrementer {
    output: VecDeque<Box<usize>>,
}

impl BoxIncrementer {
    fn new() -> Result<Self, Error> {
        Ok(Self {
            output: VecDeque::new(),
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_non_cloneable_pipeline() {
        // Create a chain of three BoxIncrementer nodes
        // Box<1> -> BoxIncrementer1 -> Box<2> -> BoxIncrementer2 -> Box<3> -> BoxIncrementer3 -> Box<4>

        // Create the root node with the correct signal type
        let root = Root::<Box<usize>, Trackable<&'static str>, DefaultThread>::new();
        let mut source = Source::of(&root).unwrap();

        // Create three BoxIncrementer nodes in a chain
        let line1 = make_pull(root, BoxIncrementer::new()).unwrap();
        let line2 = make_pull(line1, BoxIncrementer::new()).unwrap();
        let line3 = make_pull(line2, BoxIncrementer::new()).unwrap();

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

        // The value should have been incremented three times
        match result {
            Message::Data(boxed_result) => {
                assert_eq!(
                    *boxed_result, 4,
                    "Value should be incremented three times (1→2→3→4)"
                );
            }
            _ => panic!("Expected Message::Data but got {}", format_message(&result)),
        }

        // Try with a flush message
        source.push(Message::Flush("flush-test".into())).unwrap();
        let flush_result = Pullable::pull(&mut sink).unwrap();
        
        // Check if it's a flush message (can't directly compare the values)
        match flush_result {
            Message::Flush(_) => println!("Successfully received flush message"),
            _ => panic!("Expected Message::Flush but got {}", format_message(&flush_result)),
        }

        // Push another value to see if the pipeline was reset properly
        let second_input = Box::new(10);
        println!("Pushing second input value: Box({})", *second_input);
        source.push(Message::Data(second_input)).unwrap();

        let second_result = Pullable::pull(&mut sink).unwrap();
        println!("Second result from pipeline: {}", format_message(&second_result));

        // The second value should also be incremented three times
        match second_result {
            Message::Data(boxed_result) => {
                assert_eq!(
                    *boxed_result, 13,
                    "Value should be incremented three times (10→11→12→13)"
                );
            }
            _ => panic!("Expected Message::Data but got {}", format_message(&second_result)),
        }

        println!("=== Non-cloneable pipeline test completed successfully ===");
    }
}

