//! Test to demonstrate that non-cloneable objects (Box<usize>) can be passed through a pipeline of pullable nodes.
//!
//! This test creates a chain of three BoxIncrementer nodes:
//! - Each node receives a Box<usize>, increments the value inside, and forwards it
//! - We pass Box<1> through the pipeline and expect Box<4> at the end
//! - Box<T> is not Clone, so this demonstrates the ability to use non-cloneable objects in the pipeline

use crate::connect::graph::{Pullable, Pushable}; // Add Pushable and Pullable traits
use crate::error::Error;
use crate::node::line::LineRoutine;
use crate::pull::Sink;
use crate::pull::{Root, make_pull};
use crate::signal::Trackable; // Add Trackable for signal types
use crate::work::Source;
use crate::{DefaultThread, Message, Next, Send}; // Remove unused Origin import
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
    use crate::Origin;

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
            _ => panic!(
                "Expected Message::Flush but got {}",
                format_message(&flush_result)
            ),
        }

        // Push another value to see if the pipeline was reset properly
        let second_input = Box::new(10);
        println!("Pushing second input value: Box({})", *second_input);
        source.push(Message::Data(second_input)).unwrap();

        let second_result = Pullable::pull(&mut sink).unwrap();
        println!(
            "Second result from pipeline: {}",
            format_message(&second_result)
        );

        // The second value should also be incremented three times
        match second_result {
            Message::Data(boxed_result) => {
                assert_eq!(
                    *boxed_result, 13,
                    "Value should be incremented three times (10→11→12→13)"
                );
            }
            _ => panic!(
                "Expected Message::Data but got {}",
                format_message(&second_result)
            ),
        }

        println!("=== Non-cloneable pipeline test completed successfully ===");
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

    // Helper function to format messages with boxed signals
    fn format_boxed_message(msg: &Message<Box<usize>, BoxedOrigin>) -> String {
        match msg {
            Message::Data(boxed) => format!("Message::Data(Box({}))", **boxed),
            Message::Flush(signal) => format!("Message::Flush(Box({}))", *signal.0),
            Message::Marker(signal) => format!("Message::Marker(Box({}))", *signal.0),
        }
    }

    #[test]
    fn test_non_cloneable_signal() {
        // Create a root node using our BoxedOrigin type as the signal type
        let root = Root::<Box<usize>, BoxedOrigin, DefaultThread>::new();
        let mut source = Source::of(&root).unwrap();

        // Create a pullable pipeline
        let line1 = make_pull(root, BoxIncrementer::new()).unwrap();
        let line2 = make_pull(line1, BoxIncrementer::new()).unwrap();

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

        // The value should have been incremented twice
        match result {
            Message::Data(boxed_result) => {
                assert_eq!(
                    *boxed_result, 44,
                    "Value should be incremented twice (42→43→44)"
                );
            }
            _ => panic!(
                "Expected Message::Data but got {}",
                format_boxed_message(&result)
            ),
        }

        // Now push a non-cloneable signal (Box<usize> wrapped in BoxedOrigin)
        let boxed_signal = Box::new(100);
        println!("Pushing flush with boxed signal: Box({})", *boxed_signal);
        source
            .push(Message::Flush(BoxedOrigin(boxed_signal)))
            .unwrap();

        // Pull the result with the non-cloneable signal
        let signal_result = Pullable::pull(&mut sink).unwrap();
        println!(
            "Signal result from pipeline: {}",
            format_boxed_message(&signal_result)
        );

        // Verify we received a flush with our boxed signal
        match signal_result {
            Message::Flush(signal) => {
                assert_eq!(*signal.0, 100, "Signal value should be preserved");
                println!(
                    "Successfully received flush with boxed signal: Box({})",
                    *signal.0
                );
            }
            _ => panic!(
                "Expected Message::Flush but got {}",
                format_boxed_message(&signal_result)
            ),
        }

        // Now try a marker with a boxed signal
        let boxed_marker = Box::new(200);
        println!("Pushing marker with boxed signal: Box({})", *boxed_marker);
        source
            .push(Message::Marker(BoxedOrigin(boxed_marker)))
            .unwrap();

        // Pull the marker result
        let marker_result = Pullable::pull(&mut sink).unwrap();
        println!(
            "Marker result from pipeline: {}",
            format_boxed_message(&marker_result)
        );

        // Verify we received a marker with our boxed signal
        match marker_result {
            Message::Marker(signal) => {
                assert_eq!(*signal.0, 200, "Marker signal value should be preserved");
                println!(
                    "Successfully received marker with boxed signal: Box({})",
                    *signal.0
                );
            }
            _ => panic!(
                "Expected Message::Marker but got {}",
                format_boxed_message(&marker_result)
            ),
        }

        println!("=== Non-cloneable signal test completed successfully ===");
    }
}
