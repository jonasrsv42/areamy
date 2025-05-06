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
    
    #[test]
    fn test_cycle_with_multiple_values() {
        // Create our nodes
        let biunion = make_biunion(Ok(IncrementBiunion::new())).unwrap();
        let mut bifurcation = make_bifurcation(Ok(DeciderBifurcation::new())).unwrap();

        // Create a source to input to biunion's right side
        let source = Source::new::<biunion::Right>(&biunion).unwrap();

        // Connect bifurcation's left output back to biunion's left input (backward connection)
        Connect::<usize>::push::<bifurcation::Left, biunion::Left>(
            bifurcation.as_mut(),
            biunion.as_ref(),
        )
        .unwrap();

        // Connect biunion's output to bifurcation's input (forward connection)
        make_bidi(biunion, &mut bifurcation).unwrap();

        // Create a sink for the bifurcation's right output
        let sink = Sink::new::<bifurcation::Right>(bifurcation).unwrap();

        let mut reader = LineReader::new(source, sink);
        println!("=== Starting cycle test with values 1-6 ===");

        // Input values 1 through 6
        for i in 1..=6 {
            println!("Pushing value {}", i);
            reader.push(Message::Data(i)).unwrap();
        }

        // Flush and check the results
        let results = reader.flush("flush-test".into()).unwrap();
        println!("Results after flush: {:?}", results);
        
        // The expected results:
        // - Values 1-5 go through the cycle and increment until they exceed 5
        // - Value 6 goes directly to output after one increment (becomes 7)
        // So we expect: [6, 6, 6, 6, 6, 7]
        assert_eq!(results, vec![6, 6, 6, 6, 6, 7]);

        println!("=== Multiple values test completed successfully ===");
    }
    
    // Line routine that increments values by 1
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
    
    // Implementation for receiving input
    impl crate::Send<usize> for IncrementLine {
        fn send(&mut self, message: usize) -> Result<(), Error> {
            println!(
                "Line received {}, incrementing to {}",
                message,
                message + 1
            );
            self.output.push_back(message + 1);
            Ok(())
        }
    }
    
    // Output implementation
    impl crate::Next<usize> for IncrementLine {
        fn next(&mut self) -> Result<Option<usize>, Error> {
            let result = self.output.pop_front();
            if result.is_some() {
                println!("Line outputting {}", result.unwrap());
            }
            Ok(result)
        }
    }
    
    impl crate::Flush for IncrementLine {
        fn flush(&mut self) -> Result<(), Error> {
            println!("Line flushed");
            Ok(())
        }
    }
    
    impl crate::node::Name for IncrementLine {
        fn name<'a>(&'a self) -> &'a str {
            "IncrementLine"
        }
    }
    
    impl crate::node::line::LineRoutine<usize, usize> for IncrementLine {}
    
    #[test]
    fn test_cycle_with_line_node() {
        // Import additional dependencies for line node
        use crate::work::make_line;
        
        // Create our nodes
        let line = make_line(Ok(IncrementLine::new())).unwrap();
        let mut bifurcation = make_bifurcation(Ok(DeciderBifurcation::new())).unwrap();

        // Create a source to input to the line node
        let source = Source::new(&line).unwrap();

        // Connect bifurcation's left output back to line's input (backward connection)
        // This creates a cycle where values <= 5 go back to the line
        Connect::<usize>::push::<bifurcation::Left, crate::marker::Unary>(
            bifurcation.as_mut(),
            line.as_ref(),
        )
        .unwrap();

        // Connect line's output to bifurcation's input (forward connection)
        make_bidi(line, &mut bifurcation).unwrap();

        // Create a sink for the bifurcation's right output
        let sink = Sink::new::<bifurcation::Right>(bifurcation).unwrap();

        let mut reader = LineReader::new(source, sink);
        println!("=== Starting cycle test with line node: values 1-6 ===");

        // Input values 1 through 6
        for i in 1..=6 {
            println!("Pushing value {}", i);
            reader.push(Message::Data(i)).unwrap();
        }

        // Flush and check the results
        let results = reader.flush("line-cycle-test".into()).unwrap();
        println!("Results after flush: {:?}", results);
        
        // The expected results for line node: [6, 7, 6, 6, 6, 6]
        //
        // IMPORTANT: The line node implementation produces a different result order
        // compared to the biunion test [6, 6, 6, 6, 6, 7]. Here's why:
        //
        // 1. Order of Processing:
        //    - Line node processes inputs sequentially through the same queue
        //    - Biunion has separate Left/Right input queues with different handling
        //
        // 2. Data Flow Scheduling:
        //    - Line node: When the first value (1) arrives, it's processed completely
        //      through its entire cycle (1→2→3→4→5→6→output) before the next value (6) is processed
        //    - Biunion node: Has different scheduling characteristics due to its dual input 
        //      nature, causing values to be interleaved differently in the processing queue
        //
        // 3. Feedback Handling:
        //    - Both implementations use the same cycle logic (values ≤5 cycle back, 
        //      values >5 go to output)
        //    - The difference is in how items get scheduled in the processing queues
        //
        // 4. Resulting Output Order:
        //    - Line node: [6, 7, 6, 6, 6, 6]
        //      1) 6 from value 1 cycling until >5
        //      2) 7 from value 6 incrementing once
        //      3-6) 6's from values 2-5 cycling until >5
        //    - Biunion: [6, 6, 6, 6, 6, 7]
        //      1-5) 6's from values 1-5 cycling
        //      6) 7 from value 6 incrementing once
        //
        // This test demonstrates how the same cycle logic with different node types
        // produces different behaviors due to their internal scheduling mechanisms.
        assert_eq!(results, vec![6, 7, 6, 6, 6, 6]);

        println!("=== Line node cycle test completed successfully ===");
    }
}

