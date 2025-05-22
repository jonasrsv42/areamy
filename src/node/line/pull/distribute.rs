use crate::{error::Error, fatal, Connection, Message, Pullable};
use crossbeam::channel::{self, Receiver};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Helper function to send with timeout and shutdown check
fn send_with_timeout_check<T>(
    sender: &crossbeam::channel::Sender<T>,
    mut message: T,
    running: &Arc<AtomicBool>,
) -> bool {
    const SEND_TIMEOUT: Duration = Duration::from_millis(100);

    while running.load(Ordering::Relaxed) {
        match sender.send_timeout(message, SEND_TIMEOUT) {
            Ok(()) => return true,
            Err(crossbeam::channel::SendTimeoutError::Timeout(msg)) => message = msg,
            Err(crossbeam::channel::SendTimeoutError::Disconnected(_)) => return false,
        }
    }
    false
}

/// Lock-free SPMC (Single Producer Multiple Consumer) distributor using crossbeam channels
/// Distributes messages from a single producer to multiple consumers in round-robin fashion
pub struct Distributor<PullableType>
where
    PullableType: Pullable + Send + 'static,
    PullableType::DataType: Clone + Send + 'static,
    PullableType::SignalType: Clone + Send + 'static,
{
    /// Receivers for each consumer (to be handed out once)
    consumer_receivers:
        Option<Vec<Receiver<Message<PullableType::DataType, PullableType::SignalType>>>>,
    /// Handle to the producer thread
    producer_handle: Option<thread::JoinHandle<Result<(), Error>>>,
    /// Running flag for graceful termination
    running: Arc<AtomicBool>,
}

impl<PullableType> Distributor<PullableType>
where
    PullableType: Pullable + Send + 'static,
    PullableType::DataType: Clone + Send + 'static,
    PullableType::SignalType: Clone + Send + 'static,
{
    /// Create a new distributor with specified number of consumers and buffer size per consumer
    pub fn new(
        mut producer: PullableType,
        consumer_count: usize,
        buffer_size_per_consumer: usize,
    ) -> Self {
        let mut consumer_senders = Vec::with_capacity(consumer_count);
        let mut consumer_receivers = Vec::with_capacity(consumer_count);

        // Create bounded channels for each consumer
        for _ in 0..consumer_count {
            let (tx, rx) = channel::bounded(buffer_size_per_consumer);
            consumer_senders.push(tx);
            consumer_receivers.push(rx);
        }

        let running = Arc::new(AtomicBool::new(true));
        let running_clone = Arc::clone(&running);

        // Start producer thread that pulls from source and distributes messages
        let producer_handle = thread::spawn(move || -> Result<(), Error> {
            let mut current_consumer = 0;

            while running_clone.load(Ordering::Relaxed) {
                let message = producer.pull()?;
                
                if !match &message {
                    Message::Data(_) => {
                        send_with_timeout_check(&consumer_senders[current_consumer], message, &running_clone)
                            .then(|| current_consumer = (current_consumer + 1) % consumer_count)
                            .is_some()
                    }
                    _ => consumer_senders.iter().all(|s| send_with_timeout_check(s, message.clone(), &running_clone))
                } { break; }
            }
            Ok(())
        });

        Self {
            consumer_receivers: Some(consumer_receivers),
            producer_handle: Some(producer_handle),
            running,
        }
    }

    /// Create consumer instances that can pull from the distributed stream
    /// This can only be called once as it consumes the receivers
    pub fn create_consumers(&mut self) -> Result<Vec<DistributedConsumer<PullableType>>, Error> {
        Ok(self.consumer_receivers
            .take()
            .ok_or_else(|| fatal!("Consumers already created"))?
            .into_iter()
            .map(|receiver| DistributedConsumer { receiver })
            .collect())
    }

    /// Gracefully shutdown the distributor
    /// Returns any error that occurred in the producer thread
    pub fn shutdown(&mut self) -> Result<(), Error> {
        // Use compare_exchange to ensure only one thread shuts down
        self.running
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
            .then(|| {
                self.producer_handle
                    .take()
                    .ok_or_else(|| fatal!("No producer handle"))?
                    .join()
                    .map_err(|_| fatal!("Producer thread panicked"))?
            })
            .unwrap_or(Ok(()))
    }
}

impl<PullableType> Drop for Distributor<PullableType>
where
    PullableType: Pullable + Send + 'static,
    PullableType::DataType: Clone + Send + 'static,
    PullableType::SignalType: Clone + Send + 'static,
{
    fn drop(&mut self) {
        // Gracefully shutdown when distributor is dropped (ignore errors in drop)
        let _ = self.shutdown();
    }
}

/// Individual consumer that receives messages from the distributor
#[derive(Debug)]
pub struct DistributedConsumer<PullableType>
where
    PullableType: Pullable,
{
    receiver: Receiver<Message<PullableType::DataType, PullableType::SignalType>>,
}

impl<PullableType> Connection for DistributedConsumer<PullableType> where PullableType: Pullable {}

impl<PullableType> Pullable for DistributedConsumer<PullableType>
where
    PullableType: Pullable,
{
    type ThreadId = PullableType::ThreadId;
    type DataType = PullableType::DataType;
    type SignalType = PullableType::SignalType;

    fn pull(&mut self) -> Result<Message<Self::DataType, Self::SignalType>, Error> {
        self.receiver
            .recv()
            .map_err(|_| fatal!("Producer disconnected"))
    }
}

/// Builder for creating distributors with a fluent API
pub struct DistributorBuilder {
    consumer_count: usize,
    buffer_size_per_consumer: usize,
}

impl Default for DistributorBuilder {
    fn default() -> Self {
        Self {
            consumer_count: 1,
            buffer_size_per_consumer: 100,
        }
    }
}

impl DistributorBuilder {
    /// Create a new builder with default settings
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the number of consumers
    pub fn consumer_count(mut self, count: usize) -> Self {
        self.consumer_count = count;
        self
    }

    /// Set the buffer size per consumer
    pub fn buffer_size(mut self, size: usize) -> Self {
        self.buffer_size_per_consumer = size;
        self
    }

    /// Build the distributor with the given producer
    pub fn build<PullableType>(
        self,
        producer: PullableType,
    ) -> (
        Distributor<PullableType>,
        Vec<DistributedConsumer<PullableType>>,
    )
    where
        PullableType: Pullable + Send + 'static,
        PullableType::DataType: Clone + Send + 'static,
        PullableType::SignalType: Clone + Send + 'static,
    {
        let mut distributor =
            Distributor::new(producer, self.consumer_count, self.buffer_size_per_consumer);
        let consumers = distributor
            .create_consumers()
            .expect("Failed to create consumers");
        (distributor, consumers)
    }
}

impl<PullableType> Distributor<PullableType>
where
    PullableType: Pullable,
    PullableType::DataType: Clone + Send + 'static,
    PullableType::SignalType: Clone + Send + 'static,
{
    /// Create a builder for this distributor
    pub fn builder() -> DistributorBuilder {
        DistributorBuilder::new()
    }
}

/// Convenience function to create a distributor and consumers
/// Returns (distributor, vector_of_consumers)
pub fn distribute<PullableType>(
    producer: PullableType,
    consumer_count: usize,
    buffer_size_per_consumer: usize,
) -> (
    Distributor<PullableType>,
    Vec<DistributedConsumer<PullableType>>,
)
where
    PullableType: Pullable + Send + 'static,
    PullableType::DataType: Clone + Send + 'static,
    PullableType::SignalType: Clone + Send + 'static,
{
    DistributorBuilder::new()
        .consumer_count(consumer_count)
        .buffer_size(buffer_size_per_consumer)
        .build(producer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DefaultThread, Message};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    // Simple test producer that generates incrementing numbers
    struct TestProducer {
        counter: Arc<AtomicUsize>,
        max_count: usize,
    }

    impl TestProducer {
        fn new(max_count: usize) -> Self {
            Self {
                counter: Arc::new(AtomicUsize::new(0)),
                max_count,
            }
        }
    }

    impl Connection for TestProducer {}

    impl Pullable for TestProducer {
        type ThreadId = DefaultThread;
        type DataType = usize;
        type SignalType = usize;

        fn pull(&mut self) -> Result<Message<Self::DataType, Self::SignalType>, Error> {
            let current = self.counter.fetch_add(1, Ordering::SeqCst);
            if current < self.max_count {
                Ok(Message::Data(current))
            } else {
                Err(fatal!("Producer exhausted"))
            }
        }
    }

    #[test]
    fn test_basic_distribution() {
        let producer = TestProducer::new(10);
        let (_distributor, mut consumers) = distribute(producer, 3, 100);

        // Give producer thread time to generate messages
        thread::sleep(Duration::from_millis(100));

        // Collect all messages from all consumers
        let mut received_values: Vec<usize> = consumers
            .iter_mut()
            .flat_map(|consumer| {
                std::iter::from_fn(|| match consumer.pull() {
                    Ok(Message::Data(value)) => Some(value),
                    _ => None,
                })
            })
            .collect();
        let total_received = received_values.len();

        // Should have received all 10 messages
        assert_eq!(total_received, 10);

        // Should have received values 0-9
        received_values.sort();
        assert_eq!(received_values, (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn test_round_robin_distribution() {
        let producer = TestProducer::new(6); // 6 messages for 3 consumers = 2 each
        let (_distributor, mut consumers) = distribute(producer, 3, 100);

        // Give producer thread time to generate messages
        thread::sleep(Duration::from_millis(100));

        // Each consumer should get exactly 2 messages in round-robin fashion
        for (i, consumer) in consumers.iter_mut().enumerate() {
            let mut count = 0;
            let mut values = Vec::new();

            loop {
                match consumer.pull() {
                    Ok(Message::Data(value)) => {
                        values.push(value);
                        count += 1;
                    }
                    _ => break,
                }
            }

            assert_eq!(count, 2, "Consumer {} should receive exactly 2 messages", i);

            // Values should be i, i+3 (round-robin pattern)
            assert_eq!(values.len(), 2);
            assert_eq!(values[0], i);
            assert_eq!(values[1], i + 3);
        }
    }

    #[test]
    fn test_empty_consumer() {
        let producer = TestProducer::new(0); // No messages
        let (_distributor, mut consumers) = distribute(producer, 2, 100);

        // Give producer thread time
        thread::sleep(Duration::from_millis(50));

        // All consumers should be empty
        for consumer in &mut consumers {
            match consumer.pull() {
                Err(ref e)
                    if e.to_string().contains("No data available")
                        || e.to_string().contains("Producer disconnected") => {}
                other => panic!("Expected error, got: {:?}", other),
            }
        }
    }

    #[test]
    fn test_consumer_creation_only_once() {
        let producer = TestProducer::new(5);
        let mut distributor = Distributor::new(producer, 2, 100);

        // First call should succeed
        let consumers1 = distributor.create_consumers();
        assert!(consumers1.is_ok());
        assert_eq!(consumers1.unwrap().len(), 2);

        // Second call should fail
        let consumers2 = distributor.create_consumers();
        assert!(consumers2.is_err());
        if let Err(e) = consumers2 {
            assert!(e.to_string().contains("already created"));
        }
    }

    //#[test]
    //fn test_bounded_buffer() {
    //    // Create a producer that generates many messages
    //    let producer = TestProducer::new(1000);

    //    // Use very small buffer to test backpressure
    //    let (_distributor, mut consumers) = distribute(producer, 2, 5);

    //    // Don't consume immediately - let buffer fill up
    //    thread::sleep(Duration::from_millis(50));

    //    // Now start consuming what's buffered
    //    let mut total_consumed = 0;
    //    for consumer in &mut consumers {
    //        loop {
    //            match consumer.pull() {
    //                Ok(Message::Data(_)) => total_consumed += 1,
    //                _ => break,
    //            }
    //        }
    //    }

    //    // Should have consumed some messages (at least what was buffered)
    //    // With 2 consumers * 5 buffer each = 10 max buffered messages
    //    assert!(total_consumed > 0);
    //    assert!(total_consumed <= 10, "Expected ≤10 messages, got {}", total_consumed);
    //}

    #[test]
    fn test_builder_pattern() {
        let producer = TestProducer::new(5);

        // Use builder pattern
        let (_distributor, mut consumers) = DistributorBuilder::new()
            .consumer_count(3)
            .buffer_size(50)
            .build(producer);

        // Give producer thread time
        thread::sleep(Duration::from_millis(100));

        // Should have 3 consumers
        assert_eq!(consumers.len(), 3);

        let mut total_received = 0;
        for consumer in &mut consumers {
            loop {
                match consumer.pull() {
                    Ok(Message::Data(_)) => total_received += 1,
                    _ => break,
                }
            }
        }

        assert_eq!(total_received, 5);
    }

    #[test]
    fn test_graceful_shutdown() {
        let producer = TestProducer::new(10); // Finite messages
        let (mut distributor, _consumers) = distribute(producer, 2, 100);

        // Let it run until completion
        thread::sleep(Duration::from_millis(100));

        // Shutdown should return producer's final result
        let result = distributor.shutdown();
        assert!(result.is_err()); // Producer exhausted
    }

    #[test]
    fn test_signal_broadcasting() {
        // Test producer that generates both data and signals
        struct SignalProducer {
            counter: Arc<AtomicUsize>,
        }

        impl SignalProducer {
            fn new() -> Self {
                Self {
                    counter: Arc::new(AtomicUsize::new(0)),
                }
            }
        }

        impl Connection for SignalProducer {}

        impl Pullable for SignalProducer {
            type ThreadId = DefaultThread;
            type DataType = usize;
            type SignalType = usize;

            fn pull(&mut self) -> Result<Message<Self::DataType, Self::SignalType>, Error> {
                let current = self.counter.fetch_add(1, Ordering::SeqCst);
                match current {
                    0 => Ok(Message::Data(1)),     // Data message
                    1 => Ok(Message::Flush(100)),  // Flush signal
                    2 => Ok(Message::Data(2)),     // Data message
                    3 => Ok(Message::Marker(200)), // Marker signal
                    _ => Err(fatal!("Producer exhausted")),
                }
            }
        }

        let producer = SignalProducer::new();
        let (_distributor, mut consumers) = distribute(producer, 3, 100);

        // Give producer thread time
        thread::sleep(Duration::from_millis(100));

        let mut consumer_messages: Vec<Vec<Message<usize, usize>>> = vec![Vec::new(); 3];

        // Collect all messages from all consumers
        for (i, consumer) in consumers.iter_mut().enumerate() {
            loop {
                match consumer.pull() {
                    Ok(message) => consumer_messages[i].push(message),
                    _ => break,
                }
            }
        }

        // Check distribution:
        // Data messages should be round-robin: consumer 0 gets Data(1), consumer 1 gets Data(2)
        // Signal messages should be broadcast: all consumers get Flush(100) and Marker(200)

        // Consumer 0: Data(1), Flush(100), Marker(200)
        assert_eq!(consumer_messages[0].len(), 3);
        assert!(consumer_messages[0].contains(&Message::Data(1)));
        assert!(consumer_messages[0].contains(&Message::Flush(100)));
        assert!(consumer_messages[0].contains(&Message::Marker(200)));

        // Consumer 1: Data(2), Flush(100), Marker(200)
        assert_eq!(consumer_messages[1].len(), 3);
        assert!(consumer_messages[1].contains(&Message::Data(2)));
        assert!(consumer_messages[1].contains(&Message::Flush(100)));
        assert!(consumer_messages[1].contains(&Message::Marker(200)));

        // Consumer 2: Flush(100), Marker(200) (no data messages)
        assert_eq!(consumer_messages[2].len(), 2);
        assert!(consumer_messages[2].contains(&Message::Flush(100)));
        assert!(consumer_messages[2].contains(&Message::Marker(200)));
    }
}
