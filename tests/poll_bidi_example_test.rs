//! Bidi streaming integration test using [areamy::poll::FutureRoutine].
//!
//! Demonstrates async connect → concurrent writer + reader via
//! [areamy::poll::Join], with flush triggering half-close and
//! reconnect per segment.

use areamy::error::Error;
use areamy::poll;
use areamy::poll::Join;
use areamy::poll::future::line::FutureRoutine;
use areamy::poll::future::queue::{Input, InputConsumer, OutputProducer};
use areamy::{
    Closeable, Message, Pushable, SyncEdge, ThreadBundle, ThreadId, ThreadStream, make_push,
    make_work,
};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;

#[derive(Debug)]
struct IoThread;
impl ThreadId for IoThread {}

// ============================================================
// Fake I/O primitives
// ============================================================

/// Simulates a real I/O operation: first poll returns Pending and fires
/// the waker (like OS registering interest), second poll returns Ready.
struct ImmediateFut<T> {
    value: Option<T>,
    polled_once: bool,
}

impl<T> ImmediateFut<T> {
    fn new(value: T) -> Self {
        Self {
            value: Some(value),
            polled_once: false,
        }
    }
}

impl<T: Unpin> Future for ImmediateFut<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> core::task::Poll<T> {
        if !self.polled_once {
            self.polled_once = true;
            cx.waker().wake_by_ref();
            core::task::Poll::Pending
        } else {
            core::task::Poll::Ready(self.value.take().unwrap())
        }
    }
}

/// Fake socket with async API mimicking real network I/O.
#[derive(Clone)]
struct FakeSocket {
    buffer: Rc<RefCell<VecDeque<usize>>>,
    closed: Rc<RefCell<bool>>,
}

impl FakeSocket {
    fn connect(_addr: &str) -> ImmediateFut<Self> {
        ImmediateFut::new(Self {
            buffer: Rc::new(RefCell::new(VecDeque::new())),
            closed: Rc::new(RefCell::new(false)),
        })
    }

    fn write(&self, val: usize) -> ImmediateFut<()> {
        self.buffer.borrow_mut().push_back(val);
        ImmediateFut::new(())
    }

    fn read(&self) -> SocketReadFut {
        SocketReadFut {
            socket: self.clone(),
            polled_once: false,
        }
    }

    fn half_close(&self) -> ImmediateFut<()> {
        *self.closed.borrow_mut() = true;
        ImmediateFut::new(())
    }
}

struct SocketReadFut {
    socket: FakeSocket,
    polled_once: bool,
}

impl Future for SocketReadFut {
    type Output = Option<usize>;

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Option<usize>> {
        if !self.polled_once {
            self.polled_once = true;
            cx.waker().wake_by_ref();
            return core::task::Poll::Pending;
        }

        match self.socket.buffer.borrow_mut().pop_front() {
            Some(item) => core::task::Poll::Ready(Some(item)),
            None if *self.socket.closed.borrow() => core::task::Poll::Ready(None),
            None => core::task::Poll::Pending,
        }
    }
}

// ============================================================
// Sync Double
// ============================================================

struct Double {
    output: VecDeque<usize>,
}

impl Double {
    fn new() -> Self {
        Self {
            output: VecDeque::new(),
        }
    }
}

impl areamy::Send<usize> for Double {
    fn send(&mut self, message: usize) -> Result<(), Error> {
        self.output.push_back(message * 2);
        Ok(())
    }
}

impl areamy::Next<usize> for Double {
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.output.pop_front())
    }
}

impl areamy::Flush for Double {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl areamy::node::Name for Double {}
impl areamy::LineRoutine<usize, usize> for Double {}

// ============================================================
// Test
// ============================================================

/// Bidi streaming: connect → writer + reader via Join → half-close on flush.
/// Uses areamy::poll::FutureRoutine with FutureRoutine::factory().
/// Multi-segment: flush resets the future, reconnects.
#[test]
fn bidi_with_join() -> Result<(), Error> {
    let mut source_node = areamy::work::make_line(Double::new());
    let mut source = areamy::work::Source::<usize>::of(&source_node)?;

    let mut async_thread = poll::Thread::<IoThread>::new();

    let routine = FutureRoutine::factory(
        |input: InputConsumer<usize>, output: OutputProducer<usize>| {
            Box::pin(async move {
                let socket = FakeSocket::connect("fake://server").await;

                let writer_input = input.clone();
                let writer_socket = socket.clone();
                let writer: Pin<Box<dyn Future<Output = Result<(), Error>>>> =
                    Box::pin(async move {
                        loop {
                            match writer_input.recv().await? {
                                Input::Data(val) => writer_socket.write(val * 3).await,
                                Input::Flush => {
                                    writer_socket.half_close().await;
                                    break;
                                }
                            }
                        }
                        Ok(())
                    });

                let reader_socket = socket;
                let reader_output = output.clone();
                let reader: Pin<Box<dyn Future<Output = Result<(), Error>>>> =
                    Box::pin(async move {
                        while let Some(val) = reader_socket.read().await {
                            reader_output.push(val + 1);
                        }
                        Ok(())
                    });

                Join::join([writer, reader]).await
            })
        },
    );

    let mut node = async_thread
        .line(routine)
        .input::<areamy::poll::Sync>()
        .output::<areamy::poll::Sync>();
    make_push(&mut source_node, &node)?;

    let output = Arc::new(SyncEdge::new());
    make_push(&mut node, &output)?;

    async_thread.add(node);

    let mut sync_thread = ThreadStream::<areamy::DefaultThread>::new();
    make_work(source_node, &mut sync_thread)?;

    let mut bundle = ThreadBundle::new();
    bundle.add(sync_thread).add(async_thread);
    let handle = bundle.start();

    // 5 → Double → 10 → writer(×3) → 30 → reader(+1) → 31
    source.push(Message::Data(5))?;
    source.push(Message::Data(2))?;
    source.push(Message::Flush("s1".into()))?;

    assert_eq!(output.read_front()?, Message::Data(31));
    assert_eq!(output.read_front()?, Message::Data(13)); // 2→4→12→13
    assert_eq!(output.read_front()?, Message::Flush("s1".into()));

    // Second segment — future recreated, new socket
    source.push(Message::Data(1))?;
    source.push(Message::Flush("s2".into()))?;

    assert_eq!(output.read_front()?, Message::Data(7)); // 1→2→6→7
    assert_eq!(output.read_front()?, Message::Flush("s2".into()));

    source.close()?;
    let errors = handle.join()?;
    assert!(errors.is_empty());
    Ok(())
}
