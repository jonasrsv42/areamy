//! Proof-of-concept: bidi streaming routines using async/await.
//!
//! Two patterns:
//!
//! 1. **SingleFnRoutine** — user provides one async fn. Handles socket setup,
//!    reader, writer, everything. Framework just wraps the future.
//!
//! 2. **BidiRoutine** — framework manages socket lifecycle. Connects on first
//!    poll, passes socket to user-provided writer + reader fns.
//!    Handles Running → HalfFlush → Flushed → reconnect.

use areamy::error::Error;
use areamy::node::Name;
use areamy::{
    AsyncThread, Closeable, Message, Pushable, SyncEdge, ThreadBundle, ThreadId, ThreadStream,
    make_push, make_work,
};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
struct IoThread;
impl ThreadId for IoThread {}

// ============================================================
// Async primitives
// ============================================================

struct AsyncQueue<T>(Arc<Mutex<VecDeque<T>>>);

impl<T> Clone for AsyncQueue<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> AsyncQueue<T> {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(VecDeque::new())))
    }

    fn push(&self, item: T) {
        self.0.lock().unwrap().push_back(item);
    }

    fn pop(&self) -> Option<T> {
        self.0.lock().unwrap().pop_front()
    }
}

struct RecvFut<T>(AsyncQueue<T>);

impl<T: Unpin> Future for RecvFut<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, _cx: &mut core::task::Context<'_>) -> core::task::Poll<T> {
        match self.0.pop() {
            Some(item) => core::task::Poll::Ready(item),
            None => core::task::Poll::Pending,
        }
    }
}

/// Simulates a real I/O operation: first poll returns Pending and fires
/// the waker (like OS registering interest), second poll returns Ready
/// with the value. Reusable for any fake async I/O.
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
            // First poll: register interest, fire waker (like OS/epoll would)
            self.polled_once = true;
            cx.waker().wake_by_ref();
            core::task::Poll::Pending
        } else {
            // Second poll: I/O complete
            core::task::Poll::Ready(self.value.take().unwrap())
        }
    }
}

/// Fake socket with async API mimicking real network I/O.
/// Each operation takes one Pending + waker round-trip via ImmediateFut.
struct FakeSocket {
    buffer: AsyncQueue<usize>,
    closed: Arc<Mutex<bool>>,
}

impl Clone for FakeSocket {
    fn clone(&self) -> Self {
        Self {
            buffer: self.buffer.clone(),
            closed: self.closed.clone(),
        }
    }
}

impl FakeSocket {
    fn connect(_addr: &str) -> ImmediateFut<Self> {
        ImmediateFut::new(Self {
            buffer: AsyncQueue::new(),
            closed: Arc::new(Mutex::new(false)),
        })
    }

    fn write(&self, val: usize) -> ImmediateFut<()> {
        self.buffer.push(val);
        ImmediateFut::new(())
    }

    fn read(&self) -> SocketReadFut {
        SocketReadFut {
            socket: self.clone(),
            polled_once: false,
        }
    }

    fn half_close(&self) -> ImmediateFut<()> {
        *self.closed.lock().unwrap() = true;
        ImmediateFut::new(())
    }
}

/// Read future: first poll returns Pending + fires waker.
/// Second poll checks buffer: Some(data) or None if closed.
/// If buffer empty and not closed, stays Pending (real I/O: waiting for data).
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

        match self.socket.buffer.pop() {
            Some(item) => core::task::Poll::Ready(Some(item)),
            None if *self.socket.closed.lock().unwrap() => core::task::Poll::Ready(None),
            None => core::task::Poll::Pending, // waiting for data from network
        }
    }
}

use areamy::poll::Join;

enum InputItem {
    Data(usize),
    Flush,
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

impl Name for Double {}
impl areamy::LineRoutine<usize, usize> for Double {}

// ============================================================
// Pattern 1: SingleFnRoutine — user handles everything
// ============================================================

/// User provides one async fn. Framework wraps it and manages reset.
/// When the future completes → Ready. Next poll → factory creates new future.
struct SingleFnRoutine<F>
where
    F: Fn(AsyncQueue<InputItem>, AsyncQueue<usize>) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>
        + Send,
{
    input: AsyncQueue<InputItem>,
    output: AsyncQueue<usize>,
    factory: F,
    future: Option<Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>>,
}

impl<F> SingleFnRoutine<F>
where
    F: Fn(AsyncQueue<InputItem>, AsyncQueue<usize>) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>
        + Send,
{
    fn new(factory: F) -> Self {
        let input = AsyncQueue::new();
        let output = AsyncQueue::new();
        let future = (factory)(input.clone(), output.clone());
        Self {
            input,
            output,
            factory,
            future: Some(future),
        }
    }
}

impl<F> areamy::Send<usize> for SingleFnRoutine<F>
where
    F: Fn(AsyncQueue<InputItem>, AsyncQueue<usize>) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>
        + Send,
{
    fn send(&mut self, message: usize) -> Result<(), Error> {
        self.input.push(InputItem::Data(message));
        Ok(())
    }
}

impl<F> areamy::Next<usize> for SingleFnRoutine<F>
where
    F: Fn(AsyncQueue<InputItem>, AsyncQueue<usize>) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>
        + Send,
{
    fn next(&mut self) -> Result<Option<usize>, Error> {
        Ok(self.output.pop())
    }
}

impl<F> areamy::Flush for SingleFnRoutine<F>
where
    F: Fn(AsyncQueue<InputItem>, AsyncQueue<usize>) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>
        + Send,
{
    fn flush(&mut self) -> Result<(), Error> {
        self.input.push(InputItem::Flush);
        Ok(())
    }
}

impl<F> areamy::Poll for SingleFnRoutine<F>
where
    F: Fn(AsyncQueue<InputItem>, AsyncQueue<usize>) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>
        + Send,
{
    fn poll(&mut self, cx: &mut core::task::Context<'_>) -> Result<core::task::Poll<()>, Error> {
        let future = self.future.get_or_insert_with(|| {
            (self.factory)(self.input.clone(), self.output.clone())
        });

        match future.as_mut().poll(cx) {
            core::task::Poll::Ready(result) => {
                result?;
                self.future = None;
                Ok(core::task::Poll::Ready(()))
            }
            core::task::Poll::Pending => Ok(core::task::Poll::Pending),
        }
    }
}

impl<F> Name for SingleFnRoutine<F> where
    F: Fn(AsyncQueue<InputItem>, AsyncQueue<usize>) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>
        + Send
{
}
impl<F> areamy::LineRoutine<usize, usize> for SingleFnRoutine<F> where
    F: Fn(AsyncQueue<InputItem>, AsyncQueue<usize>) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>
        + Send
{
}
impl<F> areamy::AsyncLineRoutine<usize, usize> for SingleFnRoutine<F> where
    F: Fn(AsyncQueue<InputItem>, AsyncQueue<usize>) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>
        + Send
{
}

// ============================================================
// Tests
// ============================================================

/// Bidi streaming via SingleFnRoutine + Join.
/// User's async fn connects, spawns writer + reader, joins them.
/// No framework-managed state machine — Join handles concurrent polling.
#[test]
fn bidi_with_join() -> Result<(), Error> {
    let mut source_node = areamy::work::make_line(Double::new());
    let mut source = areamy::work::Source::<usize>::of(&source_node)?;

    let mut async_thread = AsyncThread::<IoThread>::new();

    let routine = SingleFnRoutine::new(|input, output| Box::pin(async move {
        let socket = FakeSocket::connect("fake://server").await;

        let writer_input = input.clone();
        let writer_socket = socket.clone();
        let writer: Pin<Box<dyn Future<Output = Result<(), Error>> + Send>> =
            Box::pin(async move {
                loop {
                    let item = RecvFut(writer_input.clone()).await;
                    match item {
                        InputItem::Data(val) => writer_socket.write(val * 3).await,
                        InputItem::Flush => {
                            writer_socket.half_close().await;
                            break;
                        }
                    }
                }
                Ok(())
            });

        let reader_socket = socket;
        let reader_output = output.clone();
        let reader: Pin<Box<dyn Future<Output = Result<(), Error>> + Send>> =
            Box::pin(async move {
                while let Some(val) = reader_socket.read().await {
                    reader_output.push(val + 1);
                }
                Ok(())
            });

        Join::pair(writer, reader).await
    }));

    let mut node = async_thread.node(routine).typed::<areamy::poll::Sync>();
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
