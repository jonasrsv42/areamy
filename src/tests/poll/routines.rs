//! Future routine factories take any future: a plain or generic `async fn`, an unannotated
//! `async move` closure, or a hand-boxed one. Each runs segment by segment through the routine
//! traits, created by its factory as a node would, so its types are inferred from the same bounds.

use crate::biunion;
use crate::error::Error;
use crate::node::biunion::poll::routine::tests::noop_biunion_wakers;
use crate::node::line::poll::routine::tests::noop_line_wakers;
use crate::node::routine::{Accept, Flush, Next, Poll, TrySend};
use crate::poll::context::mock::noop_context;
use crate::poll::future::biunion::FutureRoutine as BiunionFuture;
use crate::poll::future::line::{FutureRoutine, once};
use crate::poll::future::{Input, Receiver, Sender};
use crate::poll::{
    BiunionRoutine, BiunionRoutineFactory, Either, LineRoutine, LineRoutineFactory, race, try_join,
};
use core::future::Future;
use core::num::NonZeroUsize;
use core::pin::Pin;

/// Polls per segment before it counts as hung.
const POLLS: usize = 100;

/// The old spelling: a trait-object future.
type BoxFut<'params> = Pin<Box<dyn Future<Output = Result<(), Error>> + 'params>>;

async fn triple(input: Receiver<usize>, output: Sender<usize>) -> Result<(), Error> {
    while let Input::Data(value) = input.recv().await? {
        output.send(value * 3).await?;
    }
    Ok(())
}

/// `T` comes from the routine's use.
async fn relay<T>(input: Receiver<T>, output: Sender<T>) -> Result<(), Error> {
    forward(&input, &output).await
}

async fn forward<T>(input: &Receiver<T>, output: &Sender<T>) -> Result<(), Error> {
    while let Input::Data(value) = input.recv().await? {
        output.send(value).await?;
    }
    Ok(())
}

async fn merge<T>(left: Receiver<T>, right: Receiver<T>, output: Sender<T>) -> Result<(), Error> {
    try_join(forward(&left, &output), forward(&right, &output)).await?;
    Ok(())
}

/// One segment, as the node runs it: sends `data` as the queue takes it (via `send`), then the
/// Flush (via `flush`), polling and draining the output until the segment ends.
fn segment<R: Poll + Next<usize>>(
    routine: &mut R,
    data: &[usize],
    mut send: impl FnMut(&mut R, usize) -> Accept<usize>,
    flush: impl FnOnce(&mut R),
) -> Vec<usize> {
    let mut cx = noop_context();
    let mut data = data.iter().copied().peekable();
    let mut flush = Some(flush);
    let mut out = Vec::new();
    for _ in 0..POLLS {
        while let Some(&value) = data.peek() {
            match send(routine, value) {
                Accept::Accepted => _ = data.next(),
                Accept::Full(_) => break,
            }
        }
        if data.peek().is_none()
            && let Some(flush) = flush.take()
        {
            flush(routine);
        }
        let ready = routine.poll(&mut cx).unwrap().is_ready();
        while let Some(value) = routine.next().unwrap() {
            out.push(value);
        }
        if ready {
            return out;
        }
    }
    panic!("segment never ended");
}

/// Runs `segments` through the line routine `factory` creates; the output per segment.
fn run_line<'params, F>(factory: F, segments: &[&[usize]]) -> Vec<Vec<usize>>
where
    F: LineRoutineFactory<'params>,
    F::Routine: LineRoutine<usize, usize>,
{
    let mut routine = factory.create(noop_line_wakers());
    segments
        .iter()
        .map(|data| {
            segment(
                &mut routine,
                data,
                |routine, value| routine.try_send(value).unwrap(),
                |routine| routine.flush().unwrap(),
            )
        })
        .collect()
}

/// Runs `segments` through the biunion routine `factory` creates, alternating data between left
/// and right; the output per segment, sorted as the sides interleave.
fn run_biunion<'params, F>(factory: F, segments: &[&[usize]]) -> Vec<Vec<usize>>
where
    F: BiunionRoutineFactory<'params>,
    F::Routine: BiunionRoutine<usize, usize, usize>,
{
    let mut routine = factory.create(noop_biunion_wakers());
    segments
        .iter()
        .map(|data| {
            let mut left = true;
            let mut out = segment(
                &mut routine,
                data,
                |routine, value| {
                    let accept = if left {
                        TrySend::<usize, biunion::Left>::try_send(routine, value)
                    } else {
                        TrySend::<usize, biunion::Right>::try_send(routine, value)
                    };
                    left = !left;
                    accept.unwrap()
                },
                |routine| routine.flush().unwrap(),
            );
            out.sort_unstable();
            out
        })
        .collect()
}

#[test]
fn an_async_fn_is_a_routine() {
    let out = run_line(FutureRoutine::factory(triple), &[&[1, 2], &[3]]);
    assert_eq!(out, [vec![3, 6], vec![9]]);
}

#[test]
fn a_generic_async_fn_infers_its_type_from_the_routine() {
    let out = run_line(FutureRoutine::factory(relay), &[&[4], &[5, 6]]);
    assert_eq!(out, [vec![4], vec![5, 6]]);
}

#[test]
fn an_unannotated_closure_infers_its_types_from_the_routine() {
    let out = run_line(
        FutureRoutine::factory(|input, output| async move {
            while let Input::Data(value) = input.recv().await? {
                output.send(value + 1).await?;
            }
            Ok(())
        })
        .bounded(NonZeroUsize::new(2)),
        &[&[1, 2, 3], &[4]],
    );
    assert_eq!(out, [vec![2, 3, 4], vec![5]]);
}

#[test]
fn a_closure_may_borrow_from_the_scope() {
    let scale = 7;
    let scale = &scale;
    let out = run_line(
        FutureRoutine::factory(move |input: Receiver<usize>, output| async move {
            while let Input::Data(value) = input.recv().await? {
                output.send(value * *scale).await?;
            }
            Ok(())
        }),
        &[&[2]],
    );
    assert_eq!(out, [vec![14]]);
}

#[test]
fn a_hand_boxed_closure_still_works() {
    let boxed = run_line(
        FutureRoutine::factory(|input: Receiver<usize>, output: Sender<usize>| {
            Box::pin(async move { forward(&input, &output).await })
        }),
        &[&[9]],
    );
    let dyn_boxed = run_line(
        FutureRoutine::factory(
            |input: Receiver<usize>, output: Sender<usize>| -> BoxFut<'_> {
                Box::pin(async move { forward(&input, &output).await })
            },
        ),
        &[&[10]],
    );
    assert_eq!((boxed, dyn_boxed), (vec![vec![9]], vec![vec![10]]));
}

/// Not `Clone`: only an [FnOnce] can move it into the future.
struct Lease(usize);

#[test]
fn once_takes_an_unannotated_closure_that_moves_its_captures() {
    let lease = Lease(5);
    let out = run_line(
        once::FutureRoutine::factory(move |input, output| async move {
            let lease = lease;
            while let Input::Data(value) = input.recv().await? {
                output.send(value + lease.0).await?;
            }
            Ok(())
        }),
        &[&[1, 2]],
    );
    assert_eq!(out, [vec![6, 7]]);
}

#[test]
fn once_takes_an_async_fn() {
    let out = run_line(once::FutureRoutine::factory(triple), &[&[7]]);
    assert_eq!(out, [vec![21]]);
}

#[test]
fn once_takes_a_hand_boxed_closure() {
    let out = run_line(
        once::FutureRoutine::factory(|input: Receiver<usize>, output: Sender<usize>| {
            Box::pin(async move { forward(&input, &output).await })
        }),
        &[&[8]],
    );
    assert_eq!(out, [vec![8]]);
}

#[test]
fn biunion_takes_a_generic_async_fn() {
    let out = run_biunion(BiunionFuture::factory(merge), &[&[1, 2], &[3]]);
    assert_eq!(out, [vec![1, 2], vec![3]]);
}

#[test]
fn biunion_takes_an_unannotated_racing_closure() {
    let out = run_biunion(
        BiunionFuture::factory(|left, right, output| async move {
            let other = loop {
                match race(left.recv(), right.recv()).await {
                    Either::Left(item) => match item? {
                        Input::Data(value) => output.send(value).await?,
                        Input::Flush => break &right,
                    },
                    Either::Right(item) => match item? {
                        Input::Data(value) => output.send(value).await?,
                        Input::Flush => break &left,
                    },
                }
            };
            forward(other, &output).await
        }),
        &[&[1, 2, 3], &[4]],
    );
    assert_eq!(out, [vec![1, 2, 3], vec![4]]);
}

#[test]
fn biunion_takes_a_hand_boxed_closure() {
    let out = run_biunion(
        BiunionFuture::factory(
            |left: Receiver<usize>, right: Receiver<usize>, output: Sender<usize>| {
                Box::pin(merge(left, right, output))
            },
        ),
        &[&[5, 6]],
    );
    assert_eq!(out, [vec![5, 6]]);
}
