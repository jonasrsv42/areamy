//! Injection logic for [`ThreadBundle::on_first_error`].
//!
//! [`ThreadBundle::on_first_error`]: super::ThreadBundle::on_first_error

use crate::thread::done::Failure;
use crate::thread::type_erase::TypeErasedInternalThreadStream;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};

pub(super) type OnFirstError = Box<dyn FnOnce(&Failure) + Send + 'static>;

/// Inject an `on_done` hook on every thread that races to drain
/// `callbacks` on the first `Done::Error` / `Done::Panic`.
///
/// The mutex + `mem::take` pattern is the at-most-once latch: the
/// first thread to lock and find a non-empty vec drains it and fires
/// all callbacks; subsequent winners find the slot empty and do
/// nothing.
///
/// Each user callback is dispatched inside [`catch_unwind`] so one
/// misbehaving callback cannot prevent its siblings from running and
/// cannot escape into the spawn closure (which would otherwise
/// double-panic when fired from the panic path).
pub(super) fn inject<'params>(
    threads: &mut [Box<dyn TypeErasedInternalThreadStream<'params> + 'params>],
    callbacks: Vec<OnFirstError>,
) {
    if callbacks.is_empty() {
        return;
    }
    let slot: Arc<Mutex<Vec<OnFirstError>>> = Arc::new(Mutex::new(callbacks));

    for thread in threads.iter_mut() {
        let slot = slot.clone();
        thread.on_done(Box::new(move |done| {
            // Check if a failure before grabbing lock to avoid unnecessary locking.
            let Some(failure) = Failure::from_done(done) else {
                return;
            };
            // Grab lock and take all callbacks for the first failure, subsequent failures will
            // have empty vec with no callbacks.
            let callbacks = match slot.lock() {
                Ok(mut guard) => std::mem::take(&mut *guard),
                Err(_) => {
                    #[cfg(not(feature = "silent"))]
                    eprintln!("on_first_error: mutex poisoned, skipping callbacks");
                    return;
                }
            };
            for cb in callbacks {
                if catch_unwind(AssertUnwindSafe(|| cb(&failure))).is_err() {
                    #[cfg(not(feature = "silent"))]
                    eprintln!("on_first_error callback panicked, continuing");
                }
            }
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::super::ThreadBundle;
    use super::super::fixtures::{ImmediateClose, Panicker, ThreadA, ThreadB, WorkError};
    use crate::graph::Add;
    use crate::thread::{Failure, ThreadStream};
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn fires_on_thread_error() {
        let mut thread_a = ThreadStream::<'_, ThreadA>::new();
        thread_a.add(Box::new(WorkError::<ThreadA>::new())).unwrap();
        let thread_b = ThreadStream::<'_, ThreadB>::new();

        let count = Arc::new(AtomicUsize::new(0));
        let count_cb = count.clone();
        let mut bundle = ThreadBundle::new();
        bundle.add(thread_a).add(thread_b).on_first_error(move |_| {
            count_cb.fetch_add(1, Ordering::SeqCst);
        });

        std::thread::scope(|s| {
            let _ = bundle.start(s).join();
        });
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn fires_on_panic() {
        let mut thread_a = ThreadStream::<'_, ThreadA>::new();
        thread_a.add(Box::new(Panicker::<ThreadA>::new())).unwrap();
        let thread_b = ThreadStream::<'_, ThreadB>::new();

        let count = Arc::new(AtomicUsize::new(0));
        let count_cb = count.clone();
        let mut bundle = ThreadBundle::new();
        bundle.add(thread_a).add(thread_b).on_first_error(move |_| {
            count_cb.fetch_add(1, Ordering::SeqCst);
        });

        std::thread::scope(|s| {
            let _ = bundle.start(s).join();
        });
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn does_not_fire_on_clean_exit() {
        let mut thread_a = ThreadStream::<'_, ThreadA>::new();
        thread_a
            .add(Box::new(ImmediateClose::<ThreadA>::new()))
            .unwrap();
        let mut thread_b = ThreadStream::<'_, ThreadB>::new();
        thread_b
            .add(Box::new(ImmediateClose::<ThreadB>::new()))
            .unwrap();

        let count = Arc::new(AtomicUsize::new(0));
        let count_cb = count.clone();
        let mut bundle = ThreadBundle::new();
        bundle.add(thread_a).add(thread_b).on_first_error(move |_| {
            count_cb.fetch_add(1, Ordering::SeqCst);
        });

        std::thread::scope(|s| {
            let _ = bundle.start(s).join();
        });
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn fires_at_most_once_even_with_many_errors() {
        let mut thread_a = ThreadStream::<'_, ThreadA>::new();
        thread_a.add(Box::new(WorkError::<ThreadA>::new())).unwrap();
        let mut thread_b = ThreadStream::<'_, ThreadB>::new();
        thread_b.add(Box::new(Panicker::<ThreadB>::new())).unwrap();

        let count = Arc::new(AtomicUsize::new(0));
        let count_cb = count.clone();
        let mut bundle = ThreadBundle::new();
        bundle.add(thread_a).add(thread_b).on_first_error(move |_| {
            count_cb.fetch_add(1, Ordering::SeqCst);
        });

        std::thread::scope(|s| {
            let _ = bundle.start(s).join();
        });
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn multiple_registrations_all_fire_in_order() {
        let mut thread_a = ThreadStream::<'_, ThreadA>::new();
        thread_a.add(Box::new(WorkError::<ThreadA>::new())).unwrap();

        let order = Arc::new(Mutex::new(Vec::<u32>::new()));
        let o1 = order.clone();
        let o2 = order.clone();
        let o3 = order.clone();

        let mut bundle = ThreadBundle::new();
        bundle
            .add(thread_a)
            .on_first_error(move |_| o1.lock().unwrap().push(1))
            .on_first_error(move |_| o2.lock().unwrap().push(2))
            .on_first_error(move |_| o3.lock().unwrap().push(3));

        std::thread::scope(|s| {
            let _ = bundle.start(s).join();
        });
        assert_eq!(*order.lock().unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn callback_receives_error_reference() {
        let mut thread_a = ThreadStream::<'_, ThreadA>::new();
        thread_a.add(Box::new(WorkError::<ThreadA>::new())).unwrap();

        let captured = Arc::new(Mutex::new(None::<String>));
        let cap = captured.clone();
        let mut bundle = ThreadBundle::new();
        bundle.add(thread_a).on_first_error(move |failure| {
            let label = match failure {
                Failure::Error(e) => format!("error: {}", e),
                Failure::Panic => "panic".to_string(),
            };
            *cap.lock().unwrap() = Some(label);
        });

        std::thread::scope(|s| {
            let _ = bundle.start(s).join();
        });
        let got = captured.lock().unwrap().clone().unwrap();
        assert!(got.starts_with("error: "));
        assert!(got.contains("work failed"));
    }

    /// A panicking on_first_error callback must not propagate (would
    /// double-panic when the failure path is `Done::Panic`) and must
    /// not prevent its siblings from running.
    #[test]
    fn panicking_callback_does_not_block_siblings() {
        let mut thread_a = ThreadStream::<'_, ThreadA>::new();
        thread_a.add(Box::new(Panicker::<ThreadA>::new())).unwrap();

        let seen = Arc::new(Mutex::new(Vec::<u32>::new()));
        let s1 = seen.clone();
        let s3 = seen.clone();
        let mut bundle = ThreadBundle::new();
        bundle
            .add(thread_a)
            .on_first_error(move |_| {
                s1.lock().unwrap().push(1);
            })
            .on_first_error(|_| panic!("on_first_error #2 panics"))
            .on_first_error(move |_| {
                s3.lock().unwrap().push(3);
            });

        // If catch_unwind were missing in inject, the panic on the
        // panic-path (Done::Panic) would double-panic and abort.
        std::thread::scope(|s| {
            let _ = bundle.start(s).join();
        });
        assert_eq!(*seen.lock().unwrap(), vec![1, 3]);
    }

    #[test]
    fn callback_receives_panic_marker() {
        let mut thread_a = ThreadStream::<'_, ThreadA>::new();
        thread_a.add(Box::new(Panicker::<ThreadA>::new())).unwrap();

        let captured = Arc::new(Mutex::new(None::<&'static str>));
        let cap = captured.clone();
        let mut bundle = ThreadBundle::new();
        bundle.add(thread_a).on_first_error(move |failure| {
            *cap.lock().unwrap() = Some(match failure {
                Failure::Error(_) => "error",
                Failure::Panic => "panic",
            });
        });

        std::thread::scope(|s| {
            let _ = bundle.start(s).join();
        });
        assert_eq!(*captured.lock().unwrap(), Some("panic"));
    }
}
