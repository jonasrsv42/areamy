use crate::Origin;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub struct Trackable<OriginType>
where
    OriginType: Origin + Hash,
{
    origin: Arc<OriginType>,
    active: Arc<AtomicUsize>,
}

impl<OriginType> Clone for Trackable<OriginType>
where
    OriginType: Origin + Hash,
{
    fn clone(&self) -> Self {
        self.active.fetch_add(1, Ordering::Relaxed);
        Trackable {
            origin: self.origin.clone(),
            active: self.active.clone(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        *self = source.clone()
    }
}

impl<OriginType> Drop for Trackable<OriginType>
where
    OriginType: Origin + Hash,
{
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::Relaxed);
    }
}

impl<OriginType> Trackable<OriginType>
where
    OriginType: Clone + Origin + Hash + 'static,
{
    pub fn new(origin: OriginType) -> Self {
        Trackable {
            origin: Arc::new(origin),
            active: Arc::new(AtomicUsize::new(1)),
        }
    }

    pub fn active(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }
}

impl Into<Trackable<&'static str>> for &'static str {
    fn into(self) -> Trackable<&'static str> {
        Trackable::new(self)
    }
}

impl<OriginType> Hash for Trackable<OriginType>
where
    OriginType: Origin,
{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Hash::hash(&self.origin, state)
    }
}
impl<OriginType> PartialEq for Trackable<OriginType>
where
    OriginType: Origin,
{
    fn eq(&self, other: &Self) -> bool {
        self.origin == other.origin
    }
}

impl<OriginType> Eq for Trackable<OriginType> where OriginType: Origin {}

impl<OriginType> Origin for Trackable<OriginType> where OriginType: Origin + Clone + 'static {}

pub struct Visitors {
    visitors: HashSet<u64>,
    hasher: DefaultHasher,
}

impl Visitors {
    pub fn new() -> Self {
        Visitors {
            visitors: HashSet::new(),
            hasher: DefaultHasher::new(),
        }
    }

    pub fn contains<OriginType>(&mut self, origin: &OriginType) -> bool
    where
        OriginType: Origin,
    {
        origin.hash(&mut self.hasher);
        let hash = self.hasher.finish();

        self.visitors.contains(&hash)
    }

    pub fn insert<OriginType>(&mut self, origin: &OriginType) -> bool
    where
        OriginType: Origin,
    {
        origin.hash(&mut self.hasher);
        let hash = self.hasher.finish();

        self.visitors.insert(hash)
    }

    pub fn clear(&mut self) -> () {
        if self.visitors.len() != 0 {
            self.visitors.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::{sync::make_bifurcation, BifurcationRoutine, Message, Pushable};
    use crate::{sync::Sink, sync::Source};
    use std::collections::VecDeque;

    struct MockBifurcation {
        left_out: VecDeque<usize>,
        right_out: VecDeque<usize>,
    }

    impl MockBifurcation {
        pub fn new() -> Self {
            MockBifurcation {
                left_out: VecDeque::new(),
                right_out: VecDeque::new(),
            }
        }
    }

    impl BifurcationRoutine<usize, usize, usize> for MockBifurcation {
        fn left_output(&mut self) -> &mut VecDeque<usize> {
            &mut self.left_out
        }

        fn right_output(&mut self) -> &mut VecDeque<usize> {
            &mut self.right_out
        }

        fn work(&mut self, object: usize) -> Result<(), Error> {
            self.left_out.push_back(object);
            self.right_out.push_back(object);

            Ok(())
        }

        fn flush(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }

    #[test]
    fn trackable_signal_tracks_active() {
        let bifur = make_bifurcation(Ok(MockBifurcation::new())).unwrap();

        let mut source = Source::new(bifur.input()).unwrap();

        let mut left_sink = Sink::new(bifur.workable(), &mut bifur.output().left).unwrap();
        let mut right_sink = Sink::new(bifur.workable(), &mut bifur.output().right).unwrap();

        let hello_track = Trackable::new("hello");

        // It is the only one so it's final.
        assert_eq!(hello_track.active(), 1);

        // Add one flush
        source.push(Message::Flush(hello_track.clone())).unwrap();

        assert_eq!(hello_track.active(), 2);

        {
            let _signal = left_sink.read().unwrap();

            // There is 3 instances alive now. One inside the signal variable above and one inside
            // the output of the right_sink and the original one.
            assert_eq!(hello_track.active(), 3);
        }

        // Now 2 cause we dropped the last _signal
        assert_eq!(hello_track.active(), 2);

        {
            let _signal = right_sink.read().unwrap();

            // There is 2 instances alive now. This one and the original one.
            assert_eq!(hello_track.active(), 2);
        }

        // Now only original instance is alive.
        assert_eq!(hello_track.active(), 1);
    }
}
