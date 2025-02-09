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
    use crate::node::bifurcation::routine::tests::MockBifurcation;
    use crate::{
        node::bifurcation::sync::node::{LeftSink, RightSink},
        sink::sync::tee,
        sync::Source,
    };
    use crate::{sync::make_bifurcation, DefaultThread, Message, Pushable, Workable};

    #[test]
    fn trackable_signal_tracks_active() {
        let mut bifur = make_bifurcation(Ok(MockBifurcation::new())).unwrap();

        let mut source = Source::new(&bifur).unwrap();

        let mut left_sink = tee::Sink::new::<LeftSink>(&mut bifur).unwrap();
        let mut right_sink = tee::Sink::new::<RightSink>(&mut bifur).unwrap();

        let mut workable: Box<dyn Workable<ThreadId = DefaultThread>> = bifur;

        let hello_track = Trackable::new("hello");

        // It is the only one so it's final.
        assert_eq!(hello_track.active(), 1);

        // Add one flush
        source.push(Message::Flush(hello_track.clone())).unwrap();

        assert_eq!(hello_track.active(), 2);
        workable.work().unwrap();

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
