//! Wiring that must not compile, pinned by error code. [Wiring] is the passing twin.
//!
//! Collected by rustdoc only (`cfg(doctest)`); routines come from `misuse/routines.rs`.

/// The passing wiring the cases below break.
///
/// ```
/// # include!("misuse/routines.rs");
/// # use areamy::work::{Bifurcation, Biunion, Line};
/// # use areamy::{At, Pushes, Reads, Schedules, Tees, Via, Writes, bifurcation, biunion};
/// # fn main() -> Result<(), areamy::error::Error> {
/// let head = Line::of(pass());
/// let _input = head.writer()?;
/// let mut router = head.via(Bifurcation::of(split()))?;
/// let noise = Line::of(pass());
/// router.at_mut::<bifurcation::Right>().pushes(&noise)?;
/// let mut _side = router.at_mut::<bifurcation::Right>().tee()?;
/// let speech = router.at::<bifurcation::Left>().via(Line::of(pass()))?;
///
/// let merge = Biunion::of(join());
/// let _text = merge.at_ref::<biunion::Right>().writer::<String>()?;
/// let mut merge = speech.via(merge.at::<biunion::Left>())?;
/// merge.at_mut::<biunion::Left>().schedules(noise)?;
/// let _out = merge.reader()?;
/// # Ok(())
/// # }
/// ```
pub struct Wiring;

/// An upstream is moved by `via`: owning it twice (a cycle's first step) is a use after move.
///
/// ```compile_fail,E0382
/// # include!("misuse/routines.rs");
/// # use areamy::work::Line;
/// # use areamy::{Reads, Via, Writes};
/// # fn main() -> Result<(), areamy::error::Error> {
/// let head = Line::of(pass());
/// let _input = head.writer()?;
/// let _first = head.via(Line::of(pass()))?.reader()?;
/// let _second = head.via(Line::of(pass()))?.reader()?;
/// # Ok(())
/// # }
/// ```
pub struct OwnedTwice;

/// A moved side is owned too: the router can't be wired again after `at().via`.
///
/// ```compile_fail,E0382
/// # include!("misuse/routines.rs");
/// # use areamy::work::{Bifurcation, Line};
/// # use areamy::{At, Reads, Tees, Via, Writes, bifurcation};
/// # fn main() -> Result<(), areamy::error::Error> {
/// # let head = Line::of(pass());
/// # let _input = head.writer()?;
/// let mut router = head.via(Bifurcation::of(split()))?;
/// let _speech = router.at::<bifurcation::Left>().via(Line::of(pass()))?.reader()?;
/// let _noise = router.at_mut::<bifurcation::Right>().tee::<usize>()?;
/// # Ok(())
/// # }
/// ```
pub struct SideOwnedTwice;

/// A two-sided upstream needs its side named; no guessing by data type.
///
/// ```compile_fail,E0277
/// # include!("misuse/routines.rs");
/// # use areamy::work::{Bifurcation, Line};
/// # use areamy::{Reads, Via, Writes};
/// # fn main() -> Result<(), areamy::error::Error> {
/// # let head = Line::of(pass());
/// # let _input = head.writer()?;
/// let router = head.via(Bifurcation::of(split()))?;
/// let _speech = router.via(Line::of(pass()))?.reader()?;
/// # Ok(())
/// # }
/// ```
pub struct UpstreamSideMissing;

/// A two-sided downstream needs its side named, even when the input types differ.
///
/// ```compile_fail,E0277
/// # include!("misuse/routines.rs");
/// # use areamy::work::{Biunion, Line};
/// # use areamy::{Reads, Via, Writes};
/// # fn main() -> Result<(), areamy::error::Error> {
/// # let head = Line::of(pass());
/// # let _input = head.writer()?;
/// let _merge = head.via(Biunion::of(join()))?.reader()?;
/// # Ok(())
/// # }
/// ```
pub struct DownstreamSideMissing;

/// `at` moves: pushing from a moved side would drop the node.
///
/// ```compile_fail,E0599
/// # include!("misuse/routines.rs");
/// # use areamy::work::{Bifurcation, Line};
/// # use areamy::{At, Pushes, Via, Writes, bifurcation};
/// # fn main() -> Result<(), areamy::error::Error> {
/// # let head = Line::of(pass());
/// # let _input = head.writer()?;
/// let router = head.via(Bifurcation::of(split()))?;
/// router.at::<bifurcation::Right>().pushes(&Line::of(pass()))?;
/// # Ok(())
/// # }
/// ```
pub struct PushFromMovedSide;

/// `at_ref` only gets from a node: it can't be pushed from.
///
/// ```compile_fail,E0599
/// # include!("misuse/routines.rs");
/// # use areamy::work::{Bifurcation, Line};
/// # use areamy::{At, Pushes, Via, Writes, bifurcation};
/// # fn main() -> Result<(), areamy::error::Error> {
/// # let head = Line::of(pass());
/// # let _input = head.writer()?;
/// let router = head.via(Bifurcation::of(split()))?;
/// router.at_ref::<bifurcation::Right>().pushes(&Line::of(pass()))?;
/// # Ok(())
/// # }
/// ```
pub struct PushFromSharedSide;

/// `at_ref` only gets from a node: it can't take a tee.
///
/// ```compile_fail,E0277
/// # include!("misuse/routines.rs");
/// # use areamy::work::{Bifurcation, Line};
/// # use areamy::{At, Tees, Via, Writes, bifurcation};
/// # fn main() -> Result<(), areamy::error::Error> {
/// # let head = Line::of(pass());
/// # let _input = head.writer()?;
/// let router = head.via(Bifurcation::of(split()))?;
/// let _side = router.at_ref::<bifurcation::Right>().tee::<usize>()?;
/// # Ok(())
/// # }
/// ```
pub struct TeeOnSharedSide;

/// The data types of an owning edge must match.
///
/// ```compile_fail,E0271
/// # include!("misuse/routines.rs");
/// # use areamy::work::Line;
/// # use areamy::{Reads, Via, Writes};
/// # fn main() -> Result<(), areamy::error::Error> {
/// # let head = Line::of(pass());
/// # let _input = head.writer()?;
/// let _words = head.via(Line::of(text()))?.reader()?;
/// # Ok(())
/// # }
/// ```
pub struct DataMismatch;

/// A work node can't own a poll builder: bridge with a Sync input instead.
///
/// ```compile_fail,E0277
/// # include!("misuse/routines.rs");
/// # use areamy::poll::{self, future::line::FutureRoutine};
/// # use areamy::work::Line;
/// # use areamy::{DefaultThread, Via, Writes};
/// # fn main() -> Result<(), areamy::error::Error> {
/// let mut thread = poll::Thread::<'_, DefaultThread>::new();
/// let head = Line::of(pass());
/// let _input = head.writer()?;
/// let _node = head.via(thread.line(FutureRoutine::factory(forward)))?;
/// # Ok(())
/// # }
/// ```
pub struct WorkIntoPoll;

/// A poll node can't own a work node: bridge with a Sync output instead.
///
/// ```compile_fail,E0277
/// # include!("misuse/routines.rs");
/// # use areamy::poll::{self, future::line::FutureRoutine};
/// # use areamy::work::Line;
/// # use areamy::{DefaultThread, Reads, Via, Writes};
/// # fn main() -> Result<(), areamy::error::Error> {
/// let mut thread = poll::Thread::<'_, DefaultThread>::new();
/// let head = thread.line(FutureRoutine::factory(forward)).input::<poll::Sync>();
/// let _input = head.writer()?;
/// let _out = head.via(Line::of(pass()))?.reader()?;
/// # Ok(())
/// # }
/// ```
pub struct PollIntoWork;

/// `via` hands back the downstream; dropping it drops the subgraph.
///
/// ```compile_fail
/// #![deny(unused_must_use)]
/// # include!("misuse/routines.rs");
/// # use areamy::node::line::work::node::Node;
/// # use areamy::work::Line;
/// # use areamy::{DefaultThread, Trackable, Via, Writes};
/// # fn main() -> Result<(), areamy::error::Error> {
/// # let head: Line<'_, Node<usize, usize, Trackable<&'static str>, DefaultThread, Pass>> =
/// #     Line::of(pass());
/// # let _input = head.writer()?;
/// head.via(Line::of(pass()))?;
/// # Ok(())
/// # }
/// ```
pub struct DroppedDownstream;

/// A reader owns the node it drives; dropping it drops the subgraph.
///
/// ```compile_fail
/// #![deny(unused_must_use)]
/// # include!("misuse/routines.rs");
/// # use areamy::work::Line;
/// # use areamy::{Reads, Writes};
/// # fn main() -> Result<(), areamy::error::Error> {
/// # let tail = Line::of(pass());
/// # let _input = tail.writer()?;
/// tail.reader()?;
/// # Ok(())
/// # }
/// ```
pub struct DroppedReader;

/// A thread stream owns its root; dropping it unstarted drops the subgraph.
///
/// ```compile_fail
/// #![deny(unused_must_use)]
/// # include!("misuse/routines.rs");
/// # use areamy::work::Line;
/// # use areamy::{DefaultThread, Streams, Writes};
/// # fn main() -> Result<(), areamy::error::Error> {
/// # let root = Line::of(pass());
/// # let _input = root.writer()?;
/// root.stream::<DefaultThread>();
/// # Ok(())
/// # }
/// ```
pub struct DroppedStream;
