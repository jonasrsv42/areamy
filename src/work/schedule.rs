use crate::error::Error;
use crate::graph::Add;
use crate::graph::marker::Multiplicity;
use crate::work::Workable;

/// [`Schedule`] is a scheduling-only connection. See [Schedule::connect].
///
/// ```ignore
/// work::Schedule::connect(parent, &mut child)?;
/// work::Schedule::connect(parent, &mut biunion.at::<Right>())?;
/// ```
pub enum Schedule {}

impl Schedule {
    /// [`Schedule::connect`] creates a `work` connection between two nodes.
    ///
    /// A `work` connection in a scheduling connection. In this case the child can make the parent
    /// work.
    ///
    /// * `parent` - A [Workable]. The parent will be [Add]ed to the child so the
    ///   child can schedule the work.
    ///
    /// * `child` - A type that we can [Add] the parent [Workable] to for scheduling.
    ///
    /// The function takes the parent by value as its ownership will be transferred to the child.
    /// The child is taken by mutable reference as we will be adding the parent to it.
    ///
    /// After this call, the `child` will own the parent. The child can keep being connected into
    /// things in the graph, but the `parent` is done.
    ///
    /// <div class="info">
    /// By transferring ownership upon scheduling connection we make it hard to introduce bugs such as
    /// scheduling circles, which would be deadlocks.
    /// </div>
    pub fn connect<'params, MultiplicityType: Multiplicity, ThreadIdType>(
        parent: impl Workable<ThreadId = ThreadIdType> + 'params,
        child: &mut impl Add<dyn Workable<ThreadId = ThreadIdType> + 'params, MultiplicityType>,
    ) -> Result<(), Error> {
        Add::add(child, Box::new(parent))
    }
}
