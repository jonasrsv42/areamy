//! Utility functions for typed [make_bidi] and [make_push]
use crate::{
    Closeable, Origin, Trackable, Workable,
    error::Error,
    graph::{Add, Get},
    make_bidi, make_push,
    marker::Multiplicity,
};
use std::marker::PhantomData;

/// [`Connect`] is a utility that serves no purpose beyond improving readability
/// it provides no additional functionality beyond what [make_push] and [make_bidi] does.
///
/// What it does provide is the ability to annotate the data that is [Closeable::push]ed through
/// the connection. It has been deemed to reduce cognitive overhead when it comes to reading
/// the graph declaration if typing is more visible.
///
/// Compare
///
/// ```bash
/// areamy::work::Connect::<EncoderFrame>::bidi(a, &mut b)
/// areamy::work::Connect::<DecoderFrame>::bidi(b, &mut c)
/// areamy::work::Connect::<Decision>::bidi(c, &mut d)
/// ```
/// versus
///
/// ```bash
/// make_bidi(a, b)
/// make_bidi(b, c)
/// make_bidi(c, d)
/// ```
///
/// While rust is completely fine to infer all the types in the graph as-long as
/// all input nodes and output nodes are typed, it does help readability to be
/// able to annotate the data in the code of intermediate nodes as well.
pub struct Connect<DataType, SignalType = Trackable<&'static str>> {
    /// We do not need to store this type, the graph connections
    /// can infer the types from context.
    ///
    /// However annotating the data that flows through the
    /// graph during declaration does help with readability.
    /// (subjective).
    ///
    /// If you do not want to be annotating types, use
    /// the `make_bidi` method that this class invokes.
    /// it is only templates on `ParentType` and `ChildType`
    /// which can be inferred from context in connections.
    datatype: PhantomData<DataType>,
    signaltype: PhantomData<SignalType>,
}

impl<DataType, SignalType> Connect<DataType, SignalType>
where
    DataType: Send + Sync + 'static,
    SignalType: Origin + Send + Sync + 'static,
{
    /// Exactly the same as [make_bidi] but with ability to ergonomically annotate the DataType.
    pub fn bidi<
        ParentType,
        ChildType,
        ParentMultiplicity: Multiplicity,
        ChildMultiplicity: Multiplicity,
        ThreadIdType,
    >(
        parent: Box<ParentType>,
        child: &mut ChildType,
    ) -> Result<(), Error>
    where
        ChildType: 'static
            + Add<dyn Workable<ThreadId = ThreadIdType>, ChildMultiplicity>
            + Get<
                dyn Closeable<DataType = DataType, SignalType = SignalType> + Send + Sync,
                ChildMultiplicity,
            >,
        ParentType: Add<
                dyn Closeable<DataType = DataType, SignalType = SignalType> + Send + Sync,
                ParentMultiplicity,
            > + Workable<ThreadId = ThreadIdType>
            + 'static,
    {
        make_bidi(parent, child)?;

        Ok(())
    }

    /// Exactly the same as [make_push] but with ability to ergonomically annotate the DataType.
    pub fn push<AddMultiplicity: Multiplicity, GetMultiplicity: Multiplicity>(
        parent: &mut impl Add<
            dyn Closeable<DataType = DataType, SignalType = SignalType> + Send + Sync,
            AddMultiplicity,
        >,
        child: &impl Get<
            dyn Closeable<DataType = DataType, SignalType = SignalType> + Send + Sync,
            GetMultiplicity,
        >,
    ) -> Result<(), Error> {
        make_push(parent, child)?;

        Ok(())
    }
}
