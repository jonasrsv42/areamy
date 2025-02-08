// Utility function for typed graph declarations. These functions are only useful for improving
// readability and are not needed for anything else. The `Core` function suffice for building
// graphs because types can be inferred from context.
use crate::{
    error::Error, make_bidi, make_push, AddPushable, AddWorkable, Connection, GetPushable,
    GetWorkable, Message, Origin, Pushable, Trackable, Workable,
};
use std::marker::PhantomData;
pub struct Connect<DataType, SignalType = Trackable<&'static str>> {
    // We do not need to store this type, the graph connections
    // can infer the types from context.
    //
    // However annotating the data that flows through the
    // graph during declaration does help with readability.
    // (subjective).
    //
    // If you do not want to be annotating types, use
    // the `make_bidi` method that this class invokes.
    // it is only templates on `ParentType` and `ChildType`
    // which can be inferred from context in connections.
    datatype: PhantomData<DataType>,
    signaltype: PhantomData<SignalType>,
}

impl<DataType, SignalType> Connect<DataType, SignalType>
where
    DataType: Send + Sync + Clone,
    SignalType: Origin,
{
    // Bidi connection with type hints for the data flowing :)
    pub fn bidi<
        ParentType,
        ChildType,
        PushableType,
        ParentConnection: Connection,
        ChildConnection: Connection,
    >(
        parent: &mut ParentType,
        child: &mut ChildType,
    ) -> Result<(), Error>
    where
        PushableType: Pushable<Message = Message<DataType, SignalType>>,

        ChildType: 'static
            + AddWorkable<ChildConnection, ThreadId = <ParentType::Workable as Workable>::ThreadId>
            + GetPushable<ChildConnection, Pushable = PushableType>,
        ParentType: 'static
            + AddPushable<ParentConnection, Message = <ChildType::Pushable as Pushable>::Message>
            + GetWorkable,
    {
        make_bidi(parent, child)?;

        Ok(())
    }

    pub fn push<
        ParentType,
        ChildType,
        PushableType,
        GetConnection: Connection,
        AddConnection: Connection,
    >(
        parent: &mut ParentType,
        child: &ChildType,
    ) -> Result<(), Error>
    where
        PushableType: Pushable<Message = Message<DataType, SignalType>>,

        ChildType: GetPushable<GetConnection, Pushable = PushableType>,
        ParentType:
            AddPushable<AddConnection, Message = <ChildType::Pushable as Pushable>::Message>,
    {
        make_push(parent, child)?;

        Ok(())
    }
}
