use crate::graph::Get;
use crate::graph::marker::Multiplicity;
use crate::signal::Origin;
use crate::work::Sink;
use std::marker::PhantomData;

/// [`Push`] pins the data type of an input that stays unconnected. See [Push::open].
pub struct Push<DataType>(PhantomData<DataType>);

impl<DataType: Send + Sync + 'static> Push<DataType> {
    /// Pin the data type of an input that stays unconnected (e.g. fed by a
    /// [crate::work::Writer] elsewhere).
    pub fn open<'params, GetMultiplicity: Multiplicity>(
        _child: &impl Get<
            dyn Sink<DataType = DataType, SignalType = impl Origin + 'static>
                + Send
                + Sync
                + 'params,
            GetMultiplicity,
        >,
    ) {
    }
}
