use crate::error::Error;

mod downstream;

pub use downstream::Downstream;

/// [`Via`] is the owning edge: `up.via(down)?` moves `up` into `down` and returns `down`
/// (work) or the resolved builder (poll).
///
/// Both ends are single-sided; name a side with [crate::At::at]. The data type is an
/// optional annotation: `up.via::<Frame>(down)?`.
///
/// Scheduling can't cycle: `up` is moved. Signals are forwarded
/// ([crate::SignalPolicy::Forward]), which is safe for the same reason.
pub trait Via<DownstreamType, OwnedType>: Sized {
    fn via<DataType>(self, downstream: DownstreamType) -> Result<DownstreamType::Out, Error>
    where
        DownstreamType: Downstream<Self, DataType, OwnedType>;
}

impl<UpstreamType, DownstreamType, OwnedType> Via<DownstreamType, OwnedType> for UpstreamType {
    fn via<DataType>(self, downstream: DownstreamType) -> Result<DownstreamType::Out, Error>
    where
        DownstreamType: Downstream<Self, DataType, OwnedType>,
    {
        downstream.take(self)
    }
}
