//! [std::thread::Thread] markers.
use core::fmt::Debug;

/// [`ThreadId`] is used to mark a graph node with what group of threads are allowed to schedule it.
pub trait ThreadId: Send + Sync + 'static {
    /// The thread's name: its OS thread's, and the label in diagnostics. The type's name by
    /// default, without its module path or generic arguments (`Per<Camera>` is `Per`): Linux keeps
    /// 15 bytes of a thread name, and the path would fill them. Override it to tell apart two
    /// threads whose types share a name. On ESP-IDF it labels diagnostics only: std doesn't name
    /// the FreeRTOS task there.
    fn name() -> &'static str {
        let path = core::any::type_name::<Self>();
        let without_generics = path.split('<').next().unwrap_or(path);
        without_generics
            .rsplit("::")
            .next()
            .unwrap_or(without_generics)
    }
}

/// [`DefaultThread`] is the default thread group.
#[derive(Debug)]
pub struct DefaultThread {}

/// [DefaultThread] is a [ThreadId]
impl ThreadId for DefaultThread {}

/// Declares [ThreadId] marker types: `thread_id!(pub EncoderThread, DecoderThread);`
#[macro_export]
macro_rules! thread_id {
    ($($vis:vis $name:ident),+ $(,)?) => {
        $(
            #[derive(Debug, Clone)]
            $vis struct $name;
            impl $crate::ThreadId for $name {}
        )+
    };
}
