use std::fmt::Debug;
use std::hash::Hash;

pub trait Origin: Debug + Eq + Sync + Send + Hash {}

impl Origin for usize {}
impl Origin for &'static str {}

#[cfg(test)]
mod tests {

    #[test]
    fn signal_origin_can_pass() {}
}
