//! Wall-clock limits shared by deadline-arming futures.

use std::time::{Duration, Instant};

/// ~30 years: far beyond any process lifetime, safely addable to any
/// live `Instant`.
const FAR_FUTURE: Duration = Duration::from_secs(60 * 60 * 24 * 365 * 30);

/// `Instant::now() + timeout`, saturating to [FAR_FUTURE] instead of
/// panicking on `Instant` overflow (e.g. `Duration::MAX` as "never").
pub(crate) fn deadline_after(timeout: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(timeout).unwrap_or(now + FAR_FUTURE)
}
