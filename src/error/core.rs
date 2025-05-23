use std::any::Any;
use std::backtrace::Backtrace;
use std::convert::Infallible;

/// [`Location`] in the code of an error.
#[derive(Debug)]
pub struct Location {
    pub file: &'static str,
    pub line: u32,
}

impl std::fmt::Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.file, self.line)
    }
}

/// [`AnyErr`] is an error type and [Any] type to support reflection
/// and propagating user defined errors via Areamy.
///
/// Since third-parties may implement nodes for Areamy and need to propagate
/// custom errors through Areamy we support this via the [AnyErr] using reflection.
pub trait AnyErr: Any + std::error::Error {}

/// [`ErrorKind`] describes the types of errors that can occur in Areamy.
#[derive(Debug)]
pub enum ErrorKind {
    /// For fatal errors that are not recoverable.
    /// These should just bubble up to the main binary
    /// and provide a helpful error message.
    Fatal(String),
    /// Other kinds of errors, could be user-defined.
    /// To handle these users have to try to runtime cast
    /// it to types they can handle and then handle it.
    /// We leverage `Any` because we cannot possibly
    /// enumerate all possible error types users may define.
    Any(Box<dyn AnyErr + Send>),
}

impl std::fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::Fatal(message) => {
                write!(f, "Fatal")?;
                write!(f, "\n\n{}", message)
            }
            ErrorKind::Any(any_err) => any_err.fmt(f),
        }
    }
}

/// [`crate::fatal`] is a macro for producing a [ErrorKind::Fatal] error.
#[macro_export]
macro_rules! fatal {
    ($message:expr) => {
        $crate::error::Error {
            kind: $crate::error::ErrorKind::Fatal($message.to_string()),
            location: $crate::error::Location {
                file: file!(),
                line: line!(),
            },
            backtrace: std::backtrace::Backtrace::capture(),
        }
    };
}

/// [`crate::any_err`] is a macro for producing a [ErrorKind::Any] error.
#[macro_export]
macro_rules! any_err {
    ($any:expr) => {
        $crate::error::Error {
            kind: $crate::error::ErrorKind::Any($any.into()),
            location: $crate::error::Location {
                file: file!(),
                line: line!(),
            },
            backtrace: std::backtrace::Backtrace::capture(),
        }
    };
}

/// [`Error`] is the error type typically returned in [std::result::Result]. it supports user defined errors, backtraces and storing error
/// locations.
pub struct Error {
    /// The type of error carried, could be user defined.
    pub kind: ErrorKind,
    /// The code location of the error.
    pub location: Location,
    /// The backtrace, if supported.
    pub backtrace: Backtrace,
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.kind)?;
        write!(f, "\n\nAt: {}", self.location)?;
        write!(f, "\n\nTrace: ")?;
        write!(f, "\n\n{}", self.backtrace)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

impl<T> From<Error> for Result<T, Box<dyn std::error::Error>> {
    fn from(value: Error) -> Self {
        Err(Box::new(value))
    }
}

impl<T> From<Error> for Result<T, Error> {
    fn from(value: Error) -> Self {
        Err(value)
    }
}

impl From<Infallible> for Error {
    fn from(_: Infallible) -> Self {
        unreachable!("Infallible errors can never occur")
    }
}

#[cfg(test)]
mod tests {

    fn inside_a_fn() {
        let error: Result<(), Box<dyn std::error::Error>> = fatal!("Invalid").into();
        error.unwrap()
    }

    #[ignore]
    #[test]
    fn error_make_stacktrace() {
        inside_a_fn()
    }
}
