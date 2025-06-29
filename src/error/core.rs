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
pub trait AnyErr: Any + std::error::Error + Send {}

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
    Any(Box<dyn AnyErr>),
}

impl Error {
    /// Try to downcast an Any error to a specific type, consuming self.
    /// Returns `Ok(Box<T>)` if the error is of type T, otherwise returns `Err(self)`.
    /// 
    /// # Example
    /// ```
    /// use areamy::error::{AnyErr, Error, ErrorKind};
    /// use std::fmt;
    /// 
    /// #[derive(Debug)]
    /// struct TensorError;
    /// impl fmt::Display for TensorError {
    ///     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result { write!(f, "TensorError") }
    /// }
    /// impl std::error::Error for TensorError {}
    /// impl AnyErr for TensorError {}
    /// 
    /// #[derive(Debug)]  
    /// struct ChannelDisconnected;
    /// impl fmt::Display for ChannelDisconnected {
    ///     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result { write!(f, "ChannelDisconnected") }
    /// }
    /// impl std::error::Error for ChannelDisconnected {}
    /// impl AnyErr for ChannelDisconnected {}
    /// 
    /// fn handle_error(error: Error) {
    ///     // You can chain multiple attempts:
    ///     match error.downcast_any::<TensorError>() {
    ///         Ok(tensor_err) => {
    ///             // handle tensor error with owned value
    ///             println!("Got tensor error: {:?}", tensor_err);
    ///         }
    ///         Err(e) => match e.downcast_any::<ChannelDisconnected>() {
    ///             Ok(_) => {
    ///                 // handle disconnection
    ///                 println!("Channel disconnected");
    ///                 return;
    ///             }
    ///             Err(e) => {
    ///                 // handle other errors
    ///                 eprintln!("Worker error: {}", e);
    ///             }
    ///         }
    ///     }
    /// }
    /// ```
    pub fn downcast_any<T: AnyErr + 'static>(self) -> Result<Box<T>, Self> {
        // 1. Use a non-consuming guard clause. If the type doesn't match,
        //    return the original `self` without any mutations.
        if !self.is::<T>() {
            return Err(self);
        }

        // 2. We now know the type matches and `self.kind` is `ErrorKind::Any`.
        //    We can safely move out the inner `any_err` and downcast it.
        match self.kind {
            ErrorKind::Any(any_err) => {
                // 3. The downcast is guaranteed to succeed. Using `unreachable!` is
                //    more semantically correct than `expect()` because this branch
                //    is logically impossible.
                match (any_err as Box<dyn Any>).downcast::<T>() {
                    Ok(boxed) => Ok(boxed),
                    Err(_) => unreachable!("The type was already checked with `self.is::<T>()`"),
                }
            }
            // 4. This branch is also unreachable because of the guard clause.
            _ => unreachable!("The type was already checked with `self.is::<T>()`"),
        }
    }
    
    /// Check if the error contains an Any error of a specific type.
    /// 
    /// # Example
    /// ```
    /// use areamy::error::{AnyErr, Error};
    /// use std::fmt;
    /// 
    /// #[derive(Debug)]
    /// struct ChannelDisconnected;
    /// impl fmt::Display for ChannelDisconnected {
    ///     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result { write!(f, "ChannelDisconnected") }
    /// }
    /// impl std::error::Error for ChannelDisconnected {}
    /// impl AnyErr for ChannelDisconnected {}
    /// 
    /// fn handle_error(error: &Error) {
    ///     if error.is::<ChannelDisconnected>() {
    ///         // Channel disconnected - return gracefully
    ///         println!("Channel disconnected");
    ///         return;
    ///     }
    ///     // For all other errors, log as worker error
    ///     eprintln!("Worker error: {}", error);
    /// }
    /// ```
    pub fn is<T: AnyErr + 'static>(&self) -> bool {
        match &self.kind {
            ErrorKind::Any(any_err) => {
                (any_err.as_ref() as &dyn Any).downcast_ref::<T>().is_some()
            }
            _ => false,
        }
    }
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
/// Supports format arguments like `println!`.
/// 
/// # Examples
/// ```
/// use areamy::fatal;
/// 
/// // Simple message
/// let error = fatal!("Something went wrong");
/// 
/// // With format arguments
/// let user_id = 42;
/// let error = fatal!("User {} not found", user_id);
/// 
/// // With multiple arguments
/// let error = fatal!("Failed to connect to {}:{} after {} attempts", "localhost", 8080, 3);
/// ```
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
    ($fmt:expr, $($arg:tt)*) => {
        $crate::error::Error {
            kind: $crate::error::ErrorKind::Fatal(format!($fmt, $($arg)*)),
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
    use super::*;
    use std::fmt;

    // Define some test error types for our tests
    #[derive(Debug)]
    struct TestErrorA {
        message: String,
    }

    impl fmt::Display for TestErrorA {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "TestErrorA: {}", self.message)
        }
    }

    impl std::error::Error for TestErrorA {}
    impl AnyErr for TestErrorA {}

    #[derive(Debug)]
    struct TestErrorB {
        code: i32,
    }

    impl fmt::Display for TestErrorB {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "TestErrorB: {}", self.code)
        }
    }

    impl std::error::Error for TestErrorB {}
    impl AnyErr for TestErrorB {}

    fn inside_a_fn() {
        let error: Result<(), Box<dyn std::error::Error>> = fatal!("Invalid").into();
        error.unwrap()
    }

    #[ignore]
    #[test]
    fn error_make_stacktrace() {
        inside_a_fn()
    }

    #[test]
    fn test_downcast_any_success() {
        let test_err = TestErrorA {
            message: "test message".to_string(),
        };
        let error = any_err!(Box::new(test_err) as Box<dyn AnyErr>);

        match error.downcast_any::<TestErrorA>() {
            Ok(downcasted) => {
                assert_eq!(downcasted.message, "test message");
            }
            Err(_) => panic!("Should have successfully downcasted to TestErrorA"),
        }
    }

    #[test]
    fn test_downcast_any_failure() {
        let test_err = TestErrorA {
            message: "test message".to_string(),
        };
        let error = any_err!(Box::new(test_err) as Box<dyn AnyErr>);

        match error.downcast_any::<TestErrorB>() {
            Ok(_) => panic!("Should not have downcasted to TestErrorB"),
            Err(original_error) => {
                // Should get back the original error
                assert!(original_error.is::<TestErrorA>());
            }
        }
    }

    #[test]
    fn test_downcast_any_chaining() {
        let test_err = TestErrorB { code: 42 };
        let error = any_err!(Box::new(test_err) as Box<dyn AnyErr>);

        // Chain multiple downcast attempts
        match error.downcast_any::<TestErrorA>() {
            Ok(_) => panic!("Should not have downcasted to TestErrorA"),
            Err(e) => match e.downcast_any::<TestErrorB>() {
                Ok(downcasted) => {
                    assert_eq!(downcasted.code, 42);
                }
                Err(_) => panic!("Should have successfully downcasted to TestErrorB in chain"),
            }
        }
    }

    #[test]
    fn test_downcast_any_fatal_error() {
        let error = fatal!("This is a fatal error");

        match error.downcast_any::<TestErrorA>() {
            Ok(_) => panic!("Should not downcast fatal error"),
            Err(original_error) => {
                // Should get back the original fatal error
                match original_error.kind {
                    ErrorKind::Fatal(ref msg) => {
                        assert_eq!(msg, "This is a fatal error");
                    }
                    _ => panic!("Should be a fatal error"),
                }
            }
        }
    }

    #[test]
    fn test_is_method() {
        let test_err_a = TestErrorA {
            message: "test".to_string(),
        };
        let error = any_err!(Box::new(test_err_a) as Box<dyn AnyErr>);

        assert!(error.is::<TestErrorA>());
        assert!(!error.is::<TestErrorB>());

        let fatal_error = fatal!("fatal");
        assert!(!fatal_error.is::<TestErrorA>());
        assert!(!fatal_error.is::<TestErrorB>());
    }

    #[test]
    fn test_fatal_macro_formatting() {
        // Test simple message
        let error = fatal!("Simple message");
        match error.kind {
            ErrorKind::Fatal(ref msg) => {
                assert_eq!(msg, "Simple message");
            }
            _ => panic!("Should be a fatal error"),
        }

        // Test format arguments
        let user_id = 42;
        let error = fatal!("User {} not found", user_id);
        match error.kind {
            ErrorKind::Fatal(ref msg) => {
                assert_eq!(msg, "User 42 not found");
            }
            _ => panic!("Should be a fatal error"),
        }

        // Test multiple format arguments
        let host = "localhost";
        let port = 8080;
        let attempts = 3;
        let error = fatal!("Failed to connect to {}:{} after {} attempts", host, port, attempts);
        match error.kind {
            ErrorKind::Fatal(ref msg) => {
                assert_eq!(msg, "Failed to connect to localhost:8080 after 3 attempts");
            }
            _ => panic!("Should be a fatal error"),
        }
    }
}
