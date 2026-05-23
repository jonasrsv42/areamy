//! Shared test fixtures for the bundle module.

use crate::Workable;
use crate::connect::marker::Connection;
use crate::error::Error;
use crate::thread::ThreadId;
use crate::{closed, fatal};
use std::marker::PhantomData;

#[derive(Debug, Clone)]
pub struct ThreadA;
impl ThreadId for ThreadA {}

#[derive(Debug, Clone)]
pub struct ThreadB;
impl ThreadId for ThreadB {}

pub struct ImmediateClose<T>(PhantomData<T>);
impl<T> ImmediateClose<T> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}
impl<T> Connection for ImmediateClose<T> {}
impl<T: ThreadId> Workable for ImmediateClose<T> {
    type ThreadId = T;
    fn work(&mut self) -> Result<(), Error> {
        Err(closed!())
    }
}

pub struct WorkError<T>(PhantomData<T>);
impl<T> WorkError<T> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}
impl<T> Connection for WorkError<T> {}
impl<T: ThreadId> Workable for WorkError<T> {
    type ThreadId = T;
    fn work(&mut self) -> Result<(), Error> {
        Err(fatal!("work failed"))
    }
}

pub struct Panicker<T>(PhantomData<T>);
impl<T> Panicker<T> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}
impl<T> Connection for Panicker<T> {}
impl<T: ThreadId> Workable for Panicker<T> {
    type ThreadId = T;
    fn work(&mut self) -> Result<(), Error> {
        panic!("intentional panic");
    }
}
