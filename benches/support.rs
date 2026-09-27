//! Shared bench routines and payloads.
// Each bench uses a subset.
#![allow(dead_code)]

use areamy::biunion::{Left, Right};
use areamy::error::Error;
use areamy::{BiunionRoutine, Flush, LineRoutine, Next, Send as RoutineSend};
use std::collections::VecDeque;

/// Samples in one audio-sized frame; large enough that a clone is a real memcpy.
pub const FRAME: usize = 4096;

/// Forwards every input unchanged.
pub struct Pass<T> {
    output: VecDeque<T>,
}

impl<T> Pass<T> {
    pub fn new() -> Self {
        Pass {
            output: VecDeque::new(),
        }
    }
}

impl<T> RoutineSend<T> for Pass<T> {
    fn send(&mut self, message: T) -> Result<(), Error> {
        self.output.push_back(message);
        Ok(())
    }
}

impl<T> Next<T> for Pass<T> {
    fn next(&mut self) -> Result<Option<T>, Error> {
        Ok(self.output.pop_front())
    }
}

impl<T> Flush for Pass<T> {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl<T: Clone + Send + Sync> LineRoutine<T, T> for Pass<T> {}

/// Forwards inputs from both sides unchanged.
pub struct Merge<T> {
    output: VecDeque<T>,
}

impl<T> Merge<T> {
    pub fn new() -> Self {
        Merge {
            output: VecDeque::new(),
        }
    }
}

impl<T> RoutineSend<T, Left> for Merge<T> {
    fn send(&mut self, message: T) -> Result<(), Error> {
        self.output.push_back(message);
        Ok(())
    }
}

impl<T> RoutineSend<T, Right> for Merge<T> {
    fn send(&mut self, message: T) -> Result<(), Error> {
        self.output.push_back(message);
        Ok(())
    }
}

impl<T> Next<T> for Merge<T> {
    fn next(&mut self) -> Result<Option<T>, Error> {
        Ok(self.output.pop_front())
    }
}

impl<T> Flush for Merge<T> {
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl<T: Clone + Send + Sync> BiunionRoutine<T, T, T> for Merge<T> {}

pub fn frame() -> Vec<f32> {
    vec![0.5; FRAME]
}
