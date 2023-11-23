/// ///          _|                  _|                  _|  _|
/// ///  _|_|_|  _|_|_|      _|_|_|  _|    _|_|_|    _|_|_|  _|_|_|
/// ///_|    _|  _|    _|  _|    _|  _|  _|    _|  _|    _|  _|    _|
/// ///_|    _|  _|    _|  _|    _|  _|  _|    _|  _|    _|  _|    _|
/// ///  _|_|_|  _|    _|    _|_|_|  _|    _|_|_|    _|_|_|  _|_|_|
/// ///      _|
/// ///  _|_|
/// ///

#[macro_use]
extern crate log;

mod config;
mod core;
mod dec;
pub mod error;
mod gc;
mod ghaladb;
mod keys;
mod utils;
mod vlog;
pub use crate::ghaladb::GhalaDB;
