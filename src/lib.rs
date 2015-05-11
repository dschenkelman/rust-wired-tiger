// Copyright 2015 Damian Schenkelman

#![crate_name = "wiredtiger"]
#![crate_type = "lib"]

extern crate libc;

mod wiredtiger_def;

/// A friendly Rust wrapper for the Wired Tiger C library

pub mod wiredtiger;