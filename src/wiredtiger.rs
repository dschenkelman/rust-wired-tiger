use wiredtiger_def::{WT_CONNECTION,WT_SESSION,WT_CURSOR,
  wiredtiger_open,wiredtiger_strerror};

use libc::{c_int, c_char};
use std::ffi::{CStr,CString};
use std::{ptr,mem,str};
use std::ops::{Drop};

unsafe fn string_from_ptr(ptr: *const c_char) -> String {
  let slice = CStr::from_ptr(ptr);
  str::from_utf8(slice.to_bytes()).unwrap().to_string()
}

unsafe fn get_error(code: c_int) -> String {
  string_from_ptr(wiredtiger_strerror(code))
}

fn c_str(val: &str) -> CString{
  CString::new(val).unwrap()
}

pub fn open() -> Result<Connection, String> {
  let action = c_str("create");
  unsafe {
    let mut connection: *mut WT_CONNECTION = mem::uninitialized();

    let ret = wiredtiger_open(ptr::null(),
      ptr::null_mut(),
      action.as_ptr(),
      &mut connection);

    if ret != 0{
      return Err(get_error(ret));
    }

    Ok(Connection{
      wt_con: connection
    })
  }
}

pub struct Connection {
  wt_con: *mut WT_CONNECTION
}

impl Drop for Connection {
  fn drop(&mut self) {
    unsafe{
      match (*self.wt_con).close {
        Some(f) => { f(self.wt_con, ptr::null_mut()); }
        None => ()
      };
    }
  }
}