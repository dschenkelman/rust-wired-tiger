extern crate libc;

use libc::{c_int, c_char};
use wiredtiger::{WT_CONNECTION,WT_SESSION,WT_CURSOR,
  wiredtiger_open,wiredtiger_strerror};
use std::ffi::{CString,CStr};
use std::ptr;
use std::mem;
use std::{i32,str};

mod wiredtiger;

#[link(name = "wiredtiger")]
extern{
}

unsafe fn string_from_ptr(ptr: *mut c_char) -> String {
  let slice = CStr::from_ptr(ptr);
  str::from_utf8(slice.to_bytes()).unwrap().to_string()
}

pub fn get_error(code: i32) -> String {
  let slice = unsafe {
    CStr::from_ptr(wiredtiger_strerror(code))
  };

  str::from_utf8(slice.to_bytes()).unwrap().to_string()
}

fn c_str(val: &str) -> CString{
  CString::new(val).unwrap()
}

fn main() {
  let action = c_str("create");
  let mut ret: c_int;
  // let home = CString::new("create").unwrap();
  unsafe {
    let mut connection: *mut WT_CONNECTION = mem::uninitialized();
    ret = wiredtiger_open(ptr::null(),
      ptr::null_mut(),
      action.as_ptr(),
      &mut connection);
    if ret != 0{
      println!("{:?}", get_error(ret));
    }

    let mut session: *mut WT_SESSION = mem::uninitialized();
    ret = (*connection).open_session.unwrap()(connection,
      ptr::null_mut(), ptr::null_mut(), &mut session);
    if ret != 0{
      println!("{:?}", get_error(ret));
    }

    ret = (*session).create.unwrap()(session,
      c_str("table:access").as_ptr(),
      c_str("key_format=S,value_format=S").as_ptr());
    if ret != 0{
      println!("{:?}", get_error(ret));
    }

    let mut cursor: *mut WT_CURSOR = mem::uninitialized();
    ret = (*session).open_cursor.unwrap()(session, c_str("table:access").as_ptr(),
      ptr::null_mut(),
      ptr::null_mut(),
      &mut cursor);

    if ret != 0{
      println!("{:?}", get_error(ret));
    }

    let key = c_str("key 1");
    let val = c_str("value 1");

    (*cursor).set_key.unwrap()(cursor, key.as_ptr());
    (*cursor).set_value.unwrap()(cursor, val.as_ptr());
    ret = (*cursor).insert.unwrap()(cursor);

    if ret != 0{
      println!("{:?}", get_error(ret));
    }

    ret = (*cursor).reset.unwrap()(cursor);

    if ret != 0{
      println!("{:?}", get_error(ret));
    }

    let key: *mut c_char = mem::uninitialized();
    let value: *mut c_char = mem::uninitialized();

    while (*cursor).next.unwrap()(cursor) == 0 {
      (*cursor).get_key.unwrap()(cursor, &key);
      (*cursor).get_value.unwrap()(cursor, &value);

      let key_string = string_from_ptr(key);
      let value_string = string_from_ptr(value);

      println!("Record {}:{}", key_string, value_string)
    }

    ret = (*connection).close.unwrap()(connection, ptr::null_mut());

    if ret != 0{
      println!("{:?}", get_error(ret));
    }
  }

  println!("Hello, world!");
}
