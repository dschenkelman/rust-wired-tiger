extern crate libc;

use wiredtiger_def::{WT_CONNECTION,WT_SESSION,WT_CURSOR,
  wiredtiger_open,wiredtiger_strerror};

use self::libc::{c_int, c_char};
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

/// Opens a Wired Tiger connection and returns a new `Connection`.
/// # Examples
/// ```
/// match wiredtiger::open() {
///   Ok(mut connection) => {
///     // work with connection
///   }
///   Err(message) => {
///     // handle error
///   }
/// }
/// ```
/// # Failures
/// The function returns `Err(message)` if the connection failed to open.
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

/// Represents a Wired Tiger connection.
pub struct Connection {
  wt_con: *mut WT_CONNECTION
}

/// Represents a Wired Tiger session.
pub struct Session {
  wt_session: *mut WT_SESSION
}

/// Represents a Wired Tiger cursor.
pub struct Cursor {
  wt_cursor: *mut WT_CURSOR
}

/// A key value pair that can be used for Wired Tiger tables with that structure
pub struct KeyValuePair {
  pub key: String,
  pub value: String
}

impl Drop for Connection {
  /// When the connection is dropped the underlying connection is closed.
  /// All other components related to the connection are no longer usable.
  fn drop(&mut self) {
    unsafe{
      match (*self.wt_con).close {
        Some(close) => { close(self.wt_con, ptr::null_mut()); }
        None => ()
      };
    }
  }
}

impl Connection {
  /// Opens a Wired Tiger session and returns a `Session`.
  /// # Examples
  /// ```
  /// match wiredtiger::open().unwrap().open_session() {
  ///   Ok(session) => {
  ///     // work with session
  ///   },
  ///   Err(message) => {
  ///     // handle error
  ///   }
  /// }
  /// ```
  /// # Failures
  /// The function returns `Err(message)` if the session failed to open.
  pub fn open_session(&mut self) -> Result<Session, String>{
    unsafe{
      match (*self.wt_con).open_session {
        Some(open_session) => {
          let mut session: *mut WT_SESSION = mem::uninitialized();
          let ret = open_session(self.wt_con, ptr::null_mut(),
            ptr::null_mut(), &mut session);

          if ret != 0 {
            return Err(get_error(ret));
          }

          Ok(Session{
            wt_session: session
          })
        },
        None => Err("Failed to get open_session".to_string())
      }
    }
  }
}

impl Cursor {
  fn set_key(&mut self, key: &CString) -> Result<(), String>{
    unsafe {
      match(*self.wt_cursor).set_key {
        Some(set_key) => {
          set_key(self.wt_cursor, key.as_ptr());
          Ok(())
        }
        None => Err("Failed to get set_key".to_string())
      }
    }
  }

  fn set_value(&mut self, value: &CString) -> Result<(), String>{
    unsafe {
      match(*self.wt_cursor).set_value {
        Some(set_value) => {
          set_value(self.wt_cursor, value.as_ptr());
          Ok(())
        }
        None => Err("Failed to get set_value".to_string())
      }
    }
  }

  fn insert(&mut self) -> Result<(), String>{
    unsafe {
      match(*self.wt_cursor).insert {
        Some(insert) => {
          let ret = insert(self.wt_cursor);

          if ret != 0 {
            return Err(get_error(ret));
          }

          Ok(())
        }
        None => Err("Failed to get insert".to_string())
      }
    }
  }

  /// Inserts the `value` for the given `key` in the table related to the `Cursor`.
  /// # Examples
  /// ```
  /// cursor.insert_pair("1", "John Doe");
  /// ```
  /// # Failures
  /// The function returns `Err(message)` if:
  /// * The `key` fails to be set for the cursor
  /// * The `value` fails to be set for the cursor
  /// * The pair fail to be inserted
  pub fn insert_pair(&mut self, key: &str, value: &str) -> Result<(), String>{
    let k = c_str(key);
    let v = c_str(value);
    try!(self.set_key(&k));
    try!(self.set_value(&v));
    try!(self.insert());
    Ok(())
  }

  /// Places the cursor at its initial position
  /// # Examples
  /// ```
  /// cursor.reset();
  /// ```
  /// # Failures
  /// The function returns `Err(message)` if the cursor could not be reset
  pub fn reset(&mut self) -> Result<(), String>{
    unsafe {
      match(*self.wt_cursor).reset {
        Some(reset) => {
          let ret = reset(self.wt_cursor);

          if ret != 0 {
            return Err(get_error(ret));
          }

          Ok(())
        }
        None => Err("Failed to get reset".to_string())
      }
    }
  }

  fn next(&mut self) -> Result<(), String>{
    unsafe {
      match(*self.wt_cursor).next {
        Some(next) => {
          let ret = next(self.wt_cursor);

          if ret != 0 {
            return Err(get_error(ret));
          }

          Ok(())
        }
        None => Err("Failed to get next".to_string())
      }
    }
  }

  fn get_key(&mut self) -> Result<String, String>{
    unsafe {
      match(*self.wt_cursor).get_key {
        Some(get_key) => {
          let key: *mut c_char = mem::uninitialized();
          let ret = get_key(self.wt_cursor, &key);

          if ret != 0 {
            return Err(get_error(ret));
          }

          Ok(string_from_ptr(key))
        }
        None => Err("Failed to get get_key".to_string())
      }
    }
  }

  fn get_value(&mut self) -> Result<String, String>{
    unsafe {
      match(*self.wt_cursor).get_value {
        Some(get_value) => {
          let value: *mut c_char = mem::uninitialized();
          let ret = get_value(self.wt_cursor, &value);

          if ret != 0 {
            return Err(get_error(ret));
          }

          Ok(string_from_ptr(value))
        }
        None => Err("Failed to get get_value".to_string())
      }
    }
  }
}

impl Session {
  /// Creates a table named `name` to hold key/value pairs.
  /// # Examples
  /// ```
  /// session.create_table("users");
  /// ```
  /// # Failures
  /// The function returns `Err(message)` if the table failed to be created.
  pub fn create_table(&mut self, name: &str) -> Result<(), String> {
    unsafe {
      match(*self.wt_session).create {
        Some(create) => {
          let full_command = &format!("table:{0}", name);
          let ret = create(self.wt_session,
            c_str(full_command).as_ptr(),
            c_str("key_format=S,value_format=S").as_ptr());

          if ret != 0 {
            return Err(get_error(ret));
          }

          Ok(())
        }
        None => Err("Failed to get create".to_string())
      }
    }
  }

  /// Opens a `Cursor` for the table `table_name` and returns it.
  /// # Examples
  /// ```
  /// session.open_cursor("users");
  /// ```
  /// # Failures
  /// The function returns `Err(message)` if the cursor could not be opened.
  pub fn open_cursor(&mut self, table_name: &str) -> Result<Cursor, String>{
    unsafe {
      match(*self.wt_session).open_cursor {
        Some(open_cursor) => {
          let mut cursor: *mut WT_CURSOR = mem::uninitialized();
          let cursor_uid = &format!("table:{0}", table_name);
          let ret = open_cursor(self.wt_session,
            c_str(cursor_uid).as_ptr(),
            ptr::null_mut(),
            ptr::null_mut(),
            &mut cursor);

          if ret != 0 {
            return Err(get_error(ret));
          }

          Ok(Cursor{
            wt_cursor: cursor
          })
        }
        None => Err("Failed to get open_cursor".to_string())
      }
    }
  }
}

impl Iterator for Cursor {
  type Item = KeyValuePair;
  fn next(&mut self) -> Option<KeyValuePair> {
    match self.next() {
      Ok(_) => {
        match (self.get_key(), self.get_value()){
          (Ok(key), Ok(value)) => Some(KeyValuePair{
            key: key,
            value: value
          }),
          _ => None
        }
      }
      Err(_) => None
    }
  }
}

impl Drop for Cursor {
  /// When the connection is dropped the underlying connection is closed.
  /// All other components related to the connection are no longer usable.
  fn drop(&mut self) {
    unsafe{
      match (*self.wt_cursor).close {
        Some(close) => { close(self.wt_cursor); }
        None => ()
      };
    }
  }
}

impl Drop for Session {
  /// When the connection is dropped the underlying connection is closed.
  /// All other components related to the connection are no longer usable.
  fn drop(&mut self) {
    unsafe{
      match (*self.wt_session).close {
        Some(close) => { close(self.wt_session, ptr::null_mut()); }
        None => ()
      };
    }
  }
}