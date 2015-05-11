mod wiredtiger_def;
mod wiredtiger;

fn main() {
  match wiredtiger::open() {
    Ok(mut connection) => {
      connection.open_session().and_then(|mut session|{
        try!(session.create_table("users"));
        let mut cursor = try!(session.open_cursor("users"));
        try!(cursor.insert_pair("1", "John Doe"));
        try!(cursor.insert_pair("2", "Jane Doe"));
        try!(cursor.reset());
        for el in cursor.filter(|kvp| kvp.value.contains("a")) {
          println!("Record {}:{}", el.key, el.value);
        }
        Ok(())
      });
    },
    Err(m) => println!("{:?}", m)
  }
}
