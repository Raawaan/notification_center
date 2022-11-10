use oracle::Row;
use serde::Serialize;

#[derive(Serialize)]
pub struct Customer {
    id:i32,
    name: String,
    msisdn: String,
    segment: String,
    balance: i64,
}
impl Customer {
    pub fn map_to_customer(row:Row) -> Customer {
        Customer {
            id: row.get("ROWNUM").expect("expect Customer id"),
            name: row.get("name").expect("expect Customer name"),
            msisdn: row.get("msisdn").expect("expect Customer msisdn"),
            segment: row.get("segment").expect("expect Customer segment"),
            balance: row.get("balance").expect("expect Customer balance"),
        }
    }
}