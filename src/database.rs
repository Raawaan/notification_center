use oracle::sql_type::{Collection, FromSql, Object};
use oracle::{Row, SqlValue};
use rdkafka::ClientContext;
use rdkafka::message::DeliveryResult;
use rdkafka::producer::ProducerContext;
use serde::Serialize;

pub mod connection {
    use std::time::Duration;
    use oracle::pool::{Pool, PoolBuilder};

    // Connect to a database.
    pub fn conn_oracle() -> Pool {
        PoolBuilder::new("MOSTAFA",
                         "P00sswd",
                         "//10.237.71.79:1521/orcl_pdb")
            .connection_increment(500)
            .min_connections(100)
            .max_connections(1000)
            .build()
            .expect("failed to create pool")
    }
}


pub mod query {
    use std::time::Duration;
    use oracle::{Connection, Error, ResultSet, Row, SqlValue};
    use oracle::pool::Pool;
    use oracle::sql_type::ObjectType;
    use oracle::sql_type::OracleType::Object;
    use rayon::prelude::*;
    use crate::kafka::consumer::produce;
    use crate::template::format::get_customer_notification;
    use super::connection::conn_oracle;
    use rayon::iter::ParallelIterator;
    use rayon::{ThreadBuilder, ThreadPool, ThreadPoolBuilder};
    use rdkafka::ClientConfig;
    use rdkafka::producer::{BaseProducer, BaseRecord, DefaultProducerContext, Producer, ThreadedProducer};
    use serde::de::value::UsizeDeserializer;
    use crate::customer::Customer;

    pub const BATCH_SIZE: i32 = 10000;
    pub const PARTITIONS_NO: i32 = 5;

    // pubub fn get_data(page: i32, connection_pool: &Pool) ->  ResultSet<Result<oracle::Error,Row>> {
    //
    //         let connection = (*connection_pool).get().expect("failed to create connect");
    //         let result_set: ResultSet<Row> = connection.query("SELECT ROWNUM,CUSTOMER.* FROM Customer order by ROWNUM OFFSET :1 ROWS FETCH NEXT :2 ROWS ONLY ",
    //                                                           &[&(&page * &BATCH_SIZE), &BATCH_SIZE]).expect("expected row");
    //         connection.close().expect("error closing connection");
    //     return result_set.collect()
    // }

    fn get_total_rows_count(connection: Connection) -> i32 {
        let row = connection.query_row("SELECT COUNT(*) FROM CUSTOMER", &[]).expect("expected row");
        let count: i32 = row.get("COUNT(*)").expect("expected count");
        println!("total rows count {}", count);
        connection.close().expect("error closing connection");
        count
    }
}

//
// impl FromSql for Customer{
//     fn from_sql(val: &SqlValue) -> oracle::Result<Customer> {
//       let k= val.get::<Collection>().expect("");
//     Ok(
//         Customer{
//
//             // id: row.get("ROWNUM").expect("expect Customer id"),
//             //                 name: row.get("name").expect("expect Customer name"),
//             //                 msisdn: row.get("msisdn").expect("expect Customer msisdn"),
//             //                 segment: row.get("segment").expect("expect Customer segment"),
//             //                 balance: row.get("balance").expect("expect Customer balance"),
//             id: 0,
//             name: "".to_string(),
//             msisdn: "".to_string(),
//             segment: "".to_string(),
//             balance: 0
//         })
//
//        }
// }


// let _= result_set.map( |multirow| {
//         let Customer = multirow.map(|row| {
//             Customer {
//                 id: row.get("ROWNUM").expect("expect Customer id"),
//                 name: row.get("name").expect("expect Customer name"),
//                 msisdn: row.get("msisdn").expect("expect Customer msisdn"),
//                 segment: row.get("segment").expect("expect Customer segment"),
//                 balance: row.get("balance").expect("expect Customer balance"),
//             }
//         }).expect("invalid Customer");
//     let notification = get_customer_notification(&Customer);
//         let base_record: BaseRecord<str, String> = BaseRecord::to("Customer-msg").payload(&notification).key("key").partition(*page % PARTITIONS_NO);
//         producer.send(base_record).expect("error producing msg")
// }).collect::<()>();

// println!("producing took {:?}", chrono::offset::Local::now()-producingNow);