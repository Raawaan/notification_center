mod connection {
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
    use oracle::{Connection, SqlValue};
    use oracle::sql_type::ObjectType;
    use oracle::sql_type::OracleType::Object;
    use rayon::prelude::*;
    use crate::database::Customer;
    // use crate::kafka::kafkaconsumer::produce;
    use super::connection::conn_oracle;
    use rayon::iter::ParallelIterator;

    const BATCH_SIZE: i32 = 10000;

    pub fn get_data() {
        let pool = conn_oracle();

        let count = get_total_rows_count(pool.get().expect("failed to create connect"));
        let rem = match count % BATCH_SIZE == 0 {
            true => 0,
            false => 1
        };
        let pages = count / BATCH_SIZE + rem;
        println!("total number of pages {}", pages);
        let range: Vec<i32> = (0..pages).collect();
        let _ = range.par_iter().for_each(|page| {
            println!("fetching page {}", page);
            let connection = pool.get().expect("failed to create connect");
            let result_set = connection.query("SELECT ROWNUM,CUSTOMER.* FROM customer order by ROWNUM OFFSET :1 ROWS FETCH NEXT :2 ROWS ONLY ",
                                              &[&(page * &BATCH_SIZE), &BATCH_SIZE]).expect("expected row");

            let customer_vec: Vec<Customer> = result_set.map(|multirow| {
                let customer = multirow.map(|row| Customer {
                    id: row.get("ROWNUM").expect("expect ROWNUM"),
                    name: row.get("name").expect("expect name"),
                    msisdn: row.get("msisdn").expect("expect msisdn"),
                    segment: row.get("segment").expect("expect segment"),
                    balance: row.get("balance").expect("expect balance"),
                }).expect("invalid customer");
                customer
            }).collect();
            // produce("127.0.0.1:9093",
            //         "group-id","customer-msg",&customer_vec);customer_vec
            connection.close().expect("error closing connection");

            println!("got {} customers page {}", customer_vec.len(), page);
        });
        println!("count of open connection {}", pool.open_count().expect("failed to get open connections"));

        println!("done")
    }

    fn get_total_rows_count(connection: Connection) -> i32 {
        let row = connection.query_row("SELECT COUNT(*) FROM CUSTOMER", &[]).expect("expected row");

        let count: i32 = row.get("COUNT(*)").expect("expected count");
        println!("total rows count {}", count);
        connection.close().expect("error closing connection");
        count
    }
}

struct Customer {
    id:i32,
    name: String,
    msisdn: String,
    segment: String,
    balance: i64,
}