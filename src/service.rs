pub mod customer{
    use oracle::pool::Pool;
    use oracle::ResultSet;
    use rayon::iter::{IntoParallelRefIterator, ParallelBridge};
    use rdkafka::producer::{DefaultProducerContext, ThreadedProducer};
    use crate::customer::Customer;
    use crate::database::query::{BATCH_SIZE, PARTITIONS_NO};
    use crate::kafka::consumer::produce;
    use crate::template::format::get_customer_notification;
    use rayon::prelude::*;

    use rayon::iter::ParallelIterator;
    use rayon::{ThreadBuilder, ThreadPool, ThreadPoolBuilder};
    pub fn send_notification(producer:&ThreadedProducer<DefaultProducerContext>,connection_pool: &Pool){
            let count = 1000000;
            let pages = count / BATCH_SIZE;
            println!("total number of pages {}", pages);
            let range: Vec<i32> = (0..pages).collect();

            let now = chrono::offset::Local::now();
            println!("{:?}", now);
            range.par_iter().for_each(|page| {
                let connection = (*connection_pool).get().expect("failed to create connect");
                let result_set= connection.query("SELECT ROWNUM,CUSTOMER.* FROM Customer order by ROWNUM OFFSET :1 ROWS FETCH NEXT :2 ROWS ONLY ",
                                                 &[&(page * &BATCH_SIZE), &BATCH_SIZE]).expect("expected row");

                result_set.map(|multirow|
                    return multirow.map(|row| Customer::map_to_customer(row)).expect("invalid Customer")
                )
                    .collect::<Vec<Customer>>()
                    .iter()
                    .par_bridge()
                    .for_each(|customer| {
                        let notification = get_customer_notification(&customer);
                        produce(notification,(page) % PARTITIONS_NO,producer)
                    });
                connection.close().expect("error closing connection");

            });

            println!("{:?}", chrono::offset::Local::now() - now);
            println!("done")
        }
}