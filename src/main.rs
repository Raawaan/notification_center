mod kafka;
mod database;
mod service;
mod template;
mod customer;

use rdkafka::util::get_rdkafka_version;
use crate::kafka::consumer::consume_and_execute;
use crate::kafka::consumer::create_producer;
// use crate::database::query::get_data;
use crate::database::connection::conn_oracle;
use std::thread;
use crate::service::customer::send_notification;

fn main() {
    let (version_n, version_s) = get_rdkafka_version();

    println!("rd_kafka_version: {}, {}", version_n, version_s);
    let producer = create_producer();
    let connection_pool = conn_oracle();

    consume_and_execute("127.0.0.1:9093",
                        "group-id",
                        &vec!["trigger-campaign"],
                        send_notification,
                        producer,
                        connection_pool
    );
}
