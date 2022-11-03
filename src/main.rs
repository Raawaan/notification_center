mod kafka;
mod database;
mod service;

use rdkafka::util::get_rdkafka_version;
use crate::kafka::consumer::consume_and_execute;
use crate::database::query::get_data;
use std::thread;

fn main() {
    let (version_n, version_s) = get_rdkafka_version();

    println!("rd_kafka_version: {}, {}", version_n, version_s);
    get_data();

    consume_and_execute("127.0.0.1:9093",
                        "group-id",
                        &vec!["trigger-campaign"],
                        get_data  );
}
