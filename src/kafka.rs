pub mod consumer {
    use std::str;
    use std::time::Duration;
    use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
    use rdkafka::consumer::{BaseConsumer, Consumer, DefaultConsumerContext};
    use rdkafka::error::KafkaError;
    use rdkafka::message::Message;
    use rdkafka::producer::{BaseProducer, BaseRecord, DefaultProducerContext};

    pub fn consume_and_execute(brokers: &str, group_id: &str, topics: &[&str], execute: fn()) {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("group.id", group_id)
            .set("bootstrap.servers", brokers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create_with_context(DefaultConsumerContext)
            .expect("Consumer creation failed");
        consumer
            .subscribe(&topics.to_vec())
            .expect("Can't subscribe to specified topics");

        loop {
            for kafka_result in consumer.poll(Duration::from_millis(1)).iter() {
                match kafka_result {
                    Ok(borrowed_msg) => {
                        match borrowed_msg.payload() {
                            None => { println!("got none") }
                            Some(payload) => {
                                let msg = str::from_utf8(payload).unwrap();
                                println!("got msg: {}", msg);
                                execute()
                            }
                        }
                    }
                    Err(err) => { println!("{}", err) }
                }
            };
        }
    }

    // pub fn produce(brokers: &str, group_id: &str, topic: &str, msg: &Vec<String>) {
    //     let producer: BaseProducer = ClientConfig::new()
    //         .set("group.id", group_id)
    //         .set("bootstrap.servers", brokers)
    //         .set("enable.partition.eof", "false")
    //         .set("session.timeout.ms", "6000")
    //         .create_with_context(DefaultProducerContext)
    //         .expect("producer creation failed");
    //     let base_record = BaseRecord::to(topic).payload(msg);
    //     match producer.send(base_record) {
    //         Ok(_) => { println!("msg produced ") }
    //         Err(error) => { println!("error producing msg {}", error) }
    //     }
    // }
}