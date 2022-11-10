pub mod consumer {
    use std::str;
    use std::time::Duration;
    use oracle::pool::Pool;
    use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
    use rdkafka::consumer::{BaseConsumer, Consumer, DefaultConsumerContext};
    use rdkafka::error::KafkaError;
    use rdkafka::message::Message;
    use rdkafka::producer::{BaseProducer, BaseRecord, DefaultProducerContext, ThreadedProducer};

    pub fn consume_and_execute(brokers: &str, group_id: &str, topics: &[&str], execute: fn(&ThreadedProducer<DefaultProducerContext>,connection_pool: &Pool), producer: ThreadedProducer<DefaultProducerContext>, connection_pool: Pool) {
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
                                execute(&producer,&connection_pool)
                            }
                        }
                    }
                    Err(err) => { println!("{}", err) }
                }
            };
        }
    }

    pub fn create_producer() -> ThreadedProducer<DefaultProducerContext> {
        let producer: ThreadedProducer<DefaultProducerContext> = ClientConfig::new()
            .set("bootstrap.servers", "127.0.0.1:9093")
            .create_with_context::<DefaultProducerContext, ThreadedProducer<_>>(DefaultProducerContext)
            .expect("producer creation failed");
        producer
    }

    pub fn produce(notification: String, partition: i32, producer: &ThreadedProducer<DefaultProducerContext>) {
        let base_record: BaseRecord<str, String> = BaseRecord::to("customer-msg").payload(&notification).key("key").partition(partition);
        (*producer).send(base_record).expect("error producing msg")
    }
}