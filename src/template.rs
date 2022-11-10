use serde::Serialize;

use tinytemplate::TinyTemplate;
use std::error::Error;


static TEMPLATE: &'static str = "Hello {name} your current balance for {msisdn} is {balance}!";

pub mod format {
    use rdkafka::consumer::Consumer;
    use tinytemplate::TinyTemplate;
    use crate::customer::Customer;
    use crate::template::{TEMPLATE};

    pub fn get_customer_notification(customer:&Customer) ->String{
        let mut tt = TinyTemplate::new();
        tt.add_template("notification", TEMPLATE).expect("add tmp error");
        tt.render("notification", &customer).expect("failed to render Customer notification")
    }
}