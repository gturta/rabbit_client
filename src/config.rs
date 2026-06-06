#[derive(Clone)]
pub struct Config {
    pub rabbitmq_host: String,
    pub rabbitmq_port: u16,
    pub rabbitmq_user: String,
    pub rabbitmq_password: String,
    pub rabbitmq_stream: String,
    pub push_url: String,
    pub local_storage: String,
}

impl Default for Config{
    fn default() -> Self {
        Self { 
            rabbitmq_host: String::from("127.0.0.1"),
            rabbitmq_port: 5552,
            rabbitmq_user: String::from("guest"),
            rabbitmq_password: String::from("guest"),
            rabbitmq_stream: String::from("stream01"),
            push_url: String::from("http://localhost:7878/push"),
            local_storage: "/tmp/rabbit_consumer_storage".to_string(),
        }
    }
}
