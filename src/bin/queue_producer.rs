use tracing_subscriber::FmtSubscriber;
use tracing::info;
use lapin::{Connection, ConnectionProperties, options};

use rabbit_client::config::Config;

#[tokio::main]
async fn main() {
    // setup tracing first
    let subscriber = FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    info!("Producer starting");
    let config = Config::default();

    let addr = format!("amqp://{}:{}@{}:{}", config.rabbitmq_user, config.rabbitmq_password, config.rabbitmq_host, config.rabbitmq_amqp_port);

    let connection = Connection::connect(&addr, ConnectionProperties::default()).await.expect("Could not connect to amqp server");
    let channel = connection.create_channel().await.expect("Could not create channel");

    channel.queue_declare(config.rabbitmq_queue.clone().into(),
      options::QueueDeclareOptions::durable(),
      lapin::types::FieldTable::default())
      .await.expect("could not declare queue");

    for i in 0..100_000 {
      let confirm = channel.basic_publish("".into(), config.rabbitmq_queue.clone().into(), options::BasicPublishOptions::default(),
        format!("Hello RabbitMQ! Message no {i}").into_bytes().as_ref(), 
        lapin::BasicProperties::default()).await.expect("Could not publish message");
      confirm.await.expect("Could not confirm published message");      
    }
    info!("Production loop ended");
}
