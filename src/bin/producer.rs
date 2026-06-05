use rabbitmq_stream_client::{Environment, types::Message};
use tracing_subscriber::FmtSubscriber;
use tracing::info;

use rabbit_client::config::Config;

#[tokio::main]
async fn main() {
    // setup tracing first
    let subscriber = FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    info!("Producer starting");
    let config = Config::default();

    let env = Environment::builder()
        .host(&config.rabbitmq_host)
        .port(config.rabbitmq_port)
        .username(&config.rabbitmq_user)
        .password(&config.rabbitmq_password)
        .build().await.expect("RabbitMQ should be available");

    let producer = env.producer().build(&config.rabbitmq_stream).await.expect("Stream should be available");

    info!("Starting production loop");
    for i in 0..100_000 {
        let message = Message::builder()
            .body(format!("Hello RabbitMQ! Message no {i}"))
            .build();
        producer.send_with_confirm(message).await.expect("Message could not be sent");
        if i % 10_000 == 0 {
            info!("Reached {i}");
        }
    }
    info!("Production loop ended");
    producer.close().await.unwrap();
}
