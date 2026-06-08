use lapin::{Connection, ConnectionProperties, options};
use tracing_subscriber::FmtSubscriber;
use tracing::info;
use tokio::sync::mpsc;
use futures::stream::StreamExt;

use rabbit_client::config::Config;
use rabbit_client::error::AppError;

struct MyMessage {
    data: String,
}

#[tokio::main]
async fn main() {
    // setup tracing first
    let subscriber = FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).unwrap();
    info!("Consumer starting");
    
    let config = Config::default();
    let config_clone = config.clone();


    let (tx, rx) = mpsc::channel(100);

    tokio::select! {
        res = rabbitmq_reader(&config_clone, tx) => {info!("rabbitmq_reader ended: {res:?}");},
        res = push_sender(&config, rx) => {info!("push_sender ended: {res:?}");},
    };
}


async fn push_sender(config: &Config, mut rx: mpsc::Receiver<MyMessage>) -> Result<(), AppError> {
    let client = reqwest::Client::new();

    while let Some(message) = rx.recv().await {
        client.post(config.push_url.clone())
            .body(message.data)
            .send().await?;
    }
    Ok(())
}

async fn rabbitmq_reader(config: &Config, tx: mpsc::Sender<MyMessage>) -> Result<(), AppError> {
    info!("Starting reading loop");

    let addr = format!("amqp://{}:{}@{}:{}", config.rabbitmq_user, config.rabbitmq_password, config.rabbitmq_host, config.rabbitmq_amqp_port);

    let connection = Connection::connect(&addr, ConnectionProperties::default()).await.expect("Could not connect to amqp server");
    let channel = connection.create_channel().await.expect("Could not create channel");

    channel.queue_declare(config.rabbitmq_queue.clone().into(),
        options::QueueDeclareOptions::durable(),
        lapin::types::FieldTable::default())
        .await.expect("could not declare queue");

    let mut consumer = channel.basic_consume(config.rabbitmq_queue.clone().into(), "queue_consumer".into(),
        options::BasicConsumeOptions::default(),
        lapin::types::FieldTable::default())
        .await.expect("Could not instantiate consumer");

    let mut counter = 0;
    while let Some(Ok(delivery)) = consumer.next().await {

        let data = String::from_utf8(delivery.data).unwrap_or("invalid message format".to_string());
        //output some feedback
        counter +=1;
        if counter % 10_000 == 0 {
            info!("reached {}, {}", counter, data.clone());
        }

        //send to channel
        tx.send(MyMessage{ data }).await.unwrap();

        if counter >= 100_000 {
            break;
        }
    }
    info!("Reading loop ended");
    Ok(())
}
