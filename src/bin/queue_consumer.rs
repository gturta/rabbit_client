use lapin::{Connection, ConnectionProperties, options};
use tracing_subscriber::FmtSubscriber;
use tracing::info;
use futures::stream::StreamExt;
use tokio::task::JoinSet;
use tokio::sync::Semaphore;
use std::sync::Arc;

use rabbit_client::config::Config;
use rabbit_client::error::AppError;

#[tokio::main]
async fn main() {
    // setup tracing first
    let subscriber = FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).unwrap();
    info!("Consumer starting");
    
    let config = Config::default();


    tokio::spawn( async move{
        rabbitmq_reader(config).await.unwrap();
    }).await.unwrap();
}


async fn push_sender(config: Config, data: String) -> Result<(), AppError> {
    let client = reqwest::Client::new();

    client.post(config.push_url.clone())
        .body(data)
        .send().await?;
    Ok(())
}

async fn rabbitmq_reader(config: Config) -> Result<(), AppError> {
    info!("Starting reading loop");

    let addr = format!("amqp://{}:{}@{}:{}", config.rabbitmq_user, config.rabbitmq_password, config.rabbitmq_host, config.rabbitmq_amqp_port);

    let connection = Connection::connect(&addr, ConnectionProperties::default()).await.expect("Could not connect to amqp server");
    let channel = connection.create_channel().await.expect("Could not create channel");

    channel.queue_declare(config.rabbitmq_queue.clone().into(),
        options::QueueDeclareOptions::durable(),
        lapin::types::FieldTable::default())
        .await.expect("could not declare queue");
    channel.basic_qos(1000, options::BasicQosOptions::default()).await.unwrap();

    let mut consumer = channel.basic_consume(config.rabbitmq_queue.clone().into(), "queue_consumer".into(),
        options::BasicConsumeOptions::default(),
        lapin::types::FieldTable::default())
        .await.expect("Could not instantiate consumer");

    let mut counter = 0;
    //this is to join spawned tasks that call push
    let mut join_set = JoinSet::new();
    //and this to limit the number of spawned tasks to 100
    let semaphore = Arc::new(Semaphore::new(100));

    while let Some(Ok(delivery)) = consumer.next().await {

        let data = String::from_utf8(delivery.data.clone()).unwrap_or("invalid message format".to_string());
        //output some feedback
        counter +=1;
        if counter % 10_000 == 0 {
            info!("reached {}, {}", counter, data.clone());
        }

        //acquire permit before spawning new task
        let permit = semaphore.clone().acquire_owned().await.unwrap();

        let config_clone = config.clone();
        join_set.spawn(async move {
            //send to push
            push_sender(config_clone, data).await.unwrap();
            //and acknoledge
            delivery.ack(lapin::options::BasicAckOptions::default()).await.expect("could not deliver ack");
            //release permit
            drop(permit);
        });

        if counter >= 100_000 {
            break;
        }
    }
    //wait for all tasks in join_set
    while let Some(res) = join_set.join_next().await {
        res.unwrap(); //ignore error
    }
    info!("Reading loop ended");
    Ok(())
}
