use rabbitmq_stream_client::{Environment, types::OffsetSpecification};
use futures::StreamExt;
use tracing_subscriber::FmtSubscriber;
use tracing::info;
use tokio::sync::mpsc;

use rabbit_client::config::Config;
use rabbit_client::error::AppError;
use offset_tracker::OffsetTracker;
mod offset_tracker;

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
    let env = Environment::builder()
        .host(&config.rabbitmq_host)
        .port(config.rabbitmq_port)
        .username(&config.rabbitmq_user)
        .password(&config.rabbitmq_password)
        .build().await?;

    //this will persist the current position in the stream
    let offset_tracker = OffsetTracker::new("rabbit_consumer_offset".to_string(), &config.local_storage)
        .expect("failed building offset tracker");

    //get the locally stored offset from which to start consuming or start from First
    let offset_spec = if let Ok(Some(offset)) = offset_tracker.read() {
        OffsetSpecification::Offset(offset)
    } else { 
        OffsetSpecification::First 
    };

    info!("Starting reading loop from offset {:?}", offset_spec);

    let mut consumer = env.consumer()
        .offset(offset_spec)
        .build(&config.rabbitmq_stream).await.expect("Stream should be available");
    let handle = consumer.handle();

    //switch offset_tracker to async version
    let offset_tracker = offset_tracker.into_async();

    let mut counter = 0;
    while let Some(Ok(message)) = consumer.next().await {
        if let Some(data) = message.message().data() {
            let data = String::from_utf8(data.to_vec()).unwrap(); 
            //output some feedback
            counter +=1;
            if counter % 10_000 == 0 {
                info!("reached {}, {}", counter, data.clone());
            }

            //send to channel
            tx.send(MyMessage{ data }).await.unwrap();

            //persist the current offset
            let offset = message.offset();
            offset_tracker.write(offset).await;
        }

        if counter >= 100_000 {
            break;
        }
    }
    info!("Reading loop ended");
    offset_tracker.close().await.unwrap();
    handle.close().await.unwrap();
    Ok(())
}
