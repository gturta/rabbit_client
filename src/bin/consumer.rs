use rabbitmq_stream_client::{Environment, types::OffsetSpecification};
use futures::StreamExt;
use tracing_subscriber::FmtSubscriber;
use tracing::info;
use tokio::sync::mpsc;

use rabbit_client::config::Config;

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

    let h1 = tokio::spawn(async move{
        rabbitmq_reader(&config_clone, tx).await;
    });

    let h2 = tokio::spawn(async move{
        push_sender(&config, rx).await;
    });

    let _r = tokio::join!(h1, h2);
}

async fn push_sender(config: &Config, mut rx: mpsc::Receiver<MyMessage>) {
    let client = reqwest::Client::new();

    while let Some(message) = rx.recv().await {
        client.post(config.push_url.clone())
            .body(message.data)
            .send().await.unwrap();
    }
}

async fn rabbitmq_reader(config: &Config, tx: mpsc::Sender<MyMessage>){
    let env = Environment::builder()
        .host(&config.rabbitmq_host)
        .port(config.rabbitmq_port)
        .username(&config.rabbitmq_user)
        .password(&config.rabbitmq_password)
        .build().await.expect("RabbitMQ should be available");

    let mut consumer = env.consumer()
        .offset(OffsetSpecification::First)
        .build(&config.rabbitmq_stream).await.expect("Stream should be available");

    let handle = consumer.handle();

    info!("Starting reading loop");
    let mut counter = 0;
    while let Some(message) = consumer.next().await {
        if let Some(data) = message.unwrap().message().data() {
            let data = String::from_utf8(data.to_vec()).unwrap(); 
            //output some feedback
            counter +=1;
            if counter % 10_000 == 0 {
                info!("reached {}, {}", counter, data.clone());
            }

            //send to channel
            tx.send(MyMessage{ data }).await.unwrap();

        }

        if counter >= 100_000 {
            break;
        }
    }
    info!("Reading loop ended");
    handle.close().await.unwrap();
}
