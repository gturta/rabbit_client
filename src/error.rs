use thiserror::Error;

#[derive(Error,Debug)]
pub enum AppError{
    #[error("App Error: {0}")]
    Local(String),

    #[error("Sled Error: {0}")]
    Sled(#[from] sled::Error),

    #[error("Reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("RabbitMQ client error: {0}")]
    RabbitMQ(#[from] rabbitmq_stream_client::error::ClientError),

}
