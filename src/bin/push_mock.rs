use tracing_subscriber::FmtSubscriber;
use tracing::info;
use axum::{Router, routing::post};

#[tokio::main]
async fn main() {
    let tr = FmtSubscriber::new();
    tracing::subscriber::set_global_default(tr).unwrap();

    tracing::info!("Starting listener");

    let app = Router::new()
        .route("/push", post(push_message));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:7878").await.expect("port should be available for bind");

    axum::serve(listener, app).await.expect("axum could not start serving");

}

async fn push_message(body: String) {
    info!(body);
}

