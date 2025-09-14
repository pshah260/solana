use anyhow::Result;
#[cfg(feature = "kafka")]
use rdkafka::{consumer::{Consumer, StreamConsumer}, Message};
#[cfg(feature = "pulsar")]
use pulsar::{Consumer as PulsarConsumer, SubType};
use tokio_postgres::NoTls;
use std::sync::Arc;
use tracing::{info, warn, error};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "gemini.trades".into());
    let pulsar_url = std::env::var("PULSAR_URL").unwrap_or_else(|_| "pulsar://localhost:6650".into());
    let pg_dsn = std::env::var("PG_DSN").unwrap_or_else(|_| "host=localhost user=postgres password=postgres dbname=trades".into());

    #[cfg(feature = "kafka")]
    let consumer: StreamConsumer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "gemini-consumer")
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;
    #[cfg(feature = "kafka")]
    consumer.subscribe(&[&topic])?;

    #[cfg(feature = "pulsar")]
    let pulsar: pulsar::Pulsar<_> = pulsar::PulsarBuilder::new(pulsar_url, pulsar::TokioExecutor).build().await?;
    #[cfg(feature = "pulsar")]
    let mut consumer: PulsarConsumer<Vec<u8>, _> = pulsar.consumer()
        .with_topic(&topic)
        .with_consumer_name("gemini-consumer")
        .with_subscription_type(SubType::Exclusive)
        .with_subscription("gemini-trades-sub")
        .build()
        .await?;

    let (pg_client_raw, pg_conn) = tokio_postgres::connect(&pg_dsn, NoTls).await?;
    let pg_client = Arc::new(pg_client_raw);
    tokio::spawn(async move { if let Err(e) = pg_conn.await { error!(?e, "pg conn error"); }});

    // Create table if not exists
    pg_client.execute("CREATE TABLE IF NOT EXISTS trades (ts_ms BIGINT, symbol TEXT, price_u BIGINT, qty_u BIGINT, side TEXT)", &[]).await?;
    // Retention: delete older than 7 days
    let _retention_task = {
        let pg = Arc::clone(&pg_client);
        tokio::spawn(async move {
            loop {
                let cutoff = (chrono::Utc::now() - chrono::Duration::days(7)).timestamp_millis();
                let _ = pg.execute("DELETE FROM trades WHERE ts_ms < $1", &[&cutoff]).await;
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
        })
    };

    #[cfg(feature = "kafka")]
    loop {
        match consumer.recv().await {
            Err(e) => warn!(?e, "kafka error"),
            Ok(m) => {
                if let Some(payload) = m.payload() {
                    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(payload) {
                        let ts = v.get("ts_ms").and_then(|x| x.as_i64()).unwrap_or(0);
                        let symbol = v.get("symbol").and_then(|x| x.as_str()).unwrap_or("");
                        let price = v.get("price_u").and_then(|x| x.as_i64()).unwrap_or(0);
                        let qty = v.get("qty_u").and_then(|x| x.as_i64()).unwrap_or(0);
                        let side = v.get("side").and_then(|x| x.as_str()).unwrap_or("");
                        let _ = pg_client.execute(
                            "INSERT INTO trades (ts_ms, symbol, price_u, qty_u, side) VALUES ($1,$2,$3,$4,$5)",
                            &[&ts, &symbol, &price, &qty, &side]
                        ).await;
                    }
                }
            }
        }
    }

    #[cfg(feature = "pulsar")]
    loop {
        match consumer.try_next().await {
            Err(e) => warn!(?e, "pulsar error"),
            Ok(Some(msg)) => {
                if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&msg.payload.data) {
                    let ts = v.get("ts_ms").and_then(|x| x.as_i64()).unwrap_or(0);
                    let symbol = v.get("symbol").and_then(|x| x.as_str()).unwrap_or("");
                    let price = v.get("price_u").and_then(|x| x.as_i64()).unwrap_or(0);
                    let qty = v.get("qty_u").and_then(|x| x.as_i64()).unwrap_or(0);
                    let side = v.get("side").and_then(|x| x.as_str()).unwrap_or("");
                    let _ = pg_client.execute(
                        "INSERT INTO trades (ts_ms, symbol, price_u, qty_u, side) VALUES ($1,$2,$3,$4,$5)",
                        &[&ts, &symbol, &price, &qty, &side]
                    ).await;
                    let _ = consumer.ack(&msg).await;
                }
            }
            Ok(None) => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }

    #[cfg(not(any(feature = "kafka", feature = "pulsar")))]
    {
        info!("No messaging feature enabled. Enable with --features kafka or --features pulsar to consume messages.");
        Ok(())
    }
}
