use std::env;
use anyhow::Result;
use tracing::{info, error, warn};
use shared::{OrderBook, TopOfBook, BOOK_DEPTH, TradeEvent};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use futures_util::{StreamExt, SinkExt};
use serde_json;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    
    // Initialize rustls crypto provider
    let _ = rustls::crypto::ring::default_provider().install_default();
    
    let _kafka_brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());
    let kafka_topic = env::var("KAFKA_TOPIC").unwrap_or_else(|_| "solusd-trades".to_string());
    
    let data_dir = env::var("DATA_DIR").unwrap_or_else(|_| "/tmp/solana_market_data".to_string());
    let ob_path = format!("{}/order_book.bin", data_dir);
    let tob_path = format!("{}/top_of_book.bin", data_dir);
    
    let (_ob_mmap, order_book) = OrderBook::mmap(std::path::Path::new(&ob_path))?;
    let (_tob_mmap, top) = TopOfBook::mmap(std::path::Path::new(&tob_path))?;
    
    info!("📁 Order Book: {}", ob_path);
    info!("📁 Top of Book: {}", tob_path);

    // Clone variables for tasks
    let kafka_brokers_v1 = _kafka_brokers.clone();
    let kafka_topic_v1 = kafka_topic.clone();

    // v2 order book (depth) task
    let ob_task = tokio::spawn(async move {
        loop {
            info!("Connecting to Gemini v2 API...");
            let url = "wss://api.gemini.com/v2/marketdata";
            match connect_async(url).await {
                Ok((ws, _)) => {
                    info!("✅ Connected to Gemini v2 API");
                    let (mut write, mut read) = ws.split();
                    // Subscribe to L2 (order book) for SOLUSD
                    let sub = serde_json::json!({
                        "type": "subscribe",
                        "subscriptions": [{"name": "l2","symbols":["SOLUSD"]}]
                    });
                    let _ = write.send(Message::Text(sub.to_string())).await;
                    info!("📊 Subscribed to SOLUSD L2 order book");
                    
                    while let Some(msg) = read.next().await {
                        if let Ok(Message::Text(txt)) = msg {
                            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&txt) {
                                // Try to parse snapshot or updates - forgiving schema
                                if let Some(bids) = v.get("bids").and_then(|x| x.as_array()) {
                                    for (i, lvl) in bids.iter().take(BOOK_DEPTH).enumerate() {
                                        let p = lvl.get(0).and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                                        let q = lvl.get(1).and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                                        order_book.update_bid(i, (p*1_000_000.0) as u64, (q*1_000_000.0) as u64);
                                    }
                                    if let Some(ts) = v.get("timestampms").and_then(|t| t.as_u64()) { order_book.set_ts(ts); }
                                }
                                if let Some(asks) = v.get("asks").and_then(|x| x.as_array()) {
                                    for (i, lvl) in asks.iter().take(BOOK_DEPTH).enumerate() {
                                        let p = lvl.get(0).and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                                        let q = lvl.get(1).and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                                        order_book.update_ask(i, (p*1_000_000.0) as u64, (q*1_000_000.0) as u64);
                                    }
                                    if let Some(ts) = v.get("timestampms").and_then(|t| t.as_u64()) { order_book.set_ts(ts); }
                                }
                                // Handle incremental change-like messages (best-effort)
                                if let Some(changes) = v.get("changes").and_then(|x| x.as_array()) {
                                    for ch in changes.iter() {
                                        if let (Some(side), Some(price), Some(qty)) = (
                                            ch.get(0).and_then(|x| x.as_str()),
                                            ch.get(1).and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()),
                                            ch.get(2).and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()),
                                        ) {
                                            let pu = (price*1_000_000.0) as u64; 
                                            let qu = (qty*1_000_000.0) as u64;
                                            
                                            // Simple approach: update first few levels based on price ordering
                                            if side.eq_ignore_ascii_case("buy") {
                                                // For bids, higher prices should be at lower indices
                                                for i in 0..BOOK_DEPTH.min(10) {
                                                    let current_price = order_book.bids[i].load_price();
                                                    if qu == 0 && current_price == pu {
                                                        // Remove this level by shifting everything up
                                                        order_book.update_bid(i, 0, 0);
                                                        break;
                                                    } else if current_price == 0 || pu > current_price {
                                                        // Insert/update at this level
                                                        order_book.update_bid(i, pu, qu);
                                                        break;
                                                    } else if current_price == pu {
                                                        // Update existing level
                                                        order_book.update_bid(i, pu, qu);
                                                        break;
                                                    }
                                                }
                                            } else {
                                                // For asks, lower prices should be at lower indices  
                                                for i in 0..BOOK_DEPTH.min(10) {
                                                    let current_price = order_book.asks[i].load_price();
                                                    if qu == 0 && current_price == pu {
                                                        // Remove this level
                                                        order_book.update_ask(i, 0, 0);
                                                        break;
                                                    } else if current_price == 0 || (current_price > pu && pu > 0) {
                                                        // Insert/update at this level
                                                        order_book.update_ask(i, pu, qu);
                                                        break;
                                                    } else if current_price == pu {
                                                        // Update existing level
                                                        order_book.update_ask(i, pu, qu);
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    if let Some(ts) = v.get("timestampms").and_then(|t| t.as_u64()) { 
                                        order_book.set_ts(ts); 
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("❌ Failed to connect to Gemini v2 API: {}", e);
                    warn!("🔄 Retrying v2 connection in 5 seconds...");
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    });

    // v1 top-of-book + trades task
    let top_task = tokio::spawn(async move {
        loop {
            info!("Connecting to Gemini v1 API...");
            let url = "wss://api.gemini.com/v1/marketdata/SOLUSD";
            match connect_async(url).await {
                Ok((ws, _)) => {
                    info!("✅ Connected to Gemini v1 API");
                    let (mut write, mut read) = ws.split();
                    info!("📈 Subscribed to SOLUSD top-of-book and trades");
                    
                    while let Some(msg) = read.next().await {
                        match msg {
                            Ok(Message::Text(txt)) => {
                                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&txt) {
                                    if let Some(events) = v.get("events").and_then(|e| e.as_array()) {
                                        let ts = v.get("timestampms").and_then(|t| t.as_u64()).unwrap_or(0);
                                        for e in events {
                                            if let Some(t) = e.get("type").and_then(|x| x.as_str()) {
                                                match t {
                                                    "change" => {
                                                        let side = e.get("side").and_then(|x| x.as_str()).unwrap_or("");
                                                        let price = e.get("price").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                                                        let rem = e.get("remaining").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                                                        if side == "bid" { top.set_bid((price*1_000_000.0) as u64, (rem*1_000_000.0) as u64); }
                                                        if side == "ask" { top.set_ask((price*1_000_000.0) as u64, (rem*1_000_000.0) as u64); }
                                                        top.set_ts(ts);
                                                    },
                                                    "trade" => {
                                                        let price = e.get("price").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                                                        let qty = e.get("amount").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                                                        let side = e.get("makerSide").and_then(|x| x.as_str()).unwrap_or("");
                                                        let tr = TradeEvent { ts_ms: ts, symbol: "SOLUSD".into(), price_u: (price*1_000_000.0) as u64, qty_u: (qty*1_000_000.0) as u64, side: side.into() };
                                                        #[cfg(feature = "kafka")]
                                                        {
                                                            let producer: rdkafka::producer::FutureProducer = rdkafka::config::ClientConfig::new()
                                                                .set("bootstrap.servers", &kafka_brokers_v1)
                                                                .create()
                                                                .expect("producer");
                                                            let payload = serde_json::to_vec(&serde_json::json!({
                                                                "ts_ms": tr.ts_ms, "symbol": tr.symbol, "price_u": tr.price_u, "qty_u": tr.qty_u, "side": tr.side
                                                            })).unwrap();
                                                            let _ = producer
                                                                .send(
                                                                    rdkafka::producer::FutureRecord::to(&kafka_topic_v1).payload(&payload),
                                                                    std::time::Duration::from_secs(0),
                                                                )
                                                                .await;
                                                        }
                                                        #[cfg(feature = "pulsar")]
                                                        {
                                                            let pulsar_url = std::env::var("PULSAR_URL").unwrap_or_else(|_| "pulsar://localhost:6650".to_string());
                                                            let pulsar: pulsar::Pulsar<_> = pulsar::PulsarBuilder::new(pulsar_url, pulsar::TokioExecutor).build().await.expect("pulsar client");
                                                            let mut producer = pulsar.producer()
                                                                .with_topic(&kafka_topic_v1) // reuse topic env var
                                                                .with_name("gemini-trades")
                                                                .build()
                                                                .await
                                                                .expect("pulsar producer");
                                                            let payload = serde_json::to_vec(&serde_json::json!({
                                                                "ts_ms": tr.ts_ms, "symbol": tr.symbol, "price_u": tr.price_u, "qty_u": tr.qty_u, "side": tr.side
                                                            })).unwrap();
                                                            let _ = producer.send(payload).await;
                                                        }
                                                        #[cfg(not(any(feature = "kafka", feature = "pulsar")))]
                                                        {
                                                            let _ = (tr, &kafka_topic_v1); // suppress unused warnings
                                                        }
                                                    },
                                                    _ => {}
                                                }
                                            }
                                        }
                                    }
                                }
                            },
                            Ok(Message::Ping(_)) => {
                                let _ = write.send(Message::Pong(vec![])).await;
                            }
                            _ => {}
                        }
                    }
                }
                Err(e) => {
                    error!("❌ Failed to connect to Gemini v1 API: {}", e);
                    warn!("🔄 Retrying v1 connection in 5 seconds...");
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    });

    let _ = tokio::join!(ob_task, top_task);
    Ok(())
}