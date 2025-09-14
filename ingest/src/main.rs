use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use shared::{OrderBook, TopOfBook, BOOK_DEPTH, TradeEvent};
use std::path::Path;
use tokio_tungstenite::connect_async;
use tracing::{info, warn, error};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let ob_path = std::env::var("OB_MMAP").unwrap_or_else(|_| "/dev/shm/solusd_order_book.mmap".to_string());
    let tob_path = std::env::var("TOB_MMAP").unwrap_or_else(|_| "/dev/shm/solusd_top_of_book.mmap".to_string());
    let (_ob_mmap, order_book) = OrderBook::mmap(Path::new(&ob_path))?;
    let (_tob_mmap, top) = TopOfBook::mmap(Path::new(&tob_path))?;

    let kafka_brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let kafka_topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "gemini.trades".into());

    // v2 order book (depth) task
    let ob_task = tokio::spawn(async move {
        let url = "wss://api.gemini.com/v2/marketdata";
        let (ws, _) = connect_async(url).await.expect("v2 connect");
        let (mut write, mut read) = ws.split();
        // Subscribe to L2 (order book) for SOLUSD
        let sub = serde_json::json!({
            "type": "subscribe",
            "channels": [{"name": "l2","symbols":["SOLUSD"]}]
        });
        write.send(tokio_tungstenite::tungstenite::Message::Text(sub.to_string())).await.ok();
        while let Some(msg) = read.next().await {
            if let Ok(tokio_tungstenite::tungstenite::Message::Text(txt)) = msg {
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
                                let pu = (price*1_000_000.0) as u64; let qu = (qty*1_000_000.0) as u64;
                                // naive: just place into level 0 for quick visibility
                                if side.eq_ignore_ascii_case("buy") { order_book.update_bid(0, pu, qu); } else { order_book.update_ask(0, pu, qu); }
                            }
                        }
                        if let Some(ts) = v.get("timestampms").and_then(|t| t.as_u64()) { order_book.set_ts(ts); }
                    }
                }
            }
        }
    });

    // v1 top-of-book + trades task
    let top_task = tokio::spawn(async move {
        let url = "wss://api.gemini.com/v1/marketdata/SOLUSD";
        let (ws, _) = connect_async(url).await.expect("v1 connect");
        let (mut write, mut read) = ws.split();
        while let Some(msg) = read.next().await {
            match msg {
                Ok(tokio_tungstenite::tungstenite::Message::Text(txt)) => {
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
                                                    .set("bootstrap.servers", &kafka_brokers)
                                                    .create()
                                                    .expect("producer");
                                                let payload = serde_json::to_vec(&serde_json::json!({
                                                    "ts_ms": tr.ts_ms, "symbol": tr.symbol, "price_u": tr.price_u, "qty_u": tr.qty_u, "side": tr.side
                                                })).unwrap();
                                                let _ = producer
                                                    .send(
                                                        rdkafka::producer::FutureRecord::to(&kafka_topic).payload(&payload),
                                                        std::time::Duration::from_secs(0),
                                                    )
                                                    .await;
                                            }
                                            #[cfg(not(feature = "kafka"))]
                                            {
                                                let _ = (tr, &kafka_topic); // suppress unused warnings
                                            }
                                        },
                                        _ => {}
                                    }
                                }
                            }
                        }
                    }
                },
                Ok(tokio_tungstenite::tungstenite::Message::Ping(_)) => {
                    let _ = write.send(tokio_tungstenite::tungstenite::Message::Pong(vec![])).await;
                }
                _ => {}
            }
        }
    });

    let _ = tokio::join!(ob_task, top_task);
    Ok(())
}
