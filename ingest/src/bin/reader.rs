use anyhow::Result;
use shared::{OrderBook, TopOfBook, BOOK_DEPTH};
use std::path::Path;
use std::ptr;
use std::time::{SystemTime, UNIX_EPOCH};

fn format_price(price_u: u64) -> String {
    if price_u == 0 {
        "0.000000".to_string()
    } else {
        format!("{:.6}", price_u as f64 / 1_000_000.0)
    }
}

fn format_qty(qty_u: u64) -> String {
    if qty_u == 0 {
        "0.000000".to_string()
    } else {
        format!("{:.6}", qty_u as f64 / 1_000_000.0)
    }
}

fn format_timestamp(ts_ms: u64) -> String {
    if ts_ms == 0 {
        "no timestamp".to_string()
    } else {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let age_ms = if now_ms > ts_ms { now_ms - ts_ms } else { 0 };
        format!("{} ({:.1}s ago)", ts_ms, age_ms as f64 / 1000.0)
    }
}

fn main() -> Result<()> {
    let ob_path = std::env::var("OB_MMAP")
        .unwrap_or_else(|_| "/dev/shm/solusd_order_book.mmap".to_string());
    let tob_path = std::env::var("TOB_MMAP")
        .unwrap_or_else(|_| "/dev/shm/solusd_top_of_book.mmap".to_string());

    println!("📊 SOLUSD Market Data Reader");
    println!("═══════════════════════════");
    println!("Order Book: {}", ob_path);
    println!("Top of Book: {}", tob_path);
    println!();

    // Read Top of Book
    if Path::new(&tob_path).exists() {
        let (_tob_mmap, tob) = TopOfBook::mmap(Path::new(&tob_path))?;
        let bid_price = unsafe { ptr::read_volatile(&tob.bid_price) };
        let bid_qty = unsafe { ptr::read_volatile(&tob.bid_qty) };
        let ask_price = unsafe { ptr::read_volatile(&tob.ask_price) };
        let ask_qty = unsafe { ptr::read_volatile(&tob.ask_qty) };
        let timestamp = unsafe { ptr::read_volatile(&tob.timestamp_ms) };

        println!("🏆 TOP OF BOOK");
        println!("──────────────");
        println!("Best Bid: {} @ {}", format_price(bid_price), format_qty(bid_qty));
        println!("Best Ask: {} @ {}", format_price(ask_price), format_qty(ask_qty));
        if bid_price > 0 && ask_price > 0 {
            let spread = ask_price as f64 - bid_price as f64;
            let mid = (bid_price as f64 + ask_price as f64) / 2.0;
            println!("Spread:   {:.6} ({:.2} bps)", spread / 1_000_000.0, (spread / mid) * 10_000.0);
        }
        println!("Updated:  {}", format_timestamp(timestamp));
        println!();
    } else {
        println!("❌ Top of Book file not found: {}", tob_path);
        println!();
    }

    // Read Order Book
    if Path::new(&ob_path).exists() {
        let (_ob_mmap, ob) = OrderBook::mmap(Path::new(&ob_path))?;
        let timestamp = unsafe { ptr::read_volatile(&ob.timestamp_ms) };

        println!("📈 ORDER BOOK (First 10 levels)");
        println!("───────────────────────────────");
        println!("Updated: {}", format_timestamp(timestamp));
        println!();
        println!("{:>3} {:>12} {:>12} | {:>12} {:>12} {:>3}", 
                 "Lvl", "Bid Size", "Bid Price", "Ask Price", "Ask Size", "Lvl");
        println!("{}", "─".repeat(65));

        let levels_to_show = std::cmp::min(10, BOOK_DEPTH);
        
        for i in 0..levels_to_show {
            let bid_price = ob.bids[i].load_price();
            let bid_qty = ob.bids[i].load_qty();
            let ask_price = ob.asks[i].load_price();
            let ask_qty = ob.asks[i].load_qty();

            let bid_price_str = if bid_price > 0 { format_price(bid_price) } else { "".to_string() };
            let bid_qty_str = if bid_qty > 0 { format_qty(bid_qty) } else { "".to_string() };
            let ask_price_str = if ask_price > 0 { format_price(ask_price) } else { "".to_string() };
            let ask_qty_str = if ask_qty > 0 { format_qty(ask_qty) } else { "".to_string() };

            let lvl_str = if bid_price > 0 || ask_price > 0 { (i + 1).to_string() } else { "".to_string() };

            println!("{:>3} {:>12} {:>12} | {:>12} {:>12} {:>3}", 
                     lvl_str, bid_qty_str, bid_price_str, ask_price_str, ask_qty_str, lvl_str);
        }
        println!();

        // Summary stats
        let mut active_bid_levels = 0;
        let mut active_ask_levels = 0;
        for i in 0..BOOK_DEPTH {
            if ob.bids[i].load_price() > 0 { active_bid_levels += 1; }
            if ob.asks[i].load_price() > 0 { active_ask_levels += 1; }
        }
        println!("📊 BOOK STATS");
        println!("─────────────");
        println!("Active bid levels: {}/{}", active_bid_levels, BOOK_DEPTH);
        println!("Active ask levels: {}/{}", active_ask_levels, BOOK_DEPTH);
    } else {
        println!("❌ Order Book file not found: {}", ob_path);
    }

    Ok(())
}