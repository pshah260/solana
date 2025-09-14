use anyhow::Result;
use shared::{OrderBook, TopOfBook};
use std::path::Path;

fn main() -> Result<()> {
    println!("🔧 Creating test data in memory-mapped files...");
    
    let ob_path = std::env::var("OB_MMAP")
        .unwrap_or_else(|_| "/dev/shm/solusd_order_book.mmap".to_string());
    let tob_path = std::env::var("TOB_MMAP")
        .unwrap_or_else(|_| "/dev/shm/solusd_top_of_book.mmap".to_string());

    // Create and populate TopOfBook
    let (_tob_mmap, tob) = TopOfBook::mmap(Path::new(&tob_path))?;
    tob.set_bid(145_850_000, 2_500_000); // $145.85 @ 2.5 SOL
    tob.set_ask(145_900_000, 1_800_000); // $145.90 @ 1.8 SOL
    tob.set_ts(1726311234567); // Sample timestamp
    
    // Create and populate OrderBook with sample ladder
    let (_ob_mmap, ob) = OrderBook::mmap(Path::new(&ob_path))?;
    
    // Sample bid ladder (decreasing prices)
    let bid_data = [
        (145_850_000, 2_500_000), // L1: $145.85 @ 2.5 SOL
        (145_800_000, 3_200_000), // L2: $145.80 @ 3.2 SOL
        (145_750_000, 1_100_000), // L3: $145.75 @ 1.1 SOL
        (145_700_000, 4_500_000), // L4: $145.70 @ 4.5 SOL
        (145_650_000, 2_800_000), // L5: $145.65 @ 2.8 SOL
    ];
    
    // Sample ask ladder (increasing prices)
    let ask_data = [
        (145_900_000, 1_800_000), // L1: $145.90 @ 1.8 SOL
        (145_950_000, 2_300_000), // L2: $145.95 @ 2.3 SOL
        (146_000_000, 3_700_000), // L3: $146.00 @ 3.7 SOL
        (146_050_000, 1_600_000), // L4: $146.05 @ 1.6 SOL
        (146_100_000, 5_200_000), // L5: $146.10 @ 5.2 SOL
    ];
    
    for (i, &(price, qty)) in bid_data.iter().enumerate() {
        ob.update_bid(i, price, qty);
    }
    
    for (i, &(price, qty)) in ask_data.iter().enumerate() {
        ob.update_ask(i, price, qty);
    }
    
    ob.set_ts(1726311234567); // Same timestamp
    
    println!("✅ Test data created successfully!");
    println!("📁 Files created:");
    println!("   TopOfBook: {}", tob_path);
    println!("   OrderBook: {}", ob_path);
    println!();
    println!("💡 Now run: cargo run -p ingest --bin reader");
    
    Ok(())
}