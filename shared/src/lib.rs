use std::fs::OpenOptions;
use std::mem::size_of;
use std::path::Path;
use std::ptr;
use memmap2::MmapOptions;

pub const BOOK_DEPTH: usize = 50;

#[repr(C)]
#[derive(Default, Clone, Copy)]
pub struct OrderLevel {
    pub price: u64, // micro dollars
    pub qty: u64,   // base units (1e-6)
}

impl OrderLevel {
    #[inline] pub fn new() -> Self { Self::default() }
    #[inline] pub fn load_price(&self) -> u64 { unsafe { ptr::read_volatile(&self.price) } }
    #[inline] pub fn store_price(&mut self, v: u64) { unsafe { ptr::write_volatile(&mut self.price, v) } }
    #[inline] pub fn load_qty(&self) -> u64 { unsafe { ptr::read_volatile(&self.qty) } }
    #[inline] pub fn store_qty(&mut self, v: u64) { unsafe { ptr::write_volatile(&mut self.qty, v) } }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct OrderBook {
    pub bids: [OrderLevel; BOOK_DEPTH],
    pub asks: [OrderLevel; BOOK_DEPTH],
    pub timestamp_ms: u64,
}

impl Default for OrderBook {
    fn default() -> Self {
        Self {
            bids: [OrderLevel::default(); BOOK_DEPTH],
            asks: [OrderLevel::default(); BOOK_DEPTH],
            timestamp_ms: 0,
        }
    }
}

impl OrderBook {
    pub fn mmap(path: &Path) -> std::io::Result<(memmap2::MmapMut, &'static mut Self)> {
        let file = OpenOptions::new().read(true).write(true).create(true).open(path)?;
        file.set_len(size_of::<Self>() as u64)?;
        let mut mmap = unsafe { MmapOptions::new().map_mut(&file)? };
        let ptr = mmap.as_mut_ptr() as *mut Self;
        let ob_ref = unsafe { &mut *ptr };
        Ok((mmap, ob_ref))
    }
    #[inline] pub fn update_bid(&mut self, i: usize, price: u64, qty: u64) { if i<BOOK_DEPTH { self.bids[i].store_price(price); self.bids[i].store_qty(qty); }}
    #[inline] pub fn update_ask(&mut self, i: usize, price: u64, qty: u64) { if i<BOOK_DEPTH { self.asks[i].store_price(price); self.asks[i].store_qty(qty); }}
    #[inline] pub fn set_ts(&mut self, ts: u64) { unsafe { ptr::write_volatile(&mut self.timestamp_ms, ts) } }
}

#[repr(C)]
#[derive(Default, Clone, Copy)]
pub struct TopOfBook {
    pub bid_price: u64,
    pub bid_qty: u64,
    pub ask_price: u64,
    pub ask_qty: u64,
    pub timestamp_ms: u64,
}

impl TopOfBook {
    pub fn mmap(path: &Path) -> std::io::Result<(memmap2::MmapMut, &'static mut Self)> {
        let file = OpenOptions::new().read(true).write(true).create(true).open(path)?;
        file.set_len(size_of::<Self>() as u64)?;
        let mut mmap = unsafe { MmapOptions::new().map_mut(&file)? };
        let ptr = mmap.as_mut_ptr() as *mut Self;
        let ob_ref = unsafe { &mut *ptr };
        Ok((mmap, ob_ref))
    }
    #[inline] pub fn set_bid(&mut self, p: u64, q: u64) { unsafe { ptr::write_volatile(&mut self.bid_price, p); ptr::write_volatile(&mut self.bid_qty, q);} }
    #[inline] pub fn set_ask(&mut self, p: u64, q: u64) { unsafe { ptr::write_volatile(&mut self.ask_price, p); ptr::write_volatile(&mut self.ask_qty, q);} }
    #[inline] pub fn set_ts(&mut self, ts: u64) { unsafe { ptr::write_volatile(&mut self.timestamp_ms, ts) } }
}

#[derive(Debug, Clone)]
pub struct TradeEvent {
    pub ts_ms: u64,
    pub symbol: String,
    pub price_u: u64,
    pub qty_u: u64,
    pub side: String, // buy/sell from taker perspective
}
