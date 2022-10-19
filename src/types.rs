use std::num::ParseFloatError;

#[derive(Debug)]
pub struct Level {
    pub exchange: String,
    pub price: f64,
    pub quantity: f64,
}

#[derive(Debug)]
pub struct Summary {
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

impl Summary {
    pub fn spread(&self) -> f64 {
        self.bids[0].price - self.asks[0].price
    }
}

#[derive(Debug)]
pub enum WebsocketError {
    ParseError(ParseFloatError),
}