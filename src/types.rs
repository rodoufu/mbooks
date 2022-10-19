use std::num::ParseFloatError;

#[derive(Debug)]
pub enum WebsocketError {
    InvalidAsset(String),
    InvalidPair(String),
    ParseError(ParseFloatError),
}

#[derive(Debug, Eq, PartialEq)]
pub enum Asset {
    BTC,
    USD,
    ETH,
}

impl TryFrom<&str> for Asset {
    type Error = WebsocketError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "btc" => Ok(Asset::BTC),
            "usd" => Ok(Asset::USD),
            "eth" => Ok(Asset::ETH),
            _ => Err(WebsocketError::InvalidAsset(value.to_string())),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Pair {
    base: Asset,
    quote: Asset,
}

impl TryFrom<String> for Pair {
    type Error = WebsocketError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let value = value.to_lowercase();
        if value.len() != 6 {
            return Err(WebsocketError::InvalidPair(value));
        }
        Ok(Self {
            base: Asset::try_from(&value[0..3])?,
            quote: Asset::try_from(&value[3..6])?,
        })
    }
}

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

mod test {
    use crate::types::{Asset, Pair};

    #[test]
    fn should_parse_ethbtc_pair() {
        // Given
        let msg = "ethbtC".to_string();

        // When
        let pair = Pair::try_from(msg);

        // Then
        assert!(pair.is_ok());
        let pair = pair.ok().unwrap();
        assert_eq!(Pair { base: Asset::ETH, quote: Asset::BTC }, pair);
    }
}