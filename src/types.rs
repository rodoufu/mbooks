use crate::orderbook;
use std::{
    fmt::{
        Display,
        Formatter,
    },
    num::ParseFloatError,
};

#[derive(Debug)]
pub enum WebsocketError {
    InvalidAsset(String),
    InvalidPair(String),
    ParseError(ParseFloatError),
}

impl Display for WebsocketError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for WebsocketError {}

#[derive(Debug, Eq, PartialEq)]
pub enum Asset {
    ADA,
    BTC,
    DOT,
    ETH,
    LINK,
    LTC,
    SOL,
    USD,
    USDC,
    USDT,
}

impl TryFrom<&str> for Asset {
    type Error = WebsocketError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "ada" => Ok(Asset::ADA),
            "btc" => Ok(Asset::BTC),
            "dot" => Ok(Asset::DOT),
            "eth" => Ok(Asset::ETH),
            "link" => Ok(Asset::LINK),
            "ltc" => Ok(Asset::LTC),
            "sol" => Ok(Asset::SOL),
            "usd" => Ok(Asset::USD),
            "usdt" => Ok(Asset::USDT),
            "usdc" => Ok(Asset::USDC),
            _ => Err(WebsocketError::InvalidAsset(value.to_string())),
        }
    }
}

impl ToString for Asset {
    fn to_string(&self) -> String {
        format!("{:?}", self)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Symbol {
    pub base: Asset,
    pub quote: Asset,
}

impl TryFrom<String> for Symbol {
    type Error = WebsocketError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let value = value.to_lowercase();
        let pos_slash = value.find('/')
            .map(Ok)
            .unwrap_or_else(|| Err(WebsocketError::InvalidPair(value.clone())))?;
        Ok(Self {
            base: Asset::try_from(&value[..pos_slash])?,
            quote: Asset::try_from(&value[pos_slash + 1..])?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct Level {
    pub exchange: String,
    pub price: f64,
    pub quantity: f64,
}

impl Into<orderbook::Level> for &Level {
    fn into(self) -> orderbook::Level {
        orderbook::Level {
            exchange: self.exchange.clone(),
            price: self.price,
            amount: self.quantity,
        }
    }
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

impl Into<orderbook::Summary> for Summary {
    fn into(self) -> orderbook::Summary {
        orderbook::Summary {
            spread: self.spread(),
            bids: self.bids.iter().map(|x| x.into()).collect(),
            asks: self.asks.iter().map(|x| x.into()).collect(),
        }
    }
}

mod test {
    use crate::types::{
        Asset,
        Symbol,
    };

    #[test]
    fn should_parse_ethbtc_pair() {
        // Given
        let msg = "eth/btC".to_string();

        // When
        let pair = Symbol::try_from(msg);

        // Then
        assert!(pair.is_ok());
        let pair = pair.ok().unwrap();
        assert_eq!(Symbol { base: Asset::ETH, quote: Asset::BTC }, pair);
    }
}