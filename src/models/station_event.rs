use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::models::payment_method::PaymentMethod;

#[derive(Serialize, Deserialize, Debug, Clone)]
///Representamos los eventos que suceden en una estacion
pub enum StationEvent {
    BikeRented {
        event_id: u32,
        user_id: u32,
        station_id: u32,
        bike_id: u32,
        timestamp: DateTime<Utc>,
    },
    BikeReturned {
        event_id: u32,
        user_id: u32,
        station_id: u32,
        bike_id: u32,
        start_time: DateTime<Utc>,
        return_time: DateTime<Utc>,
        payment_method: PaymentMethod,
    },
}