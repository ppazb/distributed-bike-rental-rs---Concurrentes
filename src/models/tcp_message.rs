use serde::{Deserialize, Serialize};
use crate::models::payment_method::PaymentMethod;
use crate::models::trip_info::TripInfo;

#[derive(Serialize, Deserialize, Debug, Clone)]
///Declaracion de mensajes enviados por TCP
pub enum TcpMessage {
    GetBikes,
    Availability {
        station_id: u32,
        bikes_available: Vec<u32>,
        free_slots: usize,
    },
    RentBike {
        user_id: u32,
        bike_id: u32,
        payment_method: PaymentMethod,
    },
    RentResponse {
        success: bool,
        message: String,
    },
    ReturnBike {
        user_id: u32,
        bike_id: u32,
        payment_method: PaymentMethod,
        trip_info: TripInfo,
    },
    ReturnResponse {
        success: bool,
        message: String,
    },
    RingElection {
        highest_id: u32,
    },
    RingElected {
        leader_id: u32,
        leader_addr: String,
    },
}