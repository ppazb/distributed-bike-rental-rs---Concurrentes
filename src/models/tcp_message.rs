use serde::{Deserialize, Serialize};
use crate::models::payment_method::PaymentMethod;
use crate::models::trip_info::TripInfo;
use crate::models::station_event::StationEvent;

/// Protocolo de comunicación serializable sobre TCP.
#[derive(Serialize, Deserialize, Debug, Clone)]
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
    WhoIsLeader {
        origin_addr: String,
    },
    RingElection {
        active_nodes: Vec<u32>,
    },
    RingElected {
        active_nodes: Vec<u32>,
        leader_id: u32,
        leader_addr: String,
        initiator_id: u32, 
    },
    RequestPendingEvents {
        origin_addr: String,
        ttl: usize,
    },
    SyncEvent {
        origin_addr: String,
        event: StationEvent,
    },
    EventACK {
        event_id: u32,
    },
    ClosedTrip {
        bike_id: u32,
    },
    
    QueryTrips {
        origin_addr: String,
        ttl: usize,
    },
    QueryTripsResponse {
        trips: Vec<TripInfo>,
        error: bool,
    },
}