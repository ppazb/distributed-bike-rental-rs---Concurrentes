use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::models::station_info::StationInfo;

#[derive(Serialize, Deserialize, Debug, Clone)]
///Informacion del viaje para operaciones
pub struct TripInfo {
    pub trip_id: u32,
    pub current_bike_id: u32,
    pub start_time: DateTime<Utc>,
    pub last_owner_station: StationInfo,
}