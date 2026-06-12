use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StationInfo {
    pub station_id: u32,
    pub location: (usize, usize),
    pub addr: String,
}