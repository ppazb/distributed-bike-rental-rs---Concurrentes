use serde::{Deserialize, Serialize};

/// Representa las coordenadas x,y
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct Location {
    pub location: (usize, usize),
}