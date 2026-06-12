use serde::{Deserialize, Serialize};


#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PaymentMethod {
    /// Representa una tarjeta de crédito mediante su numeración
    CreditCard(String),
}