use bici_concu::models::payment_method::PaymentMethod;
use serde::Deserialize;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
const BANK_ADDR: &str = "127.0.0.1:4000";

#[derive(Deserialize)]
struct BankRequest {
    payment: PaymentMethod,
    amount: f64,
    user_id: u32,
    station_id: u32,
}

/// cargo run --bin bank
#[tokio::main]
async fn main() -> std::io::Result<()> {
    let addr = BANK_ADDR;
    let Some(port) = BANK_ADDR.split(':').last() else {
        eprintln!("Error: BANK_ADDR no tiene formato IP:PUERTO");
        return Ok(());
    };

    let log_path = PathBuf::from(format!("bank_{}.log", port));

    println!("[Bank] Escuchando en {}", addr);
    let listener = TcpListener::bind(&addr).await?;
    loop {
        let (stream, peer) = listener.accept().await?;
        let log_path = log_path.clone();
        tokio::spawn(async move {
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            if reader.read_line(&mut line).await.is_ok() {
                if let Ok(req) = serde_json::from_str::<BankRequest>(&line) {
                    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&log_path) {
                        let _ = writeln!(f, "RECEIVED_PAYMENT from {}: user_id={} station_id={} payment={:?} AMOUNT={}", peer, req.user_id, req.station_id, req.payment, req.amount);
                        println!("[Bank] Processed payment from {}: user_id={} station_id={} payment={:?} AMOUNT={}", peer, req.user_id, req.station_id, req.payment, req.amount);
                    }
                    // respondo true
                    let mut stream = reader.into_inner();
                    let _ = stream.write_all(b"true\n").await;
                } else {
                    // no pude mapear
                    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&log_path) {
                        let _ = writeln!(f, "INVALID_PAYMENT_JSON from {}: {}", peer, line.trim());
                    }
                    let mut stream = reader.into_inner();
                    let _ = stream.write_all(b"false\n").await;
                }
            }
        });
    }
}
