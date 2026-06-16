use actix::prelude::*;
use bici_concu::models::payment_method::PaymentMethod;
use bici_concu::models::station_actor::{HandleIncomingTcp, PaymentGateway, StationActor};
use bici_concu::models::station_info::StationInfo;
use bici_concu::models::tcp_message::TcpMessage;
use bici_concu::models::trip_info::TripInfo;

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, timeout, Duration};

fn temp_log_path(test_name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("integration_test_station_{}.log", test_name))
}

// Función para crear StationInfo
fn station_info(id: u32, addr: &str) -> StationInfo {
    StationInfo {
        station_id: id,
        location: (0, 0),
        addr: addr.to_string(),
    }
}

async fn start_station_with_listener(
    available_bikes: Vec<u32>,
    max_slots: usize,
) -> (String, Addr<StationActor>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("No se pudo bindear listener de test");

    let addr = listener.local_addr().unwrap().to_string();

    let gateway = PaymentGateway {
        bank_addr: "127.0.0.1:4000".to_string(),
    }
    .start();

    let station_actor = StationActor {
        station_id: 1,
        addr: addr.clone(),
        location: (0, 0),
        max_slots,
        available_bikes,
        leader: None,
        known_stations: vec![station_info(1, &addr)],
        next_station_addr: None,
        payment_gateway: gateway,
        pending_events: VecDeque::new(),
        pending_payments: VecDeque::new(),
        active_trips: HashMap::new(),
        orphan_returns: HashMap::new(),
        log_path: temp_log_path("integration_station"),
        is_electing: false,
        event_counter: 1,
        is_disconnected: true,
        logically_dead: Vec::new(),
    }
    .start();

    let actor_for_listener = station_actor.clone();

    // Preparamos el listener de conexiones
    tokio::spawn(async move {
        loop {
            match timeout(Duration::from_secs(5), listener.accept()).await {
                Ok(Ok((stream, _))) => {
                    actor_for_listener.do_send(HandleIncomingTcp(stream));
                }
                _ => break,
            }
        }
    });

    (addr, station_actor)
}

async fn send_tcp_message(addr: &str, msg: TcpMessage) -> TcpMessage {
    let mut stream = TcpStream::connect(addr)
        .await
        .expect("No se pudo conectar a la estación");

    let serialized = serde_json::to_string(&msg).unwrap() + "\n";
    stream.write_all(serialized.as_bytes()).await.unwrap();

    let mut reader = BufReader::new(stream);
    let mut line = String::new();

    // Leemos la respuesta de Station
    timeout(Duration::from_secs(2), reader.read_line(&mut line))
        .await
        .expect("Timeout esperando respuesta")
        .expect("Error leyendo respuesta");

    // Deserializamos el mensaje 
    serde_json::from_str::<TcpMessage>(&line).expect("Respuesta TCP inválida")
}

#[actix_rt::test]
async fn integration_get_bikes_returns_current_availability() {
    let (addr, _actor) = start_station_with_listener(vec![1, 2, 3], 5).await;

    let response = send_tcp_message(&addr, TcpMessage::GetBikes).await;

    match response {
        TcpMessage::Availability {
            station_id,
            bikes_available,
            free_slots,
        } => {
            assert_eq!(station_id, 1);
            assert_eq!(bikes_available, vec![1, 2, 3]);
            assert_eq!(free_slots, 2);
        }
        other => panic!("Se esperaba Availability, llegó {:?}", other),
    }
}

#[actix_rt::test]
async fn integration_return_bike_accepts_bike_and_updates_availability() {
    // Estación acepta una bici y aparece correctamente cuando hacemos GetBikes
    let (addr, _actor) = start_station_with_listener(vec![1], 3).await;

    let trip_info = TripInfo {
        trip_id: 1,
        current_bike_id: 2,
        start_time: chrono::Utc::now(),
        last_owner_station: station_info(1, &addr),
    };

    let return_response = send_tcp_message(
        &addr,
        TcpMessage::ReturnBike {
            user_id: 10,
            bike_id: 2,
            payment_method: PaymentMethod::CreditCard("1111-1111-1111-1111".to_string()),
            trip_info,
        },
    )
    .await;

    match return_response {
        TcpMessage::ReturnResponse { success, .. } => {
            assert!(success, "La devolución debería ser aceptada");
        }
        other => panic!("Se esperaba ReturnResponse, llegó {:?}", other),
    }

    // Esperamos un instante para que sincronice Station
    sleep(Duration::from_millis(500)).await;

    let availability_response = send_tcp_message(&addr, TcpMessage::GetBikes).await;

    match availability_response {
        TcpMessage::Availability {
            bikes_available,
            free_slots,
            ..
        } => {
            assert!(bikes_available.contains(&2));
            assert_eq!(bikes_available.len(), 2);
            assert_eq!(free_slots, 1);
        }
        other => panic!("Se esperaba Availability, llegó {:?}", other),
    }
}

#[actix_rt::test]
async fn integration_return_bike_rejects_when_station_is_full() {
    // Nuestra estación tiene dos bicis y solamente puede aceptar 2 bicis, está llena, no se debe poder acpetar más bicis
    let (addr, _actor) = start_station_with_listener(vec![1, 2], 2).await;

    let trip_info = TripInfo {
        trip_id: 1,
        current_bike_id: 5,
        start_time: chrono::Utc::now(),
        last_owner_station: station_info(1, &addr),
    };

    // Le envío al StationActor el mensaje de ReturnBike
    let response = send_tcp_message(
        &addr,
        TcpMessage::ReturnBike {
            user_id: 10,
            bike_id: 5,
            payment_method: PaymentMethod::CreditCard("4111-1111-1111-1111".to_string()),
            trip_info,
        },
    )
    .await;

    match response {
        TcpMessage::ReturnResponse { success, message } => {
            assert!(!success);
            assert!(
                message.contains("Capacidad") || message.contains("excedida"),
                "Mensaje inesperado: {}",
                message
            );
        }
        other => panic!("Se esperaba ReturnResponse, llegó {:?}", other),
    }
}