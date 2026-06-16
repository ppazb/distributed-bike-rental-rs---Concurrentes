use bici_concu::models::location::Location;
use bici_concu::models::payment_method::PaymentMethod;
use bici_concu::models::station_info::StationInfo;
use bici_concu::models::tcp_message::TcpMessage;
use bici_concu::models::trip_info::TripInfo;
use bici_concu::models::user_client::UserClient;
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::time::{timeout, Duration};

fn temp_path(test_name: &str) -> PathBuf {
   PathBuf::from(format!("log_{}.log", test_name))
}

fn station_info(id: u32, location: (usize, usize), addr: &str) -> StationInfo {
    StationInfo {
        station_id: id,
        location,
        addr: addr.to_string(),
    }
}

fn base_user(station_addr: &str) -> UserClient {
    UserClient {
        user_id: 1,
        location: Location { location: (0, 0) },
        known_stations: vec![station_info(0, (5, 5), station_addr)],
        payment_method: PaymentMethod::CreditCard("4111-1111-1111-1111".to_string()),
        current_trip: None,
        trip_counter: 1,
        log_path: temp_path("integration_user_client"),
    }
}

// Creamos una estación mockeada y le pasamos el response que queremos que nos responda
// la estación nos devolverá es response que le pasamos
async fn spawn_mock_station_once(response: TcpMessage) -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("No se pudo bindear estación mock");

    let addr = listener.local_addr().unwrap().to_string();

    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();

        let mut line = String::new();

        {
            let mut reader = BufReader::new(&mut socket);
            reader.read_line(&mut line).await.unwrap();
        }

        let serialized = serde_json::to_string(&response).unwrap() + "\n";
        socket.write_all(serialized.as_bytes()).await.unwrap();
    });

    addr
}

#[tokio::test]
async fn integration_rent_bike_success_sets_current_trip_and_writes_log() {
    let addr = spawn_mock_station_once(TcpMessage::RentResponse {
        success: true,
        message: "Alquiler Consolidado".to_string(),
    })
    .await;

    let mut user = base_user(&addr);
    let target_station = station_info(3, (30, 30), &addr);

    let _ = user.rent_bike(&addr, 1, target_station.clone()).await;

    let trip = user.current_trip.as_ref().unwrap();

    // Chequeamos si el current_trip es correcto
    assert_eq!(trip.trip_id, 1);
    assert_eq!(trip.current_bike_id, 1);
    assert_eq!(trip.last_owner_station.station_id, 3);

    // Chequeamos si se logueó correctamente
    let log_contents = std::fs::read_to_string(&user.log_path).unwrap();
    assert!(log_contents.contains("ACTIVE_TRIP"));

    let _ = std::fs::remove_file(&user.log_path);
}

#[tokio::test]
async fn integration_rent_bike_failure_does_not_create_current_trip() {
    let addr = spawn_mock_station_once(TcpMessage::RentResponse {
        success: false,
        message: "Denegado o falta stock".to_string(),
    })
    .await;

    let mut user = base_user(&addr);
    let target_station = station_info(3, (30, 30), &addr);

    // Si falla el pedido de bicicletas no se actualiza current_trip
    let success = user.rent_bike(&addr, 55, target_station).await;

    assert!(!success);
    assert!(user.current_trip.is_none());
    assert_eq!(user.trip_counter, 1);
}

#[tokio::test]
async fn integration_return_bike_success_clears_current_trip_and_writes_closed_trip() {
    let addr = spawn_mock_station_once(TcpMessage::ReturnResponse {
        success: true,
        message: "Devolución Exitosa".to_string(),
    })
    .await;

    let mut user = base_user(&addr);

    user.current_trip = Some(TripInfo {
        trip_id: 1,
        current_bike_id: 1,
        start_time: chrono::Utc::now(),
        last_owner_station: station_info(3, (30, 30), &addr),
    });

    let success = user.return_bike(&addr).await;

    // Devolución existosa de bici loguea CLOSED_TRIP en el log
    assert!(success);
    assert!(user.current_trip.is_none());

    let log_contents = std::fs::read_to_string(&user.log_path).unwrap();
    assert!(log_contents.contains("CLOSED_TRIP"));

    let _ = std::fs::remove_file(&user.log_path);
}

#[tokio::test]
async fn integration_return_bike_without_current_trip_fails_without_tcp() {
    // No se va a conectar
    let listener = TcpListener::bind("127.0.0.1:9000")
        .await
        .expect("No se pudo bindear listener");

    let addr = listener.local_addr().unwrap().to_string();
    let mut user = base_user(&addr);

    // Debe fallar return_bike
    let result = timeout(Duration::from_millis(200), user.return_bike(&addr)).await;

    assert!(result.is_ok());
    assert!(!result.unwrap());
    assert!(user.current_trip.is_none());
}