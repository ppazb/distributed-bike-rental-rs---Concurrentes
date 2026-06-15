use actix::prelude::*;
use bici_concu::models::station_actor::*; 
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use tokio::io::AsyncBufReadExt;
use tokio::net::TcpListener;

///Main asyncrono para poder manejar el lanzamiento del actor, asi como tambien las operaciones I/O
#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        eprintln!("Sintaxis requerida: cargo run --bin station <station_id> <max_slots> <ruta_archivo_config>");
        std::process::exit(1);
    }

    let station_id: u32 = args[1].parse().unwrap();
    let mut max_slots: usize = args[2].parse().unwrap();
    let config_path = &args[3];

    let known_stations = load_stations_from_config(config_path);
    // Se busca a sí mismo y se clona su propio StationInfo
    let self_station = known_stations.iter().find(|s| s.station_id == station_id).unwrap_or_else(|| {
        eprintln!("Error: ID {} no localizado en red configurada.", station_id);
        std::process::exit(1);
    }).clone();

    let port = self_station.addr.split(':').last().unwrap();
    let bank_addr = BANK_ADDR.to_string();
    let gateway = PaymentGateway { bank_addr }.start();
    let start = 1 + max_slots as u32 * (station_id - 1);
    let end = station_id * max_slots as u32;
    let initial_bikes: Vec<u32> = (start..=end).collect();
    let log_path = PathBuf::from(format!("station_{}.log", port));
    max_slots = max_slots + 3;
    
    // Desestructuramos la tupla ampliada que ahora también trae los recovered_pending_events y recovered_pending_payments
    // Nos fijamos de las bicis iniciales cuáles tenemos de acuerdo al log
    let (recovered_bikes, recovered_event_counter, recovered_pending_events, recovered_pending_payments) = recover_state_from_log(&log_path, initial_bikes);

    let station_actor = StationActor {
        station_id,
        addr: self_station.addr.clone(),
        location: self_station.location,
        max_slots,
        available_bikes: recovered_bikes, //Ahora usamos el "recuperado" que vale lo del config en caso de primer lanzamiento
        leader: None,
        known_stations,
        next_station_addr: None,
        payment_gateway: gateway,
        pending_events: recovered_pending_events, //Inicializamos la cola con los eventos recuperados desde el log
        pending_payments: recovered_pending_payments,
        active_trips: HashMap::new(), // Solamente si la estación es líder
        // Cuando la red muere, puede pasar que me llegue un evento de Return que no matchea 
        // en la recuperación. Un Return Huérfanos que no tiene el evento de Rent
        orphan_returns: HashMap::new(),
        log_path: PathBuf::from(format!("station_{}.log", port)),
        is_electing: false,
        event_counter: recovered_event_counter, //Ahora usamos el "recuperado" que vale 0 si es la primera vez que se lanza
        is_disconnected: false, //Para simular desconexion de la red
        logically_dead: Vec::new(),
    }.start();

    let listener = TcpListener::bind(&self_station.addr).await?;
    let cloned_actor = station_actor.clone();
    
    tokio::spawn(async move {
        loop {
            if let Ok((stream, _)) = listener.accept().await {
                cloned_actor.do_send(HandleIncomingTcp(stream));
            }
        }
    });

    // Reader de la consola 
    let actor_for_stdin = station_actor.clone();
    tokio::spawn(async move {
        let mut reader = tokio::io::BufReader::new(tokio::io::stdin());
        let mut line = String::new();
        loop {
            line.clear();
            if reader.read_line(&mut line).await.is_ok() {
                if line.trim() == "viajes" {
                    actor_for_stdin.do_send(ConsoleQueryTrips);
                } 
                else if line.trim() == "desconectar" {
                    actor_for_stdin.do_send(ConsoleDisconnect);
                } else if line.trim() == "conectar" {
                    actor_for_stdin.do_send(ConsoleConnect);
                }
            }
        }
    });

    // Cuando le mandemos ctrl + c se mata
    tokio::signal::ctrl_c().await?;
    Ok(())
}