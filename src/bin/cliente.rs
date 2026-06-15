use bici_concu::models::location::Location;
use bici_concu::models::payment_method::PaymentMethod;
use bici_concu::models::tcp_message::TcpMessage;
use bici_concu::models::user_client::*; 
use std::env;
use std::io::{self, Write};
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as TokioBufReader, BufReader};

///Main asincrono con manejo de I/O para insertar comandos al usuario (get_bikes, rent etc)

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 5 {
        eprintln!("Sintaxis requerida: <coord_x> <coord_y> <user_id> <ruta_archivo_config>");
        eprintln!("Ejemplo: cargo run --bin user 11 12 src/bin/config/cf1");
        std::process::exit(1);
    }

    let x: usize = args[1].parse().unwrap();
    let y: usize = args[2].parse().unwrap();
    // Obtengo el id pasado por consola 
    let id: u32 = args[3].parse().unwrap();
    let config_path = &args[4];

    // Carga la red automaticamente desde el archivo
    let known_stations = load_stations_from_config(config_path);
    println!("cargadas {} estaciones desde {}", known_stations.len(), config_path);

    let new_log_path = PathBuf::from(format!("user_{}.log", id)); 
    let reconstructed_current_trip = recover_state_from_log(&new_log_path).await; 

    let mut user = UserClient {
        user_id: id,
        location: Location { location: (x, y) },
        known_stations,
        payment_method: PaymentMethod::CreditCard("4512-4412-1102-8854".into()),
        current_trip: reconstructed_current_trip, // reconstructed_current_trip es un Option, se lo paso directo
        trip_counter: 1,
        log_path: new_log_path, // le paso el PathBuf ya construido
    };

    println!("ID de Usuario: {}", user.user_id);

    // Nos conectamos automaticamente a la estacion mas cercana
    let nearest_addr = user.get_nearest_station().addr.clone();
    
    if let Some((st_id, bikes, slots)) = user.request_availability(&nearest_addr).await {
        println!("Conexión establecida. Estación [{}]: {} bicis, {} slots", st_id, bikes.len(), slots);
    } else {
        println!("Error al conectar con la estación más cercana");
    }

    //inicio lector I/O asincrono de tokio para leer la consola
    let mut stdin_reader = TokioBufReader::new(tokio::io::stdin());
    let mut input = String::new();

    //Bucle para leer constatemente los comando que se introducen por consola
    loop {
        print!("> ");
        io::stdout().flush().unwrap();
        input.clear();
        
        if stdin_reader.read_line(&mut input).await.unwrap_or(0) == 0 {
            break;
        }

        let parts: Vec<&str> = input.trim().split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }

        // match de comandos
        match parts[0] {
            "get_bikes" => {

                    // Me conecto a la estación más cercana viva 
                    if let Some(mut stream) = user.connect_to_network().await {
                    
                        // Creo el mensaje de pedido de availability es la misma tanto para cliente como para 
                        // la estación (la estación usa el mismo tipo de mensaje para el forwardeo)
                        let msg = TcpMessage::GetAllStationsAvailability { 
                            origin_station_id: 0, // El cliente arranca con origin_station_id = 0 para que la estación pueda identificar que el mensaje vino de un cliente
                            stations_availability: vec![] 
                        };
                        // Le envío la solicitud a la estación más cercana
                        stream.write_all((serde_json::to_string(&msg).unwrap() + "\n").as_bytes()).await.unwrap();
                        
                        // Espero la respuesta ccon todas disponibles de todas las estaciones
                        let mut reader = BufReader::new(stream);
                        let mut line = String::new();
                        reader.read_line(&mut line).await.unwrap();
                        
                        if let Ok(TcpMessage::GetAllStationsAvailability { stations_availability, .. }) = 
                            serde_json::from_str(&line) {
                                for (st_id, addr, bikes, slots) in stations_availability {
                                    user.update_station_id(&addr, st_id.clone());
                                    println!("ESTACIÓN [{}]: {} bicis, {} slots libres", st_id, bikes.len(), slots);
                                    for bike in bikes {
                                        println!("  - Bici ID: {}", bike);
                                    }
                                }
                        }
                    }
            }
            "rent" => {
                if parts.len() != 3 {
                    println!("Sintaxis: rent <station_id> <bike_id>");
                    continue;
                }
                if user.current_trip.is_some() {
                    println!("Operación denegada: Ya posees un viaje activo");
                    continue;
                }

                let st_id_res = parts[1].parse::<u32>();
                let bk_id_res = parts[2].parse::<u32>();

                if let (Ok(station_id), Ok(bike_id)) = (st_id_res, bk_id_res) {
                    if let Some(station) = user.get_station_by_id(station_id).cloned() {
                        let addr = station.addr.clone();
                        if user.rent_bike(&addr, bike_id, station).await {
                            println!("Alquiler completado Bici ID: {}", bike_id);
                        }
                    } else {
                        println!("Estacion desconocida, lanzar get_bikes para ver la red");
                    }
                } else {
                    println!("Formato ID numérico inválido");
                }
            }
            "return" => {
                if parts.len() != 2 {
                    println!("Sintaxis: return <station_id>");
                    continue;
                }
                let st_id_res = parts[1].parse::<u32>();

                if let Ok(station_id) = st_id_res {
                    if let Some(station) = user.get_station_by_id(station_id).cloned() {
                        let addr = station.addr.clone();
                        if user.return_bike(&addr).await {
                            println!("Devolución completada en estación {}.", station_id);
                        } else {
                            println!("Devolución denegada.");
                        }
                    } else {
                        println!("Estacion desconocida, lanzar get_bikes para ver la red");
                    }
                } else {
                    println!("Formato ID numérico inválido");
                }
            }
            "status" => {
                if let Some(trip) = &user.current_trip {
                    println!("Viaje activo: Bici {}, inicio: {}", trip.current_bike_id, trip.start_time);
                } else {
                    println!("Sin viajes activos.");
                }
            }
            "exit" | "quit" => {
                break;
            }
            _ => {
                println!("Comandos: get_bikes | rent <station_id> <bike_id> | return <station_id> | status | exit");
            }
        }
    }
}