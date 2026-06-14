use bici_concu::models::location::Location;
use bici_concu::models::payment_method::PaymentMethod;
use bici_concu::models::station_info::StationInfo;
use bici_concu::models::tcp_message::TcpMessage;
use bici_concu::models::trip_info::TripInfo;
use std::env;
use std::fs;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as TokioBufReader, BufReader};
use tokio::net::TcpStream;


/// contiene el estado de la aplicación usuario en el "telefono"
struct UserClient {
    user_id: u32,
    location: Location,// ubicacion simulada del cliente
    known_stations: Vec<StationInfo>,//todas las estaciones  del sistema, cargada por archivo config
    payment_method: PaymentMethod,//Metodo de pago
    current_trip: Option<TripInfo>,//VIaje actual, o none
    trip_counter: u32,// contador de viaje para identificador de los viajes activos
    log_path: PathBuf
}

impl UserClient {

    // Logeo de eventos para la reconstrucción, igual que en Station
    fn log_event(&self, event_desc: &str) {
        if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&self.log_path) {
            let _ = writeln!(file, "[{}] {}", chrono::Utc::now(), event_desc);
        }
    }

    // Extrae la lógia de cálculo de distancias 
    fn distance_to(&self, station: &StationInfo) -> isize {
        let dx = (station.location.0 as isize - self.location.location.0 as isize).abs();
        let dy = (station.location.1 as isize - self.location.location.1 as isize).abs();
        dx * dx + dy * dy 
    }

    /// Me devuelve la estación más cercana de acuerdo a la distancia calculada
    fn get_nearest_station(&self) -> &StationInfo {
        self.known_stations
            .iter()
            .min_by_key(|station| self.distance_to(station))
            .unwrap()
    }

    /// Usado en get_bikes para que me devuelvan la estación más cercana viva
    async fn connect_to_network(&self) -> Option<TcpStream> {
        let mut stations = self.known_stations.clone();
        
        stations.sort_by_key(|station| self.distance_to(station));

        for station in stations {
            if let Ok(stream) = TcpStream::connect(&station.addr).await {
                return Some(stream);
            }
        }
        // Devuelve None si ninguna de las estaciones está viva 
        None
    }

    /// Busca una estación entre las estaciones conocidas local mediante su ID
    fn get_station_by_id(&self, id: u32) -> Option<&StationInfo> {
        self.known_stations.iter().find(|s| s.station_id == id)
    }

    /// Actualiza el ID de una estación entre las estaciones conocidas
    fn update_station_id(&mut self, addr: &str, new_id: u32) {
        if let Some(st) = self.known_stations.iter_mut().find(|s| s.addr == addr) {
            st.station_id = new_id;
        }
    }

    /// Envia el mensaje `GetBikes` y espera la disponibilidad
    async fn request_availability(&mut self, addr: &str) -> Option<(u32, Vec<u32>, usize)> {
        // Establece conexión TCP temporal
        let mut stream = TcpStream::connect(addr).await.ok()?;
        
        let msg = TcpMessage::GetBikes;
        let serialized = serde_json::to_string(&msg).unwrap() + "\n";
        stream.write_all(serialized.as_bytes()).await.ok()?;

        // Espera lectura de la respuesta JSON
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).await.ok()?;

        //Codigo para setear la estacion mas cercana al incio
        if let Ok(TcpMessage::Availability { station_id, bikes_available, free_slots }) = serde_json::from_str(&line) {
            self.update_station_id(addr, station_id);
            return Some((station_id, bikes_available, free_slots));
        }
        None
    }

    /// formulacion de mensaje para rentar bici de una estacion y verificar el resultado
    async fn rent_bike(&mut self, addr: &str, bike_id: u32, target_station: StationInfo) -> bool {
        let mut stream = match TcpStream::connect(addr).await {
            Ok(s) => s,
            Err(_) => return false,
        };
        
        let msg = TcpMessage::RentBike {
            user_id: self.user_id,
            bike_id,
            payment_method: self.payment_method.clone(),
        };
        stream.write_all((serde_json::to_string(&msg).unwrap() + "\n").as_bytes()).await.unwrap();

        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();

        // Validamos la confirmación del servidor
        if let Ok(TcpMessage::RentResponse { success, .. }) = serde_json::from_str(&line) {
            if success {
                let trip_id = self.trip_counter;
                self.trip_counter += 1;
                // Si fue exitoso, creamos el viaje actual y lo asignamos al usuario
                let current_trip = TripInfo {
                    trip_id,
                    current_bike_id: bike_id,
                    start_time: chrono::Utc::now(),
                    last_owner_station: target_station,
                }; 
                self.log_event(&format!("ACTIVE_TRIP: {}", serde_json::to_string(&current_trip).unwrap()));
                self.current_trip = Some(current_trip);
                return true;
            }
        }
        false
    }

    /// formulacion de mensaje para devolver bicicleta
    async fn return_bike(&mut self, addr: &str) -> bool {
        // No se puede devolver si no hay viaje en curso
        if self.current_trip.is_none() {
            return false;
        }
        
        let mut stream = match TcpStream::connect(addr).await {
            Ok(s) => s,
            Err(_) => return false,
        };
        
        let trip = self.current_trip.clone().unwrap();
        let msg = TcpMessage::ReturnBike {
            user_id: self.user_id,
            bike_id: trip.current_bike_id,
            payment_method: self.payment_method.clone(),
            trip_info: trip,
        };
        stream.write_all((serde_json::to_string(&msg).unwrap() + "\n").as_bytes()).await.unwrap();

        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();

        if let Ok(TcpMessage::ReturnResponse { success, .. }) = serde_json::from_str(&line) {
            if success {
                // si el viaje termino, desreferenciamos el viaje
                self.log_event(&format!("CLOSED_TRIP: {}", serde_json::to_string(&self.current_trip).unwrap()));
                self.current_trip = None;
                return true;
            }
        }
        false
    }
}

// Funciones Auxiliares

/// Función para la recuperación del último viaje activo no cerrado del usuario
/// como el usuario solamente puede tener un único current_trip, entonces solamente
/// me importa la última línea del log, si la última línea es un ACTIVE_TRIP
/// si es CLOSED TRIP significa que no hay ningún viaje activo 
async fn recover_state_from_log(log_path: &PathBuf) -> Option<TripInfo> {
        
        let log_contents = match tokio::fs::read_to_string(log_path).await {
            Ok(content) => content,
            Err(_) => {
                println!("[LOG] No existe log previo del usuario.");
                return None;
            }
        };

        // Busco la última línea no vacía del log 
        let last_line = log_contents
            .lines()
            .rev()
            .find(|line| !line.trim().is_empty())?;

        // Si es CLOSED_TRIP significa que el viaje ya fue cerrado, no hay ninguno pendiente
        if last_line.contains("CLOSED_TRIP") {
            println!("[LOG] Último estado: viaje cerrado.");
            return None;
        }; 

        // Busco la posición desde donde arranca ACTIVE_TRIP
        if let Some(pos) = last_line.find("ACTIVE_TRIP:") {
            // Obtengo el trim de json donde debería estar station_info guardado
            let json_trip_info = last_line[pos + "ACTIVE_TRIP:".len()..].trim();

            // Parseo TripInfo de json si no me da OK signfica que hubo error de parseo 
            // Sino devuelvo el trip_info parseado 
            match serde_json::from_str::<TripInfo>(json_trip_info) {
                Ok(trip_info) => {
                    println!("[LOG] viaje activo recuperado: bike_id={}", trip_info.current_bike_id);
                    return Some(trip_info);
                }
                Err(e) => {
                    println!("[LOG] error de parseo: {}", e);
                    return None;
                }
            }
        }

        println!("[LOG] log vacío");
        None
}

/// Parsea un archivo de texto plano para construir la red de estaciones
fn load_stations_from_config(path: &str) -> Vec<StationInfo> {
    let contents = fs::read_to_string(path).unwrap_or_else(|_| {
        eprintln!("Error: No se pudo leer el archivo de configuración en '{}'", path);
        std::process::exit(1);
    });

    let mut stations = Vec::new();
    
    for (line_num, line) in contents.lines().enumerate() {
        let line = line.trim();
        // Ignorar líneas vacías o comentarios
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() == 4 {
            let id: u32 = parts[0].parse().unwrap_or(0);
            let x: usize = parts[1].parse().unwrap_or(0);
            let y: usize = parts[2].parse().unwrap_or(0);
            let addr = parts[3].to_string();
            
            stations.push(StationInfo {
                station_id: id,
                location: (x, y),
                addr,
            });
        } else {
            eprintln!("formato invalido en línea {} del config.", line_num + 1);
        }
    }

    if stations.is_empty() {
        eprintln!("el archivo de configuracion no contiene estaciones validas.");
        std::process::exit(1);
    }

    stations
}



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
        user_id: 1,
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
                        } else {
                            println!("Alquiler denegado");
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