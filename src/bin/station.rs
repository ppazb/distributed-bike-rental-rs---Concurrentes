use actix::prelude::*;
use bici_concu::models::payment_method::PaymentMethod;
use bici_concu::models::station_event::StationEvent;
use bici_concu::models::station_info::StationInfo;
use bici_concu::models::tcp_message::TcpMessage;
use bici_concu::models::trip_info::TripInfo;
use std::collections::{HashMap, VecDeque};
use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

//Declaracion de mesajes de actor estacion
#[derive(Message)]
#[rtype(result = "()")]
struct StartElection;

#[derive(Message)]
#[rtype(result = "()")]
struct HandleIncomingTcp(TcpStream);

#[derive(Message)]
#[rtype(result = "()")]
struct RetryPendingPayments;

#[derive(Message)]
#[rtype(result = "()")]
struct ExecRent {
    user_id: u32,
    bike_id: u32,
}

#[derive(Message)]
#[rtype(result = "()")]
struct ExecReturn {
    user_id: u32,
    bike_id: u32,
    payment: PaymentMethod,
    amount: f64,
    paid: bool,
}

#[derive(Message)]
#[rtype(result = "()")]
struct SetLeaderMsg {
    leader_id: u32,
    leader_addr: String,
}

/// Realiza el ping para verificar si el vecino sigue vivo (siguiente)
#[derive(Message)]
#[rtype(result = "()")]
struct CheckNeighborHealth;

///Corregir la red del anillo
#[derive(Message)]
#[rtype(result = "()")]
struct HealRing;

/// actualiza el siguiente del anillo
#[derive(Message)]
#[rtype(result = "()")]
struct UpdateNextNode(String);


//Mensajes paymentGateway (Pendiente de implementar)
struct PaymentGateway {
    _log_path: PathBuf,
}

impl Actor for PaymentGateway {
    type Context = Context<Self>;
}

#[derive(Message)]
#[rtype(result = "bool")]
struct ProcessPayment {
    _payment: PaymentMethod,
    _amount: f64,
}

impl Handler<ProcessPayment> for PaymentGateway {
    type Result = bool;
    fn handle(&mut self, _msg: ProcessPayment, _ctx: &mut Self::Context) -> Self::Result {
        //loguear al archivo del actor payment
        true
    }
}

// Implementacion actor estacion

struct StationActor {
    station_id: u32, //Id para identificar cada estacion de manera unica, para la estructura de anillo
    addr: String, //Direccion de la estacion
    location: (usize, usize),//representa la ubicacion
    max_slots: usize,//cantidad de slot maxima de bicicleta
    available_bikes: Vec<u32>, // bicicletas en la estacion
    leader: Option<StationInfo>,//Option de la estacion lider, porque puede no tener referencia
    known_stations: Vec<StationInfo>,//todas las estaciones conocidas
    next_station_addr: Option<String>, //mi vecino
    payment_gateway: Addr<PaymentGateway>,//referencia al metodo de pago
    pending_events: VecDeque<StationEvent>,//cola de eventos pendientes para sincronizacion
    pending_payments: VecDeque<(PaymentMethod, f64)>,//cola de pagos pendientes para futuro cobro
    _active_trips: HashMap<u32, TripInfo>, // viajes abiertos, todavia no implementado
    log_path: PathBuf,//path del archivo de configuracion
    is_electing: bool,//si se esta en una eleccion de lider actualmente
    event_counter: u32, //contador de eventos para recuperacion
}

impl StationActor {
    ///Funcion para flushear eventos al archivo log de la estacion
    fn log_event(&self, event_desc: &str) {
        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)
        {
            let _ = writeln!(file, "[{}] {}", chrono::Utc::now(), event_desc);
        }
    }
    ///Funcion asyncrona para enviar los mensajes en formato json por el socket TCP de tokio
    async fn send_tcp(addr: String, msg: TcpMessage) -> Result<(), std::io::Error> {
        if let Ok(mut stream) = TcpStream::connect(addr).await {
            let serialized = serde_json::to_string(&msg)? + "\n";
            stream.write_all(serialized.as_bytes()).await?;
        }
        Ok(())
    }
    //Actualizar el contador de eventos para recuperacion
    fn next_event_id(&mut self) -> u32 {
        let id = self.event_counter;
        self.event_counter += 1;
        id
    }
    ///Funcion para concretar la renta una vez recibido el mensaje
    fn inner_rent(&mut self, user_id: u32, bike_id: u32) {
        self.available_bikes.retain(|&id| id != bike_id);
        let event = StationEvent::BikeRented {
            event_id: self.next_event_id(),
            user_id,
            station_id: self.station_id,
            bike_id,
            timestamp: chrono::Utc::now(),
        };
        self.pending_events.push_back(event);
        self.log_event(&format!("BIKE_RENTED: {}", bike_id));
    }
    ///Funcion para el manejo de retorno de bicicleta una vez recibido el mensaje
    fn inner_return(&mut self, user_id: u32, bike_id: u32, payment: PaymentMethod, amount: f64, paid: bool) {
        self.available_bikes.push(bike_id);
        let event = StationEvent::BikeReturned {
            event_id: self.next_event_id(),
            user_id,
            station_id: self.station_id,
            bike_id,
            timestamp: chrono::Utc::now(),
        };
        self.pending_events.push_back(event);
        self.log_event(&format!("BIKE_RETURNED: {}", bike_id));

        if !paid {
            self.pending_payments.push_back((payment, amount));
            self.log_event("PAYMENT_PENDING_ENQUEUED");
        } else {
            self.log_event(&format!("PAYMENT_PROCESSED: {}", amount));
        }
    }
    ///Setear el nuevo lider para la estacion
    fn set_leader(&mut self, leader_id: u32, leader_addr: String) {
        self.is_electing = false;
        self.leader = Some(StationInfo {
            station_id: leader_id,
            location: (self.location.0, self.location.1),
            addr: leader_addr,
        });
        println!("[Anillo] Nuevo lider: {}", leader_id);
    }
}

/// Implementacion de los hanlders de mensaje

impl Actor for StationActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        println!("estacion {} activa en dirección {}", self.station_id, self.addr);
        self.log_event("STATION_STARTED");

        // Reintento de transacciones
        ctx.run_interval(Duration::from_secs(5), |_actor, ctx| {
            ctx.address().do_send(RetryPendingPayments);
        });

        // Ping al vecino para verificar que esta conectado
        ctx.run_interval(Duration::from_secs(3), |_actor, ctx| {
            ctx.address().do_send(CheckNeighborHealth);
        });

        // Lanza elección inicial
        ctx.address().do_send(StartElection);
    }
}
///Implementacion del handler  del mensaje de ejecutar la renta, llama a la funcion interna para el manejo
impl Handler<ExecRent> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: ExecRent, _ctx: &mut Self::Context) -> Self::Result {
        self.inner_rent(msg.user_id, msg.bike_id);
    }
}
///Implementacion del handler del mensaje de ejecutar el retorno de una bici, llama a la funcion interna para el manejo
impl Handler<ExecReturn> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: ExecReturn, _ctx: &mut Self::Context) -> Self::Result {
        self.inner_return(msg.user_id, msg.bike_id, msg.payment, msg.amount, msg.paid);
    }
}
///Implementacion del handler para el mensaje setear ledir, llama a la funcion interna para el manejo
impl Handler<SetLeaderMsg> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: SetLeaderMsg, _ctx: &mut Self::Context) -> Self::Result {
        self.set_leader(msg.leader_id, msg.leader_addr);
    }
}
///Implementacion del handler para el mensaje actualizar nodo siguiente, mi vecino
impl Handler<UpdateNextNode> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: UpdateNextNode, _ctx: &mut Self::Context) -> Self::Result {
        self.next_station_addr = Some(msg.0.clone());
        println!("[Red] Anillo actualizado tengo un nuevo nodo adyacente: {}", msg.0);
    }
}
/// Handler para verificar que mi adyacente siga en funcionamiento
impl Handler<CheckNeighborHealth> for StationActor {
    type Result = ();
    fn handle(&mut self, _msg: CheckNeighborHealth, ctx: &mut Self::Context) -> Self::Result {
        let next_addr_opt = self.next_station_addr.clone();
        let self_addr = ctx.address();

        ctx.spawn(
            async move {
                if let Some(addr) = next_addr_opt {
                    // Verificación de conexión tcp del adyacente.
                    if TcpStream::connect(&addr).await.is_err() {
                        self_addr.do_send(HealRing);
                    }
                }
            }
            .into_actor(self)
        );
    }
}
///Implementacion del handler del mensaje para ""curar"" la red cuando el anillo, se rompe
impl Handler<HealRing> for StationActor {
    type Result = ();
    fn handle(&mut self, _msg: HealRing, ctx: &mut Self::Context) -> Self::Result {
        
        if self.is_electing { return; }
        self.is_electing = true;
        self.leader = None;

        let mut sorted_stations = self.known_stations.clone();
        sorted_stations.sort_by_key(|s| s.station_id);
        
        let my_id = self.station_id;
        let self_addr = ctx.address();

        ctx.spawn(
            async move {
                if let Some(my_idx) = sorted_stations.iter().position(|s| s.station_id == my_id) {
                    let n = sorted_stations.len();
                    // itera buscando el primer nodo vivo
                    for offset in 1..n {
                        let target_idx = (my_idx + offset) % n;
                        let candidate = &sorted_stations[target_idx];
                        
                        if TcpStream::connect(&candidate.addr).await.is_ok() {
                            self_addr.do_send(UpdateNextNode(candidate.addr.clone()));
                            self_addr.do_send(StartElection);
                            break;
                        }
                    }
                }
            }
            .into_actor(self)
        );
    }
}
///Handler para iniciar elecion de lider
impl Handler<StartElection> for StationActor {
    type Result = ();
    fn handle(&mut self, _msg: StartElection, ctx: &mut Self::Context) -> Self::Result {
        self.is_electing = true;
        if let Some(next_addr) = &self.next_station_addr {
            println!("[Anillo] enviando token del anillo.");
            let target = next_addr.clone();
            let msg = TcpMessage::RingElection {
                highest_id: self.station_id,
            };
            ctx.spawn(
                async move {
                    let _ = Self::send_tcp(target, msg).await;
                }
                .into_actor(self),
            );
        }
    }
}
///Handler para reintentar el pago (payment esta pendiente, todavia no se llama)
impl Handler<RetryPendingPayments> for StationActor {
    type Result = ();
    fn handle(&mut self, _msg: RetryPendingPayments, ctx: &mut Self::Context) -> Self::Result {
        if let Some((method, amount)) = self.pending_payments.pop_front() {
            let gateway = self.payment_gateway.clone();
            let self_addr = ctx.address();
            ctx.spawn(
                async move {
                    let success = gateway.send(ProcessPayment { _payment: method.clone(), _amount: amount }).await.unwrap_or(false);
                    if !success {
                        self_addr.do_send(StartElection);
                    }
                }
                .into_actor(self),
            );
        }
    }
}
///Hanlder para manejar la llegada de mensajes TCP y actuar en base a este
impl Handler<HandleIncomingTcp> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: HandleIncomingTcp, ctx: &mut Self::Context) -> Self::Result {
        let mut stream = msg.0;
        let self_addr = ctx.address();
        let station_id = self.station_id;
        let bikes = self.available_bikes.clone();
        let max_slots = self.max_slots;
        let next_station = self.next_station_addr.clone();
        let gateway = self.payment_gateway.clone();
        let current_addr = self.addr.clone();

        ctx.spawn(
            async move {
                let mut reader = BufReader::new(&mut stream);
                let mut line = String::new();
                
                if reader.read_line(&mut line).await.is_ok() {
                    if let Ok(tcp_msg) = serde_json::from_str::<TcpMessage>(&line) {
                        match tcp_msg {
                            TcpMessage::GetBikes => {
                                let free_slots = max_slots.saturating_sub(bikes.len());
                                let resp = TcpMessage::Availability {
                                    station_id,
                                    bikes_available: bikes,
                                    free_slots,
                                };
                                let serialized = serde_json::to_string(&resp).unwrap() + "\n";
                                let _ = stream.write_all(serialized.as_bytes()).await;
                            }
                            TcpMessage::RentBike { user_id, bike_id, payment_method } => {
                                let auth = gateway.send(ProcessPayment { _payment: payment_method, _amount: 500.0 }).await.unwrap_or(false);
                                
                                if auth && bikes.contains(&bike_id) {
                                    self_addr.do_send(ExecRent { user_id, bike_id });
                                    let resp = TcpMessage::RentResponse { success: true, message: "Alquiler Consolidado".into() };
                                    let _ = stream.write_all((serde_json::to_string(&resp).unwrap() + "\n").as_bytes()).await;
                                } else {
                                    let resp = TcpMessage::RentResponse { success: false, message: "Transacción denegada o falta de stock".into() };
                                    let _ = stream.write_all((serde_json::to_string(&resp).unwrap() + "\n").as_bytes()).await;
                                }
                            }
                            TcpMessage::ReturnBike { user_id, bike_id, payment_method, trip_info } => {
                                if bikes.len() >= max_slots {
                                    let resp = TcpMessage::ReturnResponse { success: false, message: "Capacidad excedida".into() };
                                    let _ = stream.write_all((serde_json::to_string(&resp).unwrap() + "\n").as_bytes()).await;
                                } else {
                                    let duration = (chrono::Utc::now() - trip_info.start_time).num_minutes() as f64;
                                    let amount = duration * 10.0;
                                    let paid = gateway.send(ProcessPayment { _payment: payment_method.clone(), _amount: amount }).await.unwrap_or(false);
                                    
                                    self_addr.do_send(ExecReturn { user_id, bike_id, payment: payment_method, amount, paid });
                                    
                                    let resp = TcpMessage::ReturnResponse { success: true, message: "Devolución Exitosa".into() };
                                    let _ = stream.write_all((serde_json::to_string(&resp).unwrap() + "\n").as_bytes()).await;
                                }
                            }
                            //Todo: Falta logica de caso else, provoca un bug de falta de lider
                            TcpMessage::RingElection { highest_id } => {
                                if highest_id > station_id {
                                    if let Some(next) = next_station {
                                        let _ = Self::send_tcp(next, TcpMessage::RingElection { highest_id }).await;
                                    }
                                } else if highest_id == station_id {
                                    println!("[Anillo] eleccion terminadanuevo líder ID: {}", station_id);
                                    if let Some(next) = next_station {
                                        let _ = Self::send_tcp(next, TcpMessage::RingElected { leader_id: station_id, leader_addr: current_addr }).await;
                                    }
                                }
                            }
                            TcpMessage::RingElected { leader_id, leader_addr } => {
                                self_addr.do_send(SetLeaderMsg { leader_id, leader_addr: leader_addr.clone() });
                                if let Some(next) = next_station {
                                    if leader_id != station_id {
                                        let _ = Self::send_tcp(next, TcpMessage::RingElected { leader_id, leader_addr }).await;
                                    }
                                }
                            }
                            //Todo: Mensajes respuestas al cliente, no se utiliza nada de su payload, queda pendiente por profundizar
                            TcpMessage::Availability { .. } |
                            TcpMessage::RentResponse { .. } |
                            TcpMessage::ReturnResponse { .. } => {
                                println!("[Red] Advertencia: Paquete en formato incorrecto descartado.");//Caso mensaje TCP no llego en ningun formato valido
                            }
                        }
                    }
                }
            }
            .into_actor(self)
        );
    }
}

//Funciones auxiliares

///Lee desde el archivo de configuracion, la estructura de la red del anillo, es decir
/// cuantas estaciones, su direccion y puerto para establecer la red
fn load_stations_from_config(path: &str) -> Vec<StationInfo> {
    let contents = fs::read_to_string(path).unwrap_or_else(|_| {
        eprintln!("Error: imposible resolver acceso a archivo de configuración: '{}'", path);
        std::process::exit(1);
    });

    let mut stations = Vec::new();
    
    for line in contents.lines() {
        let line = line.trim();
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
        }
    }

    if stations.is_empty() {
        eprintln!("Error en archivo de configuración para estacion");
        std::process::exit(1);
    }
    stations
}


#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        eprintln!("Sintaxis requerida: cargo run --bin station <station_id> <max_slots> <ruta_archivo_config>");
        std::process::exit(1);
    }

    let station_id: u32 = args[1].parse().unwrap();
    let max_slots: usize = args[2].parse().unwrap();
    let config_path = &args[3];

    let mut known_stations = load_stations_from_config(config_path);
    //Lanza error si el archivo no tiene la estructura esperada para los id de la configuracion
    let self_station = known_stations.iter().find(|s| s.station_id == station_id).unwrap_or_else(|| {
        eprintln!("Error: ID {} no localizado en la red configurada.", station_id);
        std::process::exit(1);
    }).clone();

    // Ordena y busca cual seria el vecino (la estacion con el id mio + 1)
    known_stations.sort_by_key(|s| s.station_id);
    let my_idx = known_stations.iter().position(|s| s.station_id == station_id).unwrap();
    let next_idx = (my_idx + 1) % known_stations.len();
    let next_station_addr = Some(known_stations[next_idx].addr.clone());

    let port = self_station.addr.split(':').last().unwrap();

    let gateway = PaymentGateway {
        _log_path: PathBuf::from(format!("gateway_{}.log", port)),
    }.start();

    let initial_bikes: Vec<u32> = (1..=10).collect();

    let station_actor = StationActor {
        station_id,
        addr: self_station.addr.clone(),
        location: self_station.location,
        max_slots,
        available_bikes: initial_bikes,
        leader: None,
        known_stations,
        next_station_addr,
        payment_gateway: gateway,
        pending_events: VecDeque::new(),
        pending_payments: VecDeque::new(),
        _active_trips: HashMap::new(),
        log_path: PathBuf::from(format!("station_{}.log", port)),
        is_electing: false,
        event_counter: 1,
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

    tokio::signal::ctrl_c().await?;
    Ok(())
}