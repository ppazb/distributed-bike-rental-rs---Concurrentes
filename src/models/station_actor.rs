use actix::prelude::*;
use super::payment_method::PaymentMethod;
use super::station_event::StationEvent;
use super::station_info::StationInfo;
use super::tcp_message::TcpMessage;
use super::trip_info::TripInfo;
use std::collections::{HashMap, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::{Write};
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

pub const BANK_ADDR: &str = "127.0.0.1:4000";

// Declaración de mesajes de actor estacion

#[derive(Message)]
#[rtype(result = "()")]
struct Bootup;

#[derive(Message)]
#[rtype(result = "()")]
struct VerifyLeaderTimeout;

#[derive(Message)]
#[rtype(result = "()")]
struct StartElection;

#[derive(Message)]
#[rtype(result = "()")]
pub struct HandleIncomingTcp(pub TcpStream);

#[derive(Message)]
#[rtype(result = "()")]
struct RetryPendingPayments;

#[derive(Message)]
#[rtype(result = "()")]
struct EnqueueFailedPayment {
    payment: PaymentMethod,
    amount: f64,
    user_id: u32,
    station_id: u32,
}

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
    start_time: chrono::DateTime<chrono::Utc>,
    payment: PaymentMethod,
}

#[derive(Message)]
#[rtype(result = "()")]
struct SetLeaderMsg {
    leader_id: u32,
    leader_addr: String,
}

#[derive(Message)]
#[rtype(result = "()")]
struct CheckNeighborHealth;

#[derive(Message)]
#[rtype(result = "()")]
struct HealRing {
    dead_addr: String,
}

#[derive(Message)]
#[rtype(result = "()")]
struct UpdateNextNode(String);

#[derive(Message)]
#[rtype(result = "()")]
struct DeclareSelfLeader;

#[derive(Message)]
#[rtype(result = "()")]
pub struct ConsoleQueryTrips;

#[derive(Message)]
#[rtype(result = "()")]
struct SyncEventToLeader {
    origin_addr: String,
    event: StationEvent,
}

#[derive(Message)]
#[rtype(result = "()")]
struct ReceiveAck(u32);

#[derive(Message)]
#[rtype(result = "()")]
struct ReceiveClosedTrip(u32);

#[derive(Message)]
#[rtype(result = "()")]
struct ReplayPendingEvents;

#[derive(Message)]
#[rtype(result = "()")]
struct ExecuteReplay;

// Mensajes de procesamiento del anillo
#[derive(Message)]
#[rtype(result = "()")]
struct ProcessRingElection(Vec<u32>);

#[derive(Message)]
#[rtype(result = "()")]
struct ProcessRingElected {
    active_nodes: Vec<u32>,
    leader_id: u32,
    leader_addr: String,
    initiator_id: u32,
}

#[derive(Message)]
#[rtype(result = "()")]
struct ProcessWhoIsLeader(String);

#[derive(Message)]
#[rtype(result = "()")]
struct ProcessQueryTrips {
    origin_addr: String,
    ttl: usize,
}

#[derive(Message)]
#[rtype(result = "()")]
struct RequestPendingEventsMsg {
    origin_addr: String,
    ttl: usize,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct ConsoleDisconnect;

#[derive(Message)]
#[rtype(result = "()")]
pub struct ConsoleConnect;

#[derive(Message)]
#[rtype(result = "()")]
struct RegisterDeadNode(String);

#[derive(Message)]
#[rtype(result = "()")]
struct RegisterAliveNode(String);


//Mensajes paymentGateway (Pendiente de implementar)
pub struct PaymentGateway {
    pub bank_addr: String,
}

impl Actor for PaymentGateway {
    type Context = Context<Self>;
}

#[derive(Message)]
#[rtype(result = "()")]
struct ProcessPayment {
    payment: PaymentMethod,
    amount: f64,
    user_id: u32,
    station_id: u32,
    reply_to: Option<tokio::sync::mpsc::Sender<bool>>,
}

impl Handler<ProcessPayment> for PaymentGateway {
    type Result = ();
    fn handle(&mut self, msg: ProcessPayment, _ctx: &mut Self::Context) -> Self::Result {
        let reply_to = msg.reply_to.clone();
        let bank_addr = self.bank_addr.clone();

        tokio::spawn(async move {
            let payment = msg.payment;
            let amount = msg.amount;
            let user_id = msg.user_id;
            let station_id = msg.station_id;

            // construyo mensaje
            let payload = match serde_json::to_string(&serde_json::json!({ "payment": payment, "amount": amount, "user_id": user_id, "station_id": station_id })) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("[PaymentGateway] Error al serializar pago para banco: {}", e);
                    if let Some(tx) = reply_to { let _ = tx.send(false).await; }
                    return;
                }
            };

            // conecto al banco
            let result = match TcpStream::connect(bank_addr.clone()).await {
                Ok(mut stream) => {
                    if let Err(e) = stream.write_all(format!("{}\n", payload).as_bytes()).await {
                        eprintln!("[PaymentGateway] Error al enviar pago al banco: {}", e);
                        false
                    } else {
                        let mut reader = BufReader::new(stream);
                        let mut resp = String::new();
                        if let Ok(_) = reader.read_line(&mut resp).await {
                            let resp_trim = resp.trim();
                            if resp_trim == "true" {
                                true
                            } else {
                                //caso no deberia existir: el banco responde siempre true a menos q se envie formato equivocado
                                eprintln!("[PaymentGateway] Pago rechazado por el banco.");
                                false
                            }
                        } else {
                            eprintln!("[PaymentGateway] Error al leer respuesta del banco.");
                            false
                        }
                    }
                }
                //va a ocurrir cuando esta desconectado el banco
                Err(e) => {
                    eprintln!("[PaymentGateway] Error al conectar con el banco {}: {}", bank_addr, e);
                    false
                }
            };

            if let Some(tx) = reply_to { let _ = tx.send(result).await; }
        });
    }
}

// Implementación del Actor StationActor 

pub struct StationActor {
    pub station_id: u32, // Id para identificar cada estación de manera única, para la estructura de anillo
    pub addr: String, // Dirección de la estación
    pub location: (usize, usize),// Representa la ubicacion
    pub max_slots: usize,//cantidad de slot maxima de bicicleta
    pub available_bikes: Vec<u32>, // bicicletas en la estacion
    pub leader: Option<StationInfo>,//Option de la estacion lider, porque puede no tener referencia
    pub known_stations: Vec<StationInfo>,//todas las estaciones conocidas
    pub next_station_addr: Option<String>, //mi vecino en el anillo
    pub payment_gateway: Addr<PaymentGateway>,//direccion del metodo de pago
    pub pending_events: VecDeque<StationEvent>,//cola de eventos pendientes para sincronizacion
    pub pending_payments: VecDeque<(PaymentMethod, f64, u32, u32)>,//cola de pagos pendientes para futuro cobro (payment, amount, user_id, station_id)
    pub active_trips: HashMap<u32, TripInfo>, // viajes abiertos,para mantener vision global de la red, solo el lider la usa
    pub orphan_returns: HashMap<u32, (String, StationEvent)>,//hashmap para devoluciones que llegaron al líder antes que el aviso de alquiler (por lag)
    pub log_path: PathBuf,//path del archivo para escribir el log de la estacion
    pub is_electing: bool,//si se esta en una eleccion de lider actualmente
    pub event_counter: u32,//contador de eventos para generar eventos unicos
    pub is_disconnected: bool, // Determina si la estación debe ignorar los eventos de anillo
    pub logically_dead: Vec<String>, // Lista de nodos que avisaron que se desconectaron, para no pingearlos
}

impl StationActor {

    ///Función para flushear eventos al archivo log de la estación
    fn log_event(&self, event_desc: &str) {
        if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&self.log_path) {
            let _ = writeln!(file, "[{}] {}", chrono::Utc::now(), event_desc);
        }
    }

    ///Helper para enviar pagos y esperar respuesta
    async fn send_payment(gateway: Addr<PaymentGateway>, payment: PaymentMethod, amount: f64, user_id: u32, station_id: u32) -> bool {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        gateway.do_send(ProcessPayment { payment, amount, user_id, station_id, reply_to: Some(tx) });
        rx.recv().await.unwrap_or(false)
    }

    ///Funcion asyncrona para enviar los mensajes a otra estacion partiendo de formato json, usa serde
    /// para serializar y enviar el por el TCP socket de tokio
    async fn send_tcp(addr: String, msg: TcpMessage) -> Result<(), std::io::Error> {
        if let Ok(mut stream) = TcpStream::connect(addr).await {
            let serialized = serde_json::to_string(&msg)? + "\n";
            stream.write_all(serialized.as_bytes()).await?;
        }
        Ok(())
    }
    // Devuelve ID actual y suma uno al contador
    fn next_event_id(&mut self) -> u32 {
        let id = self.event_counter;
        self.event_counter += 1;
        id
    }

    /// Actualizo el lider, verificando si antes yo era líder para limpiar mi registro de viajes
    fn set_leader(&mut self, leader_id: u32, leader_addr: String) {
        let was_leader = self.leader.as_ref().map_or(false, |l| l.station_id == self.station_id);
        self.is_electing = false;
        
        self.leader = Some(StationInfo {
            station_id: leader_id,
            location: self.location,
            addr: leader_addr,
        });

        // Prevención de datos residuales. Solo el nuevo líder retiene autoridad sobre el registro global.
        if was_leader && leader_id != self.station_id {
            self.active_trips.clear();
            self.orphan_returns.clear();
            println!("[Anillo] Rol de líder revocado pacíficamente. TripRegistry descartado.");
        }

        println!("[Anillo] Líder actual de la red establecido: {}", leader_id);
    }

}

/// Implementacion de los hanlders de mensaje

impl Actor for StationActor {
    type Context = Context<Self>;
    /// Función que se llama al iniciar un actor en actix, permite configurar runtimes automaticos
    /// con timers
    fn started(&mut self, ctx: &mut Self::Context) {
        println!("Estación {} activa en dirección {}", self.station_id, self.addr);
        self.log_event("STATION_STARTED");
        // Lanzamos una tarea para reintentar los pagos pendientes cada 5 segundos
        ctx.run_interval(Duration::from_secs(5), |_actor, ctx| {
            ctx.address().do_send(RetryPendingPayments);
        });
        // Lanzamos una tarea para verificar que nuestro vecino sigue vivo. El Handler de CheckNeighborHealth
        // chequea si mi siguiente ideal es igual la siguiente estáción que tengo guardada. Si son distintos 
        // y mi siguiente ideal no está en la lista de nodos muertos, se intenta conectarse al siguiente ideal
        // si logro conectarme, actualizo mi siguiente al siguiente ideal 
        // Si la estación siguiente actual es igual a la ideal, me fijo si me puedo conectar o si está en la lista
        // de los nodos muertos, si cumple con alguno de los dos, mando HealRing para reconstruir el anillo. 
        ctx.run_interval(Duration::from_secs(2), |_actor, ctx| {
            ctx.address().do_send(CheckNeighborHealth);
        });

        // Autoenviamos de mensaje para unirme a la red
        ctx.address().do_send(Bootup);
    }
}

/// Asumimos que no estoy solo en la red e intento unirme a la red de forma natural, le pregunto a mi
/// vecino quién es el lider y actualizo. Si nadie me responde, significa que estoy solo y me autoproclamo líder
impl Handler<Bootup> for StationActor {
    type Result = ();
    fn handle(&mut self, _msg: Bootup, ctx: &mut Self::Context) -> Self::Result {
        // Evitamos integrar a la red si me encuentro desconectada del resto 
        // Desconexión simulada
        if self.is_disconnected { return; } 

        let mut sorted = self.known_stations.clone();
        sorted.sort_by_key(|s| s.station_id);
        let my_idx = sorted.iter().position(|s| s.station_id == self.station_id).unwrap();
        
        let self_addr = ctx.address();
        let my_ip = self.addr.clone();
        let dead_list = self.logically_dead.clone();

        // Lanzamos tarea para buscar vecino por si ya tenemos un anillo existente, 
        // arrancando desde mi siguiente directo, el primero que me contesta
        // va a ser mi siguiente, actualizo ese valor y le pregunto quien es el lider
        ctx.spawn(async move {
            let n = sorted.len();
            let mut found = false;

            // Circulamos nuestra lista de estaciones partiendo desde mi ID
            for offset in 1..n {
                let target = &sorted[(my_idx + offset) % n];
                // Si la dirección está en la lista de los nodos desconectados, lo ignoro 
                if dead_list.contains(&target.addr) { continue; }

                // Intentamos conectarnos a la siguiente calculada, si tuvimos éxito 
                // autoenviamos UpdateNextnode para actualizar el siguente nodo, 
                // y preguntamos a este nuevo siguiente por socket quién es el líder actual  
                if TcpStream::connect(&target.addr).await.is_ok() {
                    self_addr.do_send(UpdateNextNode(target.addr.clone()));
                    let _ = Self::send_tcp(target.addr.clone(), TcpMessage::WhoIsLeader { origin_addr: my_ip.clone() }).await;
                    found = true;
                    break;
                }
            }

            // Si después de iterar toda la lista de estaciones y no encontramos ninguno 
            // "vivo", nos autoclamamos líder
            if !found {
                self_addr.do_send(DeclareSelfLeader);
            } else {
                // Si encontramos a un siguiente, esperamos 3 segundos para que se sincronicen los 
                // mensajes de consulta y respuesta 
                tokio::time::sleep(Duration::from_secs(3)).await;
                // Verificamos si el líder está vivo o la estación a la que preguntamos no nos respondió nada
                self_addr.do_send(VerifyLeaderTimeout);
            }
        }.into_actor(self));
    }
}


/// Si pasó el tiempo y todavía no tenemos lider, es decir es None y además no estamos en elección 
/// lanzamos elección para elegir un nuevo líder
impl Handler<VerifyLeaderTimeout> for StationActor {
    type Result = ();
    fn handle(&mut self, _msg: VerifyLeaderTimeout, ctx: &mut Self::Context) -> Self::Result {
        // Chequeamos si me respondieron algo, si no me respondieron nada, signfica que el 
        // líder del otro también es None. Chequeamos, también si estamos en elección
        if self.leader.is_none() && !self.is_electing {
            // Autoenviamos para arrancar una elección 
            ctx.address().do_send(StartElection);
        }
    }
}

/// Si recibimos el mensaje WhoIsLeader y tengo algún líder, repondo la inforamción del líder
impl Handler<ProcessWhoIsLeader> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: ProcessWhoIsLeader, ctx: &mut Self::Context) -> Self::Result {

        // Si estamos desconecado del resto, no procesamos nada 
        if self.is_disconnected { return; } 

        // Si tenemos ya algún líder, armamos el mensaje RingElected con el ID del líder 
        // y le enviamos el mensaje a quien nos mandó WhoIsLeader
        if let Some(l) = &self.leader {
            let target = msg.0;
            let leader_id = l.station_id;
            let leader_addr = l.addr.clone();
            ctx.spawn(async move {
                let _ = Self::send_tcp(target, TcpMessage::RingElected { 
                    active_nodes: vec![],
                    leader_id, 
                    leader_addr, 
                    initiator_id: leader_id 
                }).await;
            }.into_actor(self));
        }
    }
}

/// Handleo de la operación de rentar una bici, actualizando todos los atributos del actor,
/// encolando los eventos y avisando al líder de la transacción
impl Handler<ExecRent> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: ExecRent, ctx: &mut Self::Context) -> Self::Result {
        self.available_bikes.retain(|&id| id != msg.bike_id);
        // Creamos el evento de BikeRented con la informacion correspondiente
        let event = StationEvent::BikeRented {
            event_id: self.next_event_id(),
            user_id: msg.user_id,
            station_id: self.station_id,
            bike_id: msg.bike_id,
            timestamp: chrono::Utc::now(),
        };
        // Encolamos el Evento en pending_events para sincronización con el líder
        self.pending_events.push_back(event.clone());
        self.log_event(&format!("BIKE_RENTED: {}", msg.bike_id));

        // Guardamos el evento serializado en el log para la Recuperación Total.
        // Si nos morimos, leeremos este JSON para saber que teníamos un viaje sin confirmar.
        self.log_event(&format!("PENDING_EVENT_ADD: {}", serde_json::to_string(&event).unwrap()));
        
        println!("[Estación] Alquiler local registrado: Bici {}", msg.bike_id);
        
        // Autoenviamos el mensaje SyncEventToLeader para enviar este nuevo evento al líder 
        ctx.address().do_send(SyncEventToLeader { origin_addr: self.addr.clone(), event });
    }
}

/// Handleo de la operación de retorno de bici, encola el evento pendiente, y avisa al líder que la 
/// operación termino.
impl Handler<ExecReturn> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: ExecReturn, ctx: &mut Self::Context) -> Self::Result {

        // Encolamos el ID de la bici que me devolvieron 
        self.available_bikes.push(msg.bike_id);
        // Creamos un nuevo evento de BikeReturned 
        let event = StationEvent::BikeReturned {
            event_id: self.next_event_id(),
            user_id: msg.user_id,
            station_id: self.station_id,
            bike_id: msg.bike_id,
            start_time: msg.start_time,
            return_time: chrono::Utc::now(),
            payment_method: msg.payment,
        };

        // Encolamos el Evento en eventos pendientes para sincronización con el líder
        self.pending_events.push_back(event.clone());
        self.log_event(&format!("BIKE_RETURNED_LOCAL_ACCEPT: {}", msg.bike_id));

        // Guardamos el evento serializado de devolución en el log.
        // Así, si caemos antes del ACK, podremos reconstruir este pendiente.
        self.log_event(&format!("PENDING_EVENT_ADD: {}", serde_json::to_string(&event).unwrap()));

        println!("[Estación] Devolución local aceptada y encolada: Bici {}", msg.bike_id);

        // Autoenviamos el mensaje para sincronización con el líder
        ctx.address().do_send(SyncEventToLeader { origin_addr: self.addr.clone(), event });
    }
}

/// Hanldeo de los ACK, respuestas del lider a los avisos de recibidos. busca el id del evento que 
/// matchee el que se recibio,verifica que el lider haya confirmado la devolucion y por lo tanto
/// puedo generar la informacion de cobro y luego lanzar una tarea para verificar que e pago 
/// se complete o no, si falla encolo a los pagos pendientes. Cuando se confirma, se elimina de la cola
impl Handler<ReceiveAck> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: ReceiveAck, ctx: &mut Self::Context) -> Self::Result {
        if self.is_disconnected { return; } // NUEVO
        let event_id = msg.0;
        
        let event_opt = self.pending_events.iter().find(|e| match e {
            StationEvent::BikeReturned { event_id: id, .. } => *id == event_id,
            _ => false,
        }).cloned();

        if let Some(StationEvent::BikeReturned { start_time, return_time, payment_method, bike_id, user_id, station_id, .. }) = event_opt {
            let duration = (return_time - start_time).num_minutes() as f64;
            let amount = 10.0 + duration * 10.0;
            
            let gateway = self.payment_gateway.clone();
            let self_addr = ctx.address();
            
            ctx.spawn(async move {
                let success = StationActor::send_payment(gateway, payment_method.clone(), amount, user_id, station_id).await;
                if !success {
                    self_addr.do_send(EnqueueFailedPayment { payment: payment_method, amount, user_id, station_id });
                }
            }.into_actor(self));
            
            self.log_event(&format!("BIKE_RETURNED_FULLY_SYNCED: {}", bike_id));
            println!("[Estación] Devolución sincronizada con líder y cobro autorizado: Bici {}", bike_id);
            
            // Solo registramos la remoción del log si fue una devolución confirmada.
            // Los alquileres siguen pendientes de remoción hasta que el viaje entero termine (ClosedTrip).
            self.log_event(&format!("PENDING_EVENT_REMOVE: {}", event_id));
        }

        self.pending_events.retain(|e| match e {
            StationEvent::BikeReturned { event_id: id, .. } => *id != event_id,
            _ => true,
        });
    }
}

/// Hadleo del aviso que el viaje cerró, por lo tanto ya puedo eliminar el viaje de mi cola
impl Handler<ReceiveClosedTrip> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: ReceiveClosedTrip, _ctx: &mut Self::Context) -> Self::Result {
        if self.is_disconnected { return; } 
        // Buscamos el ID del evento original de alquiler para marcarlo
        // como eliminado en el log, ya que el viaje cerró globalmente y ya no está pendiente.
        if let Some(StationEvent::BikeRented { event_id, .. }) = self.pending_events.iter().find(|e| match e {
            StationEvent::BikeRented { bike_id, .. } => *bike_id == msg.0,
            _ => false,
        }) {
            self.log_event(&format!("PENDING_EVENT_REMOVE: {}", event_id));
        }

        // Remuevo el BikeRented correspondiente a ese evento de devolución 
        self.pending_events.retain(|e| match e {
            StationEvent::BikeRented { bike_id, .. } => *bike_id != msg.0,
            _ => true,
        });
        println!("[Estacion] Notificación del líder recibida. Viaje de bici {} cerrado globalmente.", msg.0);
    }
}

impl Handler<EnqueueFailedPayment> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: EnqueueFailedPayment, _ctx: &mut Self::Context) -> Self::Result {
        self.pending_payments.push_back((msg.payment, msg.amount, msg.user_id, msg.station_id));
        self.log_event("PAYMENT_PENDING_ENQUEUED");
    }
}


/// Handleo de mensaje para mantener sincronizada la red, el lider mantiene actualizado el estado
/// global del sistema, si el lider arranca en blaco por ejemplo por una nueva votacion
/// tengo que reconstruir todo los eventos pendientes, lanzo ráfaga de mensajes para ello
impl Handler<SyncEventToLeader> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: SyncEventToLeader, ctx: &mut Self::Context) -> Self::Result {
        if self.is_disconnected || self.is_electing || self.leader.is_none() { return; }

        let is_leader = self.leader.as_ref().map_or(false, |l| l.station_id == self.station_id);
        let event = msg.event;
        let origin = msg.origin_addr;

        if is_leader {
            match event {
                StationEvent::BikeRented { event_id, bike_id, timestamp, station_id, .. } => {
                    let dummy_station = StationInfo { station_id, location: (0,0), addr: origin.clone() };
                    self.active_trips.insert(bike_id, TripInfo {
                        trip_id: event_id,
                        current_bike_id: bike_id,
                        start_time: timestamp,
                        last_owner_station: dummy_station,
                    });
                    
                    println!("[Lider] Sincronizando nuevo viaje en el registro global: Bici {}", bike_id);

                    if let Some((ret_origin, ret_event)) = self.orphan_returns.remove(&bike_id) {
                        if let StationEvent::BikeReturned { event_id: ret_event_id, .. } = ret_event {
                            let owner_addr = origin.clone();
                            self.active_trips.remove(&bike_id);
                            
                            ctx.spawn(async move {
                                let _ = Self::send_tcp(owner_addr, TcpMessage::ClosedTrip { bike_id }).await;
                                let _ = Self::send_tcp(ret_origin, TcpMessage::EventACK { event_id: ret_event_id }).await;
                            }.into_actor(self));
                        }
                    }
                    
                    ctx.spawn(async move {
                        let _ = Self::send_tcp(origin, TcpMessage::EventACK { event_id }).await;
                    }.into_actor(self));
                }
                StationEvent::BikeReturned { event_id, bike_id, .. } => {
                    if let Some(trip) = self.active_trips.remove(&bike_id) {
                        println!("[Líder] Cerrando viaje en el registro global: Bici {}", bike_id);
                        let owner_addr = trip.last_owner_station.addr.clone();
                        ctx.spawn(async move {
                            let _ = Self::send_tcp(owner_addr, TcpMessage::ClosedTrip { bike_id }).await;
                            let _ = Self::send_tcp(origin, TcpMessage::EventACK { event_id }).await;
                        }.into_actor(self));
                    } else {
                        self.orphan_returns.insert(bike_id, (origin, event.clone()));
                    }
                }
            }
        } else if let Some(leader_info) = &self.leader {
            let leader_addr = leader_info.addr.clone();
            ctx.spawn(async move {
                let _ = Self::send_tcp(leader_addr, TcpMessage::SyncEvent { origin_addr: origin, event }).await;
            }.into_actor(self));
        }
    }
}

/// Handle para comenzar a reconstruir los eventos pendientes
impl Handler<ReplayPendingEvents> for StationActor {
    type Result = ();
    fn handle(&mut self, _msg: ReplayPendingEvents, ctx: &mut Self::Context) -> Self::Result {
        let self_addr = ctx.address();
        // Diferimos la ejecución medio segundo para asegurar que el líder levantó el thread TCP.
        ctx.spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            // Autoenviamos el mensaje para enviar todos los eventos pendientes 
            self_addr.do_send(ExecuteReplay);
        }.into_actor(self));
    }
}

/// Sincronizamos eventos pendientes de pending_events de la estación con el líder 
impl Handler<ExecuteReplay> for StationActor {
    type Result = ();
    fn handle(&mut self, _msg: ExecuteReplay, ctx: &mut Self::Context) -> Self::Result {
        // Si no está vacío, avisamos de que vamos a sincronizar eventos con el líder
        if !self.pending_events.is_empty() {
            println!("[Estación] Sincronizando estado. Reenviando {} eventos al líder...", self.pending_events.len());
        }
        for event in &self.pending_events {
            ctx.address().do_send(SyncEventToLeader { origin_addr: self.addr.clone(), event: event.clone() });
        }
    }
}

// Responde a la petición del nuevo líder forzando a la estación a vaciar su cola de pendientes
impl Handler<RequestPendingEventsMsg> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: RequestPendingEventsMsg, ctx: &mut Self::Context) -> Self::Result {
        if self.is_disconnected { return; } // NUEVO
        // Si el mensaje dio la vuelta y llegó al líder que lo originó, se descarta.
        // Si yo fui la estación que originé el mensaje termino, no hago nada
        if msg.origin_addr == self.addr { return; }

        println!("[Red] Solicitud del lider para reconstruior. Enviando eventos pendientes al nuevo líder...");
        ctx.address().do_send(ReplayPendingEvents);
        // Autoenvío los eventos pendientes 

        // Pasamos el mensaje al siguiente nodo en el anillo restando 1 al TTL.
        if msg.ttl > 0 {
            if let Some(next) = &self.next_station_addr {
                let target = next.clone();
                // Si hay nodos pendientes seguimos propagando ese mensaje
                ctx.spawn(async move {
                    let _ = Self::send_tcp(target, TcpMessage::RequestPendingEvents { origin_addr: msg.origin_addr, ttl: msg.ttl - 1 }).await;
                }.into_actor(self));
            }
        }
    }
}

/// Si soy el lider muestro el registro global de viajes, si no lo soy paso el mensaje a mi siguiente
/// de esta forma siempre tenemos al lider como actor de la verdad
impl Handler<ConsoleQueryTrips> for StationActor {
    type Result = ();
    fn handle(&mut self, _msg: ConsoleQueryTrips, ctx: &mut Self::Context) -> Self::Result {
        if self.is_disconnected {
            println!("[Simulador] Estación desconectada. No se puede consultar la red.");
            return;
        }

        if self.is_electing || self.leader.is_none() {
            return;
        }

        let is_leader = self.leader.as_ref().map_or(false, |l| l.station_id == self.station_id);

        if is_leader {
            println!("--- REGISTRO GLOBAL DE VIAJES ACTIVOS (LÍDER) ---");
            if self.active_trips.is_empty() { println!("No hay viajes registrados en el sistema."); }
            for (bike, info) in &self.active_trips {
                println!("Bicicleta ID: {} | Inicio: {} | Estación Origen ID: {}", bike, info.start_time, info.last_owner_station.station_id);
            }
        } else if let Some(next) = &self.next_station_addr {
            println!("[Anillo] Reenviando solicitud de consulta de viajes...");
            let target = next.clone();
            let msg = TcpMessage::QueryTrips { origin_addr: self.addr.clone(), ttl: self.known_stations.len() };
            ctx.spawn(async move {
                let _ = Self::send_tcp(target, msg).await;
            }.into_actor(self));
        }
    }
}

impl Handler<ProcessQueryTrips> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: ProcessQueryTrips, ctx: &mut Self::Context) -> Self::Result {
        if self.is_electing || self.is_disconnected { return; } // NUEVO
        
        let is_leader = self.leader.as_ref().map_or(false, |l| l.station_id == self.station_id);
        if is_leader {
            let trips_vec: Vec<TripInfo> = self.active_trips.values().cloned().collect();
            let target = msg.origin_addr;
            ctx.spawn(async move {
                let _ = Self::send_tcp(target, TcpMessage::QueryTripsResponse { trips: trips_vec, error: false }).await;
            }.into_actor(self));
        } else if msg.ttl > 0 {
            if let Some(next) = &self.next_station_addr {
                let target = next.clone();
                ctx.spawn(async move {
                    let _ = Self::send_tcp(target, TcpMessage::QueryTrips { origin_addr: msg.origin_addr, ttl: msg.ttl - 1 }).await;
                }.into_actor(self));
            }
        } else {
            let target = msg.origin_addr;
            ctx.spawn(async move {
                let _ = Self::send_tcp(target, TcpMessage::QueryTripsResponse { trips: vec![], error: true }).await;
            }.into_actor(self));
        }
    }
}

/// Handleo para setear el lider, luego lanzo tarea para rejecutar los eventos pendientes,
/// ya que como se llavo a setear lider, este cambio y por lo tanto tenemos que sincronizar
impl Handler<SetLeaderMsg> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: SetLeaderMsg, ctx: &mut Self::Context) -> Self::Result {
        if self.is_disconnected { return; } // NUEVO
        self.set_leader(msg.leader_id, msg.leader_addr);
        ctx.address().do_send(ReplayPendingEvents);
    }
}
/// Actualizo mi vecino
impl Handler<UpdateNextNode> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: UpdateNextNode, _ctx: &mut Self::Context) -> Self::Result {
        if self.is_disconnected { return; } // NUEVO
        self.next_station_addr = Some(msg.0.clone());
        println!("[Red] Enlace establecido hacia: {}", msg.0);
    }
}
/// Me autodeclaro lider, porque o bien gane la eleccion o bien estoy solo
impl Handler<DeclareSelfLeader> for StationActor {
    type Result = ();
    fn handle(&mut self, _msg: DeclareSelfLeader, ctx: &mut Self::Context) -> Self::Result {
        // Si estoy desconectado, no hago nada, no tiene sentido
        // No se puede consultar la red
        if self.is_disconnected { return; } 
        self.is_electing = false; // No está habiendo elección 
        // Verificamos si ya éramos el líder. 
        let was_leader = self.leader.as_ref().map_or(false, |l| l.station_id == self.station_id);
        
        // Actualizamos el líder. Creamos el nuevo StationInfo para el líder
        self.leader = Some(StationInfo {
            station_id: self.station_id,
            location: self.location,
            addr: self.addr.clone(),
        });

        // Si no era el líder, limpiamos active_trips y orphan_returns
        // Si era el último líder, puedo tener mantener lo último 
        if !was_leader {
            // Si no fui el último líder, tengo que limpiar todo 
            self.active_trips.clear();
            self.orphan_returns.clear();
        }

        // Autoenvío ReplayPendingEvents
        ctx.address().do_send(ReplayPendingEvents);
    }
}

/// Pruebo si mi siguiente normal (es decir mi id +1), esta vivo, si lo esta y me
/// contesta, pero no era mi siguiente quiere decir que se callo
/// por lo tanto lo vuelvo a incluir, si no esta vivo, mando mensaje a su siguiente y asi, hasta
/// actualizar mi nuevo vecino, si mi vecino era el lider, llamo a eleccion
impl Handler<CheckNeighborHealth> for StationActor {
    type Result = ();
    fn handle(&mut self, _msg: CheckNeighborHealth, ctx: &mut Self::Context) -> Self::Result {
        if self.is_disconnected { return; }

        let current_next = self.next_station_addr.clone();
        let self_addr = ctx.address();
        
        let mut sorted = self.known_stations.clone();
        sorted.sort_by_key(|s| s.station_id);
        let my_idx = sorted.iter().position(|s| s.station_id == self.station_id).unwrap();
        let ideal_next = &sorted[(my_idx + 1) % sorted.len()];
        let ideal_addr = ideal_next.addr.clone();
        let dead_list = self.logically_dead.clone(); // Evaluamos los nodos muertos simulados

        ctx.spawn(async move {
            // Intentamos recuperar el anillo y se fija si está el siguiente ideal, si no está
            // continúa con el siguiente original 
            // Si mi ideal no es igual al siguiente de ahora no entra
            if Some(ideal_addr.clone()) != current_next {
                if !dead_list.contains(&ideal_addr) { 
                    if TcpStream::connect(&ideal_addr).await.is_ok() {
                        self_addr.do_send(UpdateNextNode(ideal_addr.clone()));
                        return;
                    }
                }
            }

            // Intentamos conectarnos a nuestro current_next
            if let Some(addr) = current_next {
                // si current_next está en la lista de los nodos muertos o no logramos 
                // conectarnos disparamos HealRing para reconstruir el anillo
                if dead_list.contains(&addr) || TcpStream::connect(&addr).await.is_err() {
                    self_addr.do_send(HealRing { dead_addr: addr });
                }
            }
        }.into_actor(self));
    }
}

/// tratamos de curar la red de anillo, si el que murio, era el lider mando a eleccion, sino solo actualizo
impl Handler<HealRing> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: HealRing, ctx: &mut Self::Context) -> Self::Result {
        if self.is_disconnected { return; } 
        let dead_addr = msg.dead_addr;
        // me fijo si el nodo muerto es líder o no
        let was_leader = self.leader.as_ref().map_or(false, |l| l.addr == dead_addr);

        self.is_electing = false; // Declaramos que no estamos en elección 
        
        let mut sorted = self.known_stations.clone();
        sorted.sort_by_key(|s| s.station_id);
        let my_id = self.station_id;
        let self_addr = ctx.address();
        let dead_list = self.logically_dead.clone();

        ctx.spawn(async move {
            // Buscamos nuestro priopio ID en la lista de estaciones 
            if let Some(my_idx) = sorted.iter().position(|s| s.station_id == my_id) {
                let n = sorted.len();
                let mut found_any = false;
                
                for offset in 1..n {
                    // Probamos circularmente para buscar una siguiente estación para conectarse
                    let target = &sorted[(my_idx + offset) % n];
                    // Si la siguiente es la que está muerta o está en a lista de los no "vivos"
                    // seguimos iterando
                    if target.addr == dead_addr || dead_list.contains(&target.addr) { continue; } 
                    
                    // Si encuentro uno que no es la estación que está muerta tampoco está en la lista
                    // de los nodos muertos, intetamos la conexión 
                    if TcpStream::connect(&target.addr).await.is_ok() {
                        // Si logramos conectar, autoenviamos actualización del siguiente nodo
                        // poniendo como siguiente a este nodo vivo 
                        self_addr.do_send(UpdateNextNode(target.addr.clone()));
                        found_any = true;
                        break;
                    }
                }
                
                if !found_any {
                    // Si no encontramos nada, signfica que estamos solo
                    // autoproclamamos líder
                    self_addr.do_send(DeclareSelfLeader);
                } else if was_leader {
                    // Si el nodo muerto era el líder, debemos arrancar elección 
                    println!("Lider caído detectado arranco eleccion.");
                    self_addr.do_send(StartElection);
                }
            }
        }.into_actor(self));
    }
}
/// Elección en red de anillo
impl Handler<StartElection> for StationActor {
    type Result = ();
    fn handle(&mut self, _msg: StartElection, ctx: &mut Self::Context) -> Self::Result {
        if self.is_disconnected { return; } // Si estoy desconectado, no elijo líder
        if self.is_electing { return; } // Si ya estoy eligiendo, no disparo otra
        self.is_electing = true; // Declaro estar en elección 
        
        // Si tengo siguiente 
        if let Some(next_addr) = &self.next_station_addr {
            let target = next_addr.clone();
            // Mandamos mensajes de RingElection a la siguiente 
            let msg = TcpMessage::RingElection { active_nodes: vec![self.station_id] };
            // Enviamos el mensaje de elección a la siguiente 
            ctx.spawn(async move {
                let _ = Self::send_tcp(target, msg).await;
            }.into_actor(self));
        }
    }
}

/// Handleo del mensaje de RingElection 
impl Handler<ProcessRingElection> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: ProcessRingElection, ctx: &mut Self::Context) -> Self::Result {
        if self.is_disconnected { return; } 
        // Nos agarramos el primer parámetro que nos entregan que tiene todos los IDs
        // cola de todos los nodos que han ecolado su IDs
        let mut active_nodes = msg.0;
        if !self.is_electing { self.is_electing = true; }

        // Comparamos nuestro ID con el primer ID de la cola, si son iguales 
        // significa que el mensaje de elección ya dio toda la vuelta y se deja de propagar
        if active_nodes[0] == self.station_id {
            let leader_id = *active_nodes.iter().max().unwrap();
            let leader_info = self.known_stations.iter().find(|s| s.station_id == leader_id).unwrap();
            let leader_addr = leader_info.addr.clone();

            self.set_leader(leader_id, leader_addr.clone());
            ctx.address().do_send(ReplayPendingEvents);

            // El nuevo líder pide explícitamente a la red que envíe todos sus eventos pendientes 
            // para poder reconstruir el registro global de viajes perdidos.
            if let Some(next) = &self.next_station_addr {
                let target = next.clone();
                let req_msg = TcpMessage::RequestPendingEvents { origin_addr: self.addr.clone(), ttl: self.known_stations.len() };
                ctx.spawn(async move {
                    let _ = Self::send_tcp(target, req_msg).await;
                }.into_actor(self));
            }

            // Le avisa al siguiente el mensaje de RingElected
            if let Some(next) = &self.next_station_addr {
                let target = next.clone();
                let initiator = self.station_id;
                ctx.spawn(async move {
                    // Quien inició la elección manda RingElected
                    let _ = Self::send_tcp(target, TcpMessage::RingElected { 
                        active_nodes, leader_id, leader_addr, initiator_id: initiator 
                    }).await;
                }.into_actor(self));
            }
        } else if active_nodes.contains(&self.station_id) {
        } else {
            active_nodes.push(self.station_id);
            if let Some(next) = &self.next_station_addr {
                let target = next.clone();
                // Si no estoy en la lista significa que es la primera vez que me llegue el mensaje 
                // entonces encolo mi ID y se lo mando a mi siguiente 
                ctx.spawn(async move {
                    let _ = Self::send_tcp(target, TcpMessage::RingElection { active_nodes }).await;
                }.into_actor(self));
            }
        }
    }
}

/// Handleo del mensaje RingElected
impl Handler<ProcessRingElected> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: ProcessRingElected, ctx: &mut Self::Context) -> Self::Result {
        if self.is_disconnected { return; } 

        // Si fui quien inició la elección, termino 
        if msg.initiator_id == self.station_id {
            return;
        }

        // Seteo el nuevo líder 
        self.set_leader(msg.leader_id, msg.leader_addr.clone());
        // Si me caí, me volví a conectar, ya tengo el líder
        // vuelvo a enviar los eventos pendientes
        ctx.address().do_send(ReplayPendingEvents);

        // Si tenemos siguite, propagamos RingElected al siguiente. 
        if let Some(next) = &self.next_station_addr {
            let target = next.clone();
            ctx.spawn(async move {
                let _ = Self::send_tcp(target, TcpMessage::RingElected { 
                    active_nodes: msg.active_nodes,
                    leader_id: msg.leader_id, 
                    leader_addr: msg.leader_addr, 
                    initiator_id: msg.initiator_id 
                }).await;
            }.into_actor(self));
        }
    }
}

impl Handler<RetryPendingPayments> for StationActor {
    type Result = ();
    fn handle(&mut self, _msg: RetryPendingPayments, ctx: &mut Self::Context) -> Self::Result {
        if let Some((method, amount, user_id, station_id)) = self.pending_payments.pop_front() {
            let gateway = self.payment_gateway.clone();
            let self_addr = ctx.address();
            ctx.spawn(async move {
                let success = StationActor::send_payment(gateway, method.clone(), amount, user_id, station_id).await;
                if success {
                    println!("[Estación] Pago pendiente procesado exitosamente para usuario {} en estación {} por ${}.", user_id, station_id, amount);
                } else {
                    self_addr.do_send(EnqueueFailedPayment { payment: method, amount, user_id, station_id });
                }
            }.into_actor(self));
        }
    }
}

// Permite aislar a la estación avisando al resto del anillo, mientras sigue atendiendo clientes.
impl Handler<ConsoleDisconnect> for StationActor {
    type Result = ();
    fn handle(&mut self, _msg: ConsoleDisconnect, ctx: &mut Self::Context) -> Self::Result {
        if self.is_disconnected { return; }
        self.is_disconnected = true;
        self.leader = None;
        self.next_station_addr = None;
        self.is_electing = false;
        println!("[Simulador] Estación desconectada del anillo lógicamente. Aceptando solo clientes locales.");
        
        // Avisar a todas las demás estaciones que simulen una falla TCP hacia nosotros
        let dead_addr = self.addr.clone();
        for st in &self.known_stations {
            if st.addr != dead_addr {
                let target = st.addr.clone();
                let dead = dead_addr.clone();
                ctx.spawn(async move {
                    let _ = Self::send_tcp(target, TcpMessage::NodeDisconnected { dead_addr: dead }).await;
                }.into_actor(self));
            }
        }
    }
}

impl Handler<ConsoleConnect> for StationActor {
    type Result = ();
    fn handle(&mut self, _msg: ConsoleConnect, ctx: &mut Self::Context) -> Self::Result {
        if !self.is_disconnected { return; }
        self.is_disconnected = false;
        println!("[Simulador] Estación reconectándose al anillo...");
        
        // Avisa a todos que volvió para que no nos ignoren
        let alive_addr = self.addr.clone();
        for st in &self.known_stations {
            if st.addr != alive_addr {
                let target = st.addr.clone();
                let msg = TcpMessage::NodeReconnected { alive_addr: alive_addr.clone() };
                ctx.spawn(async move {
                    let _ = Self::send_tcp(target, msg).await;
                }.into_actor(self));
            }
        }
        
        // Inicia el proceso nativo para sumarse a la red como cuando arranca el programa
        ctx.address().do_send(Bootup);
    }
}

// Estos comandos son los que reciben los vecinos cuando la estación avisa que se desconectó
impl Handler<RegisterDeadNode> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: RegisterDeadNode, ctx: &mut Self::Context) -> Self::Result {
        if !self.logically_dead.contains(&msg.0) {
            self.logically_dead.push(msg.0.clone());
        }
        // Si el muerto era mi vecino en la red, o mi líder, llamo a re-armar el anillo
        if self.next_station_addr == Some(msg.0.clone()) || 
           self.leader.as_ref().map(|l| l.addr == msg.0).unwrap_or(false) {
            ctx.address().do_send(HealRing { dead_addr: msg.0 });
        }
    }
}

impl Handler<RegisterAliveNode> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: RegisterAliveNode, _ctx: &mut Self::Context) -> Self::Result {
        self.logically_dead.retain(|a| a != &msg.0);
        println!("[Red] Nodo {} vuelve a estar disponible lógicamente.", msg.0);
    }
}

///handleo de los mensajes recibidos por TCP, escucho por el socket y proceso la respuesta en
/// base al mensaje recibido, devio en el caso correspondiente, renta, devolucion, sincronizar, ack,etc
impl Handler<HandleIncomingTcp> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: HandleIncomingTcp, ctx: &mut Self::Context) -> Self::Result {
        let mut stream = msg.0;
        let self_addr = ctx.address();
        let station_id = self.station_id;
        let bikes = self.available_bikes.clone();
        let max_slots = self.max_slots;
        let gateway = self.payment_gateway.clone();
        let disconnected = self.is_disconnected.clone(); 
        let next_node = self.next_station_addr.clone();
        let station_addr = self.addr.clone(); 

        ctx.spawn(async move {
            let mut reader = BufReader::new(&mut stream);
            let mut line = String::new();
            
            if reader.read_line(&mut line).await.is_ok() {
                if let Ok(tcp_msg) = serde_json::from_str::<TcpMessage>(&line) {
                    match tcp_msg {
                        TcpMessage::GetBikes => {
                            let free_slots = max_slots.saturating_sub(bikes.len());
                            let resp = TcpMessage::Availability { station_id, bikes_available: bikes, free_slots };
                            let serialized = serde_json::to_string(&resp).unwrap() + "\n";
                            let _ = stream.write_all(serialized.as_bytes()).await;
                        }

                        TcpMessage::GetAllStationsAvailability { origin_station_id, mut stations_availability} => {
                            let free_slots = max_slots.saturating_sub(bikes.len());
                            if disconnected {
                                // Si está desconectado del resto, solamente devuelve su dispoinibilidad
                                stations_availability.push((station_id, station_addr.clone(), bikes.clone(), free_slots));
                                let resp = TcpMessage::GetAllStationsAvailability { origin_station_id, stations_availability };
                                let serialized = serde_json::to_string(&resp).unwrap() + "\n";
                                let _ = stream.write_all(serialized.as_bytes()).await;
                            } else if origin_station_id == station_id {
                                // Si origin_station_id == station_id significa que se dio la vuelta y tendría que volver 
                                println!("[GetBikes] Devuelvo Availability: {} | {:?}", origin_station_id, stations_availability); 
                                let resp = TcpMessage::GetAllStationsAvailability { origin_station_id, stations_availability };
                                let serialized = serde_json::to_string(&resp).unwrap() + "\n";
                                // Este stream viene de la estación anterior que forwardeo la consula, por lo que se lo mando devuelta para 
                                // que vuelva el mensaje hacia la primera estación que forwardeo el request 
                                let _ = stream.write_all(serialized.as_bytes()).await;
                            } else {
                                println!("[GetBikes] pasando query"); 
                                        // Es una consulta recursiva para no perder el stream del cliente que hizo la consulta                     
                                        // Si origin_station_id es 0 significa que es el primer request del cliente
                                        // debo cambiar el origin_station_id por mi station_id, sino dejo el station_id que me pasaron
                                        let origin = if origin_station_id == 0 { station_id } else { origin_station_id }; 
                                        stations_availability.push((station_id, station_addr.clone(), bikes.clone(), free_slots)); 
                                        
                                        // Creo el mensaje de Forward para la siguiente estación 
                                        let forward_msg = TcpMessage::GetAllStationsAvailability { 
                                            origin_station_id: origin, 
                                            stations_availability 
                                        };
                                        let serialized_fwd = serde_json::to_string(&forward_msg).unwrap() + "\n";

                                        if let Some(next_addr) = &next_node { 
                                            // Me conecto a la siguiente estación 
                                            if let Ok(mut next_stream) = TcpStream::connect(next_addr).await {
                                                
                                                // Enviamos el foward_msg a la siguiente estación
                                                
                                                let _ = next_stream.write_all(serialized_fwd.as_bytes()).await;
                                                let mut reader = BufReader::new(next_stream);
                                                let mut line = String::new();
                                                
                                                // Nos quedamos esperando la respuesta de la siguiente estacióń
                                                // la consulta es recursiva, la consulta completa volverá a la estación 
                                                // de donde el cliente hizo la consulta. 
                                                if reader.read_line(&mut line).await.is_ok() && !line.is_empty() {
                                                    // Si la siguiente estación nos devuelve la respuesta se lo mandamos 
                                                    // a la estación anterior que forwardeo la consulta y asi sucesivamente hasta llegar
                                                    // a la primera estación que recibió la consulta del cliente
                                                    let _ = stream.write_all(line.as_bytes()).await;
                                                }
                                            } else {
                                                // Si no logro conectarme a la siguiente devuelvo lo que tengo 
                                                let _ = stream.write_all(serialized_fwd.as_bytes()).await;
                                            }
                                        }
                            }
                        }
                        
                        TcpMessage::RentBike { user_id, bike_id, payment_method } => {
    
                            if bikes.contains(&bike_id) {
                                let auth = StationActor::send_payment(gateway.clone(), payment_method, 500.0, user_id, station_id).await;
                                if auth {
                                self_addr.do_send(ExecRent { user_id, bike_id });
                                let resp = TcpMessage::RentResponse { success: true, message: "Alquiler Consolidado".into() };
                                let _ = stream.write_all((serde_json::to_string(&resp).unwrap() + "\n").as_bytes()).await;
                                }else{
                                    let resp = TcpMessage::RentResponse { success: false, message: "Banco fuera de servicio".into() };
                                    let _ = stream.write_all((serde_json::to_string(&resp).unwrap() + "\n").as_bytes()).await;
                                }
                            } else {
                                let resp = TcpMessage::RentResponse { success: false, message: "Denegado o falta stock".into() };
                                let _ = stream.write_all((serde_json::to_string(&resp).unwrap() + "\n").as_bytes()).await;
                            }
                        }
                        TcpMessage::ReturnBike { user_id, bike_id, payment_method, trip_info } => {
                            if bikes.len() >= max_slots {
                                let resp = TcpMessage::ReturnResponse { success: false, message: "Capacidad excedida".into() };
                                let _ = stream.write_all((serde_json::to_string(&resp).unwrap() + "\n").as_bytes()).await;
                            } else {
                                self_addr.do_send(ExecReturn { 
                                    user_id, bike_id, 
                                    start_time: trip_info.start_time, 
                                    payment: payment_method 
                                });
                                let resp = TcpMessage::ReturnResponse { success: true, message: "Devolución Exitosa".into() };
                                let _ = stream.write_all((serde_json::to_string(&resp).unwrap() + "\n").as_bytes()).await;
                            }
                        }
                        TcpMessage::RequestPendingEvents { origin_addr, ttl } => {
                            self_addr.do_send(RequestPendingEventsMsg { origin_addr, ttl });
                        }
                        TcpMessage::SyncEvent { origin_addr, event } => {
                            self_addr.do_send(SyncEventToLeader { origin_addr, event });
                        }
                        TcpMessage::EventACK { event_id } => {
                            self_addr.do_send(ReceiveAck(event_id));
                        }
                        TcpMessage::ClosedTrip { bike_id } => {
                            self_addr.do_send(ReceiveClosedTrip(bike_id));
                        }
                        TcpMessage::QueryTrips { origin_addr, ttl } => {
                            self_addr.do_send(ProcessQueryTrips { origin_addr, ttl });
                        }
                        TcpMessage::QueryTripsResponse { trips, error } => {
                            if error {
                                println!("[Red] Consulta descartada por latencia o anillo en reestructuración.");
                            } else {
                                println!("--- REGISTRO GLOBAL DE VIAJES ACTIVOS (RECIBIDO DEL LÍDER) ---");
                                if trips.is_empty() { println!("No hay viajes registrados en el sistema."); }
                                for info in trips {
                                    println!("Bicicleta ID: {} | Inicio: {} | Estación Origen: {}", info.current_bike_id, info.start_time, info.last_owner_station.addr);
                                }
                            }
                        }
                        TcpMessage::WhoIsLeader { origin_addr } => {
                            self_addr.do_send(ProcessWhoIsLeader(origin_addr));
                        }
                        TcpMessage::RingElection { active_nodes } => {
                            self_addr.do_send(ProcessRingElection(active_nodes));
                        }
                        TcpMessage::RingElected { active_nodes, leader_id, leader_addr, initiator_id } => {
                            self_addr.do_send(ProcessRingElected { active_nodes, leader_id, leader_addr, initiator_id });
                        }
                        TcpMessage::NodeDisconnected { dead_addr } => {
                            self_addr.do_send(RegisterDeadNode(dead_addr));
                        }
                        TcpMessage::NodeReconnected { alive_addr } => {
                            self_addr.do_send(RegisterAliveNode(alive_addr));
                        }
                        TcpMessage::Availability { .. } | TcpMessage::RentResponse { .. } | TcpMessage::ReturnResponse { .. } => {
                            println!("[Red] EL paquete no tiene la serializacion correcta, descartado.");
                        }
                    }
                }
            }
        }.into_actor(self));
    }
}


/// Funciones auxiliares

/// Función para procesar y leer los archivos de configuracion para armar la estacion
pub fn load_stations_from_config(path: &str) -> Vec<StationInfo> {
    let contents = fs::read_to_string(path).unwrap_or_else(|_| {
        eprintln!("Error: Imposible acceder al archivo de configuracion: '{}'", path);
        std::process::exit(1);
    });

    let mut stations = Vec::new();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') { continue; }

        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() == 4 {
            let id: u32 = parts[0].parse().unwrap_or(0);
            let x: usize = parts[1].parse().unwrap_or(0);
            let y: usize = parts[2].parse().unwrap_or(0);
            let addr = parts[3].to_string();
            stations.push(StationInfo { station_id: id, location: (x, y), addr });
        }
    }

    if stations.is_empty() {
        eprintln!("Error:red vacía en archivo de configuración.");
        std::process::exit(1);
    }
    stations
}

// Además de contar bicicletas, ahora buscamos los eventos PENDING_EVENT_ADD y PENDING_EVENT_REMOVE.
// Esto permite reconstruir la cola de eventos que la estación no llegó a sincronizar
// con el líder antes de morir. De esta manera, al reconectarse a un nuevo líder, 
// podrá reenviar los eventos pendientes (como viajes activos) y recuperar el estado global.
pub fn recover_state_from_log(log_path: &PathBuf, mut available_bikes: Vec<u32>) -> (Vec<u32>, u32, VecDeque<StationEvent>) {
    let mut event_counter = 1;
    // Usamos un mapa para ir guardando y borrando los eventos según el log
    let mut pending_events_map: HashMap<u32, StationEvent> = HashMap::new();
    
    if let Ok(contents) = fs::read_to_string(log_path) {
        println!("[Estacion] Archivo de log encontrado. Reconstruyendo estado físico y eventos...");
        
        for line in contents.lines() {
            if let Some(idx) = line.find("BIKE_RENTED: ") {
                let bike_id_str = &line[idx + "BIKE_RENTED: ".len()..];
                if let Ok(bike_id) = bike_id_str.trim().parse::<u32>() {
                    available_bikes.retain(|&id| id != bike_id);
                    event_counter += 1;
                }
            } else if let Some(idx) = line.find("BIKE_RETURNED_LOCAL_ACCEPT: ") {
                let bike_id_str = &line[idx + "BIKE_RETURNED_LOCAL_ACCEPT: ".len()..];
                if let Ok(bike_id) = bike_id_str.trim().parse::<u32>() {
                    if !available_bikes.contains(&bike_id) {
                        available_bikes.push(bike_id);
                    }
                    event_counter += 1;
                }
            } 
            // Recuperación de eventos pendientes.
            // Si leemos un ADD, convertimos el JSON de texto a la estructura StationEvent y la guardamos
            else if let Some(idx) = line.find("PENDING_EVENT_ADD: ") {
                let json_str = &line[idx + "PENDING_EVENT_ADD: ".len()..];
                if let Ok(event) = serde_json::from_str::<StationEvent>(json_str) {
                    let ev_id = match &event {
                        StationEvent::BikeRented { event_id, .. } => *event_id,
                        StationEvent::BikeReturned { event_id, .. } => *event_id,
                    };
                    pending_events_map.insert(ev_id, event);
                }
            }
            //Si leemos un REMOVE, significa que el líder confirmó el evento antes de que
            // esta estación muriera, por ende ya no está "pendiente". Lo borramos.
            else if let Some(idx) = line.find("PENDING_EVENT_REMOVE: ") {
                let id_str = &line[idx + "PENDING_EVENT_REMOVE: ".len()..];
                if let Ok(ev_id) = id_str.trim().parse::<u32>() {
                    pending_events_map.remove(&ev_id);
                }
            }
        }
        println!("[Estacion] Estado reconstruido. Bicicletas actuales: {:?}", available_bikes);
        //Informar cuántos eventos pendientes se recuperaron
        if !pending_events_map.is_empty() {
            println!("[Estacion] Se recuperaron {} eventos pendientes de sincronizar.", pending_events_map.len());
        }
    } else {
        println!("[Estacion] No se encontró log previo. Iniciando con estado por defecto.");
    }
    
    // Convertimos el HashMap final a una cola VecDeque para devolverlo junto a las bicis
    let pending_events: VecDeque<StationEvent> = pending_events_map.into_values().collect();
    (available_bikes, event_counter, pending_events)
}