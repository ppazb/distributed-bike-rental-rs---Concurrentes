use actix::prelude::*;
use bici_concu::models::payment_method::PaymentMethod;
use bici_concu::models::station_event::StationEvent;
use bici_concu::models::station_info::StationInfo;
use bici_concu::models::tcp_message::TcpMessage;
use bici_concu::models::trip_info::TripInfo;
use std::collections::{HashMap, VecDeque};
use std::env;
use std::fs::{self, OpenOptions};
use std::io::{Write};
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

const BANK_ADDR: &str = "127.0.0.1:4000";

//Declaracion de mesajes de actor estacion

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
struct HandleIncomingTcp(TcpStream);

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
struct ConsoleQueryTrips;

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

//Mensajes paymentGateway (Pendiente de implementar)
struct PaymentGateway {
    bank_addr: String,
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

// Implementacion actor estacion

struct StationActor {
    station_id: u32, //Id para identificar cada estacion de manera unica, para la estructura de anillo
    addr: String, //Direccion de la estacion
    location: (usize, usize),//representa la ubicacion
    max_slots: usize,//cantidad de slot maxima de bicicleta
    available_bikes: Vec<u32>, // bicicletas en la estacion
    leader: Option<StationInfo>,//Option de la estacion lider, porque puede no tener referencia
    known_stations: Vec<StationInfo>,//todas las estaciones conocidas
    next_station_addr: Option<String>, //mi vecino en el anillo
    payment_gateway: Addr<PaymentGateway>,//direccion del metodo de pago
    pending_events: VecDeque<StationEvent>,//cola de eventos pendientes para sincronizacion
    pending_payments: VecDeque<(PaymentMethod, f64, u32, u32)>,//cola de pagos pendientes para futuro cobro (payment, amount, user_id, station_id)
    active_trips: HashMap<u32, TripInfo>, // viajes abiertos,para mantener vision global de la red, solo el lider la usa
    orphan_returns: HashMap<u32, (String, StationEvent)>,//hashmap para devoluciones que llegaron al líder antes que el aviso de alquiler (por lag)
    log_path: PathBuf,//path del archivo para escribir el log de la estacion
    is_electing: bool,//si se esta en una eleccion de lider actualmente
    event_counter: u32,//contador de eventos para generar eventos unicos
}

impl StationActor {
    ///Funcion para flushear eventos al archivo log de la estacion
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
    //devuelve id actual y suma uno al contador
    fn next_event_id(&mut self) -> u32 {
        let id = self.event_counter;
        self.event_counter += 1;
        id
    }
    ///Actualizo el lider, verificando si antes yo era lider para limpiar mi registro de viajes
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
        }

        println!("[Anillo] Lider actual de la red establecido: {}", leader_id);
    }

}

/// Implementacion de los hanlders de mensaje

impl Actor for StationActor {
    type Context = Context<Self>;
    ///Funcion que se llama al iniciar un actor en actix, permite configurar runtimes automaticos
    /// con timers
    fn started(&mut self, ctx: &mut Self::Context) {
        println!("Estación {} activa en dirección {}", self.station_id, self.addr);
        self.log_event("STATION_STARTED");
        //lanzamos una tarea para reintentar pagos pendientes cada 5 segundos
        ctx.run_interval(Duration::from_secs(5), |_actor, ctx| {
            ctx.address().do_send(RetryPendingPayments);
        });
        //lanzamos una tarea para verificar que nuestro vecino sigue vivo
        ctx.run_interval(Duration::from_secs(2), |_actor, ctx| {
            ctx.address().do_send(CheckNeighborHealth);
        });
        //autoenvia de mensaje para unirme a la red
        ctx.address().do_send(Bootup);
    }
}
///Asumo que no estoy solo en la red e intento unirme a la red de forma natural, le pregunto a mi
/// vecino quien es el lider y actualizo. Si nadie me responde, entonces si estoy solo, soy el lider
impl Handler<Bootup> for StationActor {
    type Result = ();
    fn handle(&mut self, _msg: Bootup, ctx: &mut Self::Context) -> Self::Result {
        let mut sorted = self.known_stations.clone();
        sorted.sort_by_key(|s| s.station_id);
        let my_idx = sorted.iter().position(|s| s.station_id == self.station_id).unwrap();
        
        let self_addr = ctx.address();
        let my_ip = self.addr.clone();
        //lanzo tarea para buscar vecino, arrancando desde mi siguiente directo, el primero que me
        //contesta, es mi vecino, actualizo ese valor y le pregunto quien es el lider
        ctx.spawn(async move {
            let n = sorted.len();
            let mut found = false;
            for offset in 1..n {
                let target = &sorted[(my_idx + offset) % n];
                if TcpStream::connect(&target.addr).await.is_ok() {
                    self_addr.do_send(UpdateNextNode(target.addr.clone()));
                    let _ = Self::send_tcp(target.addr.clone(), TcpMessage::WhoIsLeader { origin_addr: my_ip.clone() }).await;
                    found = true;
                    break;
                }
            }
            //no encontre a nadie, soy el lider
            if !found {
                self_addr.do_send(DeclareSelfLeader);
            } else {
                tokio::time::sleep(Duration::from_secs(3)).await;
                self_addr.do_send(VerifyLeaderTimeout);
            }
        }.into_actor(self));
    }
}
///Si paso el tiempo y todavia no tengo lider, es decir es none y ademas no se esta en votacion
/// lanzo yo la votacion, hay que elegir lider
impl Handler<VerifyLeaderTimeout> for StationActor {
    type Result = ();
    fn handle(&mut self, _msg: VerifyLeaderTimeout, ctx: &mut Self::Context) -> Self::Result {
        if self.leader.is_none() && !self.is_electing {
            ctx.address().do_send(StartElection);
        }
    }
}
///Si recibo el mensaje y se quien es el lider, respondo.
impl Handler<ProcessWhoIsLeader> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: ProcessWhoIsLeader, ctx: &mut Self::Context) -> Self::Result {
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

///handleo de la operacion de rentar una bici, actualizando, todos los atributos del actor,
/// encolando los eventos y avisando al lider de la transaccion
impl Handler<ExecRent> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: ExecRent, ctx: &mut Self::Context) -> Self::Result {
        self.available_bikes.retain(|&id| id != msg.bike_id);
        //creo el evento con la informacion correspondiente
        let event = StationEvent::BikeRented {
            event_id: self.next_event_id(),
            user_id: msg.user_id,
            station_id: self.station_id,
            bike_id: msg.bike_id,
            timestamp: chrono::Utc::now(),
        };
        self.pending_events.push_back(event.clone());
        self.log_event(&format!("BIKE_RENTED: {}", msg.bike_id));

        //Guardamos el evento serializado en el log para la Recuperación
        // Si nos morimos, leemos este JSON para saber que teníamos un viaje sin confirmar.
        self.log_event(&format!("PENDING_EVENT_ADD: {}", serde_json::to_string(&event).unwrap()));
        
        println!("[Estación] Alquiler local registrado: Bici {}", msg.bike_id);
        
        ctx.address().do_send(SyncEventToLeader { origin_addr: self.addr.clone(), event });
    }
}
///handleo de la operacion de retorno de bici, encola el evento pendiente, y avisa al lider que la 
/// operacion termino.
impl Handler<ExecReturn> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: ExecReturn, ctx: &mut Self::Context) -> Self::Result {
        self.available_bikes.push(msg.bike_id);
        let event = StationEvent::BikeReturned {
            event_id: self.next_event_id(),
            user_id: msg.user_id,
            station_id: self.station_id,
            bike_id: msg.bike_id,
            start_time: msg.start_time,
            return_time: chrono::Utc::now(),
            payment_method: msg.payment,
        };
        self.pending_events.push_back(event.clone());
        self.log_event(&format!("BIKE_RETURNED_LOCAL_ACCEPT: {}", msg.bike_id));

        //Guardamos el evento serializado de devolución en el log.
        // Así, si caemos antes del ACK, podremos reconstruir este pendiente.
        self.log_event(&format!("PENDING_EVENT_ADD: {}", serde_json::to_string(&event).unwrap()));

        println!("[Estación] Devolución local aceptada y encolada: Bici {}", msg.bike_id);

        ctx.address().do_send(SyncEventToLeader { origin_addr: self.addr.clone(), event });
    }
}
///hanldeo de los ACK, respuestas del lider a los avisos de recibidos. busca el id del evento que 
/// matchee el que se recibio,verifica que el lider haya confirmado la devolucion y por lo tanto
/// puedo generar la informacion de cobro y  luego lanzar una tarea para verificar que e pago 
/// se complete o no, si falla encolo a los pagos pendientes. Cuando se confirma, se elimina de la cola
impl Handler<ReceiveAck> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: ReceiveAck, ctx: &mut Self::Context) -> Self::Result {
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
            
            //Solo registramos la remoción del log si fue una devolución confirmada.
            // Los alquileres siguen pendientes de remoción hasta que el viaje entero termine (ClosedTrip).
            self.log_event(&format!("PENDING_EVENT_REMOVE: {}", event_id));
        }

        self.pending_events.retain(|e| match e {
            StationEvent::BikeReturned { event_id: id, .. } => *id != event_id,
            _ => true,
        });
    }
}
///hadleo de que se aviso que el viaje cerro, por lo tanto ya puedo eliminar el viaje de mi cola
impl Handler<ReceiveClosedTrip> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: ReceiveClosedTrip, _ctx: &mut Self::Context) -> Self::Result {
        
        //Buscamos el ID del evento original de alquiler para marcarlo
        // como eliminado en el log, ya que el viaje cerró globalmente y ya no está pendiente.
        if let Some(StationEvent::BikeRented { event_id, .. }) = self.pending_events.iter().find(|e| match e {
            StationEvent::BikeRented { bike_id, .. } => *bike_id == msg.0,
            _ => false,
        }) {
            self.log_event(&format!("PENDING_EVENT_REMOVE: {}", event_id));
        }

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


/// handleo de mensaje para mantener sincronizada la red, el lider mantiene actualizado el estado
/// global del sistema, si el lider arranca en blaco por ejemplo por una nueva votacion
/// tengo que reconstruir todo los eventos pendientes, lanzo rafaga de mensajes para ello
impl Handler<SyncEventToLeader> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: SyncEventToLeader, ctx: &mut Self::Context) -> Self::Result {
        if self.is_electing || self.leader.is_none() { return; }

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
///Funcion para comenzar a reconstruir los eventos pendientes
impl Handler<ReplayPendingEvents> for StationActor {
    type Result = ();
    fn handle(&mut self, _msg: ReplayPendingEvents, ctx: &mut Self::Context) -> Self::Result {
        let self_addr = ctx.address();
        // Diferimos la ejecución medio segundo para asegurar que el líder levantó el thread TCP.
        ctx.spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            self_addr.do_send(ExecuteReplay);
        }.into_actor(self));
    }
}
///Funcion para ir sincronizando los eventos 
impl Handler<ExecuteReplay> for StationActor {
    type Result = ();
    fn handle(&mut self, _msg: ExecuteReplay, ctx: &mut Self::Context) -> Self::Result {
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
        // Si el mensaje dio la vuelta y llegó al líder que lo originó, se descarta.
        if msg.origin_addr == self.addr { return; }

        println!("[Red] Solicitud del lider para reconstruior. Enviando eventos pendientes al nuevo líder...");
        ctx.address().do_send(ReplayPendingEvents);

        // Pasamos el mensaje al siguiente nodo en el anillo restando 1 al TTL.
        if msg.ttl > 0 {
            if let Some(next) = &self.next_station_addr {
                let target = next.clone();
                ctx.spawn(async move {
                    let _ = Self::send_tcp(target, TcpMessage::RequestPendingEvents { origin_addr: msg.origin_addr, ttl: msg.ttl - 1 }).await;
                }.into_actor(self));
            }
        }
    }
}

///Si soy el lider muestro el registro global de viajes, si no lo soy paso el mensaje a mi siguiente
///de esta forma siempre tenemos al lider como actor de la verdad
impl Handler<ConsoleQueryTrips> for StationActor {
    type Result = ();
    fn handle(&mut self, _msg: ConsoleQueryTrips, ctx: &mut Self::Context) -> Self::Result {
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
        if self.is_electing { return; }
        
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
///handleo para setear el lider, luego lanzo tarea para rejecutar los eventos pendientes,
/// ya que como se llavo a setear lider, este cambio y por lo tanto tenemos que sincronizar
impl Handler<SetLeaderMsg> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: SetLeaderMsg, ctx: &mut Self::Context) -> Self::Result {
        self.set_leader(msg.leader_id, msg.leader_addr);
        ctx.address().do_send(ReplayPendingEvents);
    }
}
///Actualizo mi vecino
impl Handler<UpdateNextNode> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: UpdateNextNode, _ctx: &mut Self::Context) -> Self::Result {
        self.next_station_addr = Some(msg.0.clone());
        println!("[Red] Enlace establecido hacia: {}", msg.0);
    }
}
///Me autodeclaro lider, porque o bien gane la eleccion o bien estoy solo
impl Handler<DeclareSelfLeader> for StationActor {
    type Result = ();
    fn handle(&mut self, _msg: DeclareSelfLeader, ctx: &mut Self::Context) -> Self::Result {
        self.is_electing = false;
        let was_leader = self.leader.as_ref().map_or(false, |l| l.station_id == self.station_id);
        
        self.leader = Some(StationInfo {
            station_id: self.station_id,
            location: self.location,
            addr: self.addr.clone(),
        });

        if !was_leader {
            self.active_trips.clear();
            self.orphan_returns.clear();
        }
        ctx.address().do_send(ReplayPendingEvents);
    }
}
///pruebo si mi siguiente normal (es decir mi id +1), esta vivo, si lo esta y me
/// contesta, pero no era mi siguiente quiere decir que se callo
/// por lo tanto lo vuelvo a incluir, si no esta vivo, mando mensaje a su siguiente y asi, hasta
/// actualizar mi nuevo vecino, si mi vecino era el lider, llamo a eleccion
impl Handler<CheckNeighborHealth> for StationActor {
    type Result = ();
    fn handle(&mut self, _msg: CheckNeighborHealth, ctx: &mut Self::Context) -> Self::Result {
        let current_next = self.next_station_addr.clone();
        let self_addr = ctx.address();
        
        let mut sorted = self.known_stations.clone();
        sorted.sort_by_key(|s| s.station_id);
        let my_idx = sorted.iter().position(|s| s.station_id == self.station_id).unwrap();
        let ideal_next = &sorted[(my_idx + 1) % sorted.len()];
        let ideal_addr = ideal_next.addr.clone();

        ctx.spawn(async move {
            if Some(ideal_addr.clone()) != current_next {
                if TcpStream::connect(&ideal_addr).await.is_ok() {
                    self_addr.do_send(UpdateNextNode(ideal_addr.clone()));
                    return;
                }
            }

            if let Some(addr) = current_next {
                if TcpStream::connect(&addr).await.is_err() {
                    self_addr.do_send(HealRing { dead_addr: addr });
                }
            }
        }.into_actor(self));
    }
}
///trato de curar la red de anillo, si el que murio, era el lider mando a eleccion, sino solo actualizo
impl Handler<HealRing> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: HealRing, ctx: &mut Self::Context) -> Self::Result {
        let dead_addr = msg.dead_addr;
        let was_leader = self.leader.as_ref().map_or(false, |l| l.addr == dead_addr);

        self.is_electing = false; 
        
        let mut sorted = self.known_stations.clone();
        sorted.sort_by_key(|s| s.station_id);
        let my_id = self.station_id;
        let self_addr = ctx.address();

        ctx.spawn(async move {
            if let Some(my_idx) = sorted.iter().position(|s| s.station_id == my_id) {
                let n = sorted.len();
                let mut found_any = false;
                
                for offset in 1..n {
                    let target = &sorted[(my_idx + offset) % n];
                    if target.addr == dead_addr { continue; } 
                    
                    if TcpStream::connect(&target.addr).await.is_ok() {
                        self_addr.do_send(UpdateNextNode(target.addr.clone()));
                        found_any = true;
                        break;
                    }
                }
                
                if !found_any {
                    self_addr.do_send(DeclareSelfLeader);
                } else if was_leader {
                    println!("Lider caído detectado arranco eleccion.");
                    self_addr.do_send(StartElection);
                }
            }
        }.into_actor(self));
    }
}
///Eleecion en red de anillo
impl Handler<StartElection> for StationActor {
    type Result = ();
    fn handle(&mut self, _msg: StartElection, ctx: &mut Self::Context) -> Self::Result {
        if self.is_electing { return; }
        self.is_electing = true;
        
        if let Some(next_addr) = &self.next_station_addr {
            let target = next_addr.clone();
            let msg = TcpMessage::RingElection { active_nodes: vec![self.station_id] };
            ctx.spawn(async move {
                let _ = Self::send_tcp(target, msg).await;
            }.into_actor(self));
        }
    }
}

impl Handler<ProcessRingElection> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: ProcessRingElection, ctx: &mut Self::Context) -> Self::Result {
        let mut active_nodes = msg.0;
        if !self.is_electing { self.is_electing = true; }

        if active_nodes[0] == self.station_id {
            let leader_id = *active_nodes.iter().max().unwrap();
            let leader_info = self.known_stations.iter().find(|s| s.station_id == leader_id).unwrap();
            let leader_addr = leader_info.addr.clone();

            self.set_leader(leader_id, leader_addr.clone());
            ctx.address().do_send(ReplayPendingEvents);

            //El nuevo líder pide explícitamente a la red que envíe todos sus eventos pendientes 
            // para poder reconstruir el registro global de viajes perdidos.
            if let Some(next) = &self.next_station_addr {
                let target = next.clone();
                let req_msg = TcpMessage::RequestPendingEvents { origin_addr: self.addr.clone(), ttl: self.known_stations.len() };
                ctx.spawn(async move {
                    let _ = Self::send_tcp(target, req_msg).await;
                }.into_actor(self));
            }

            if let Some(next) = &self.next_station_addr {
                let target = next.clone();
                let initiator = self.station_id;
                ctx.spawn(async move {
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
                ctx.spawn(async move {
                    let _ = Self::send_tcp(target, TcpMessage::RingElection { active_nodes }).await;
                }.into_actor(self));
            }
        }
    }
}

impl Handler<ProcessRingElected> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: ProcessRingElected, ctx: &mut Self::Context) -> Self::Result {
        if msg.initiator_id == self.station_id {
            return;
        }

        self.set_leader(msg.leader_id, msg.leader_addr.clone());
        ctx.address().do_send(ReplayPendingEvents);

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
                        TcpMessage::Availability { .. } | TcpMessage::RentResponse { .. } | TcpMessage::ReturnResponse { .. } => {
                            println!("[Red] EL paquete no tiene la serializacion correcta, descartado.");
                        }
                    }
                }
            }
        }.into_actor(self));
    }
}


//Funciones auxiliares

///fucnion para procesar y leer los archivos de configuracion para armar la estacion
fn load_stations_from_config(path: &str) -> Vec<StationInfo> {
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
fn recover_state_from_log(log_path: &PathBuf, mut available_bikes: Vec<u32>) -> (Vec<u32>, u32, VecDeque<StationEvent>) {
    let mut event_counter = 1;
    //usamos un mapa para ir guardando y borrando los eventos según el log
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
            //Recuperación de eventos pendientes.
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

///Main asyncrono para poder manejar el lanzamiento del actor, asi como tambien las operaciones I/O
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

    let known_stations = load_stations_from_config(config_path);
    let self_station = known_stations.iter().find(|s| s.station_id == station_id).unwrap_or_else(|| {
        eprintln!("Error: ID {} no localizado en red configurada.", station_id);
        std::process::exit(1);
    }).clone();

    let port = self_station.addr.split(':').last().unwrap();
    let bank_addr = BANK_ADDR.to_string();
    let gateway = PaymentGateway { bank_addr }.start();
    let start = 1 + 10 * (station_id - 1);
    let end = station_id * 10;
    let initial_bikes: Vec<u32> = (start..=end).collect();
    let log_path = PathBuf::from(format!("station_{}.log", port));
    
    // Desestructuramos la tupla ampliada que ahora también trae los `recovered_pending_events`
    let (recovered_bikes, recovered_event_counter, recovered_pending_events) = recover_state_from_log(&log_path, initial_bikes);

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
        pending_payments: VecDeque::new(),
        active_trips: HashMap::new(),
        orphan_returns: HashMap::new(),
        log_path: PathBuf::from(format!("station_{}.log", port)),
        is_electing: false,
        event_counter: recovered_event_counter, //Ahora usamos el "recuperado" que vale 0 si es la primera vez que se lanza
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
            }
        }
    });

    tokio::signal::ctrl_c().await?;
    Ok(())
}