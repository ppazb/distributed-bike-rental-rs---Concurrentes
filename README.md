# Pre-Entrega Trabajo Práctico Bicicletas 

## Integrantes
- Rivero Trujillo, Tobias - 106302
- Paz Blanco, Pilar - 105600
- Chen, Michelle Melisa - 105506

## Observaciones
Se considera la utilización del crate Tokio para trabajo asíncrono. Así como también crate para manejo de timestamp.
## Diseño y Estructuras
Se utiliza el modelo de actores en su totatilad para modelar el problema.
### Usuario
El usuario será uno de los binarios presentes, se lanzaran tareas por cada usuario, de manera de consumir menos memoria y trabajar de manera asíncrona, en lugar de lanzar varias veces el mismo proceso.
La responsabilidad del usuario será iniciar la comunicación con las estaciones donde desea devolver o tomar una bicicleta.A su vez debe tener un medio de pago asociado y conocer su ubicación.

``` 
Struct User {
    user_id: usize,
    current_bike: Option<(bike_id: usize, rent_time: timestamp)> // (bike_id, timestamp)
    location:(f32,f32),
    payment_method*,
    stations: Vec<(AddrStation, distance)>
    Socket: Option<TcpStream, None> // de tokio
}
```

### Pago
Decidimos implementar Actor Payment que simularía las responsabilidades de un Banco. Los usuarios en primera instancia tienen que cargar sus medios de pago (tarjeta con su numero y código de seguridad, que se asume es válido). Luego cuando tiene que realizar un pago selecciona la tarjeta a pagar, y le pide una bicicleta a la estación.
Pueden ocurrir dos escenarios: 
1. La estación tiene Internet, por lo que cobra a ese numero de tarjeta el monto. 
2. En caso que la estación no tenga conexión, encola el dato de tarjeta y el monto para reintentar cuando tenga conexión. 

En este modelo, se plantea todo el intercambio de mensajes estación-usuario, pero se asume que habiendo conexión existe una funcion procesar_pago(n_tajeta, monto), que realiza el cobro fuera de nuestro scope, por ejemplo debitando a un banco. En nuestro modelo procesar_pago devolveria true y derivaria responsabilidades de deuda al banco hipotético.

~~~
Struct Payment {
    authorized_users: HashMap<user_id, String> // (user_id, payment_method)
}
~~~

### Estación
Cada estación tendrá una ubicación modelada como (x,y). En principio las terminales no esta conectadas entre si, puesto que con el diseño actual no encontramos necesidad de esto. En caso de que fuera necesario optamos por una topología en anillo, para disminuir la cantidad de nodo--aristas--nodo en la conexión.
~~~
Struct Station {
    station_id: usize
    free_slots: i32,
    bikes_id: Vec<i32>,
    bikes_rented: Vec<i32>, 
    leader: AddrStation,
    neighbors: Vec<AddrStation>, 
    next: AddrStation, 
    socket:TcpStream //socket de tokio,
    peding_events: VecDeque<(user_id, credit_card: String, amount: usize)>
}
~~~

#### Operación frente a caidas
Consideramos que dado que el n de biciletas es reducido (entre 10 y 20) la cantidad total de operaciones que puede ejercer por minuto es suficientemente pequeña para permitirnos actualizar un archivo de log, a medida que se ejecuta cada operación. Es decir que cada vez que se extrae o coloca una bicicleta volcar esta información en un archivo, nos permite recuperación en caso de caídas de la estación.

#### Consistencia: slots libre y bicicleta
Tras analizar los casos, no consideramos que incialmente los slots tengan suficiente funcionalidad para ser distingibles, basandonos en los requerimientos funcionales.
Debido a esto solo nos interesan 2 datos, la cantidad de slots libres y que bicicletas tiene la estacion.
Para modelar esto planteamos un modelo similar a el problema de productor-consumidor, ya que tenemos un contador de slots libres y una lista de id_bicicletas disponibles.
La idea del modelado es mantener siempre consistente la cantidad de libres frente a varias operaciones sobre las bicicletas.

## Caso Feliz: Prestado
| Emisor | Mensaje | Receptor | Comportamiento |
| :---: | :---: | :---: | :--- |
| Usuario | ConnectarAEstación | Estación | El Usuario se intenta conectar a la Estación más cercana. Una vez que el usuario se conecte a la Estación con éxito, **usuario.socket = Some(TcpStream)** |
| Usuario | SolicitarBici(bici: id_bicicleta, medio: TarjetaCredito) | Estación | La Estación recibe el pedido de prestado|
| Estación | Disponibles | Usuario | La Estación le manda al Usuario la lista de bicis disponibles y slots libres al Usuario conectado **id_bicis: Vec<id_bicicleta>**  |
| Estación | SolicitudPreAutorizacion(tarjeta: TarjetaCredito) | Pago | La Estación envía un mensaje de solicitud Pre-Autorización y espera la respuesta **enum ResultTransaction {SUCCESS, FAILED}**|
| Pago | PreAutorizacionExitoso | Estacion | La Estación una vez que recibe SUCCESS del lado de Pago, actualiza agregando 1 a **libres** e quita el el **id_biciclieta prestada** de la lista|
| Estación | SolicitudOK | Usuario | El Usuario una vez que recibe el mensaje SolicitudOK, **bicicleta = Some((id_bicicleta, timestamp))** para arrancar el viaje|
| Estación | AvisarPrestamo | Estacion | La Estación agrega el id de la bici prestada a la lista de **bikes_rented**|

## Caso Feliz: Devolución
| Emisor | Mensaje | Receptor | Comportamiento |
| :---: | :---: | :---: | :--- |
| Usuario | ConnectarAEstación | Estación | El Usuario se intenta conectar a la Estación más cercana. Una vez que el usuario se conecte a la Estación con éxito, **usuario.socket = Some(TcpStream)** |
| Usuario | DevolverBici(bici: id_bicicleta, timestamp: Timestamp, medio: TarjetaCredito) | Estación | La Estación recibe la solicitud de devolución, calcula el tiempo de viaje con el tiempo actual y calcula el monto a cobrar|
| Estación | cobro(tarjeta: TarjetaCredito, monto: usize) | Pago | La Estación envía un mensaje de de cobro y espera la respuesta **enum ResultTransaction {SUCCESS, FAILED}**|
| Pago | CobroExitoso | Estacion | La Estación una vez que recibe SUCCESS del lado de Pago, actualiza descontando 1 a **libres** e inserta el **id_biciclieta devuelta** a la lista|
| Estación | DevolucionOK | Usuario | No importa si el cobro fue SUCCESS o FAILED el Usuario recibe **DevolucionOK**. El Usuario una vez que recibe el mensaje DevolucionOK, **bicicleta = None** indicando que no posee de bicicleta|
| Estación | AvisarDevolucion | Estacion | La Estación chequea si el id de la bici devuelta está en la lista de **bikes_rented**, en caso positivo, quita el id de la lista, en caso contrario, avisa la devolución de la bici **(?: pendiente quién se encarga de esto: líder o lanzar aviso)**|

## Caso Cobro Denegado: Devolución 
| Emisor | Mensaje | Receptor | Comportamiento |
| :---: | :---: | :---: | :--- |
| Usuario | ConnectarAEstación | Estación | El Usuario se intenta conectar a la Estación más cercana. Una vez que el usuario se conecte a la Estación con éxito, **usuario.socket = Some(TcpStream)** |
| Usuario | DevolverBici(bici: id_bicicleta, timestamp: Timestamp, medio: TarjetaCredito) | Estación | La Estación recibe la solicitud de devolución, calcula el tiempo de viaje con el tiempo actual y calcula el monto a cobrar|
| Estación | cobro(tarjeta: TarjetaCredito, monto: usize) | Pago | La Estación envía un mensaje de de cobro y espera la respuesta **enum ResultTransaction {SUCCESS, FAILED}**|
| | Timeout  | Estacion | La Estación encola el cobro sin respuesta en **PendingEvent**|
| Estacion | CheckPaymentAlive (?)  | Pago | Cuando hay conexión de Pago, intentamos resolver los cobros pendientes|
| Estación | DevolucionOK | Usuario | No importa si el cobro fue SUCCESS o FAILED el Usuario recibe **DevolucionOK**. El Usuario una vez que recibe el mensaje DevolucionOK, **bicicleta = None** indicando que no posee de bicicleta|

## Caso No Hay Espacio: Devolución 
| Emisor | Mensaje | Receptor | Comportamiento |
| :---: | :---: | :---: | :--- |
| Usuario | ConnectarAEstación | Estación | El Usuario se intenta conectar a la Estación más cercana. Una vez que el usuario se conecte a la Estación con éxito, **usuario.socket = Some(TcpStream)** |
| Usuario | DevolverBici(bici: id_bicicleta, timestamp: Timestamp, medio: TarjetaCredito) | Estación | La Estación recibe la solicitud de devolución, calcula el tiempo de viaje con el tiempo actual y calcula el monto a cobrar|
| Estación | SinEspacio | Usuario | La Estación rechaza la devolución de bici del Usuario|

## Caso Caída Estación: Préstamo y Devolución
| Emisor | Mensaje | Receptor | Comportamiento |
| :---: | :---: | :---: | :--- |
| Usuario | ConnectarAEstación | Estación | El Usuario se intenta conectar a la Estación más cercana. Una vez que el usuario se conecte a la Estación con éxito, **usuario.socket = Some(TcpStream)** |
| Usuario | **DevolverBici**(bici: id_bicicleta, timestamp: Timestamp, medio: TarjetaCredito) ó **SolicitarBici**(bici: id_bicicleta, medio: TarjetaCredito)  | Estación | La Estación recibe la solicitud de devolución, calcula el tiempo de viaje con el tiempo actual y calcula el monto a cobrar|
| | Timeout  | Estacion | La Estación encola **enum PedingEvent {PreAuthorization, Collection}** sin respuesta en **PendingEvent**|
| Estacion | CheckPaymentAlive (?)  | Pago | Cuando hay conexión de Pago, intentamos resolver los cobros pendientes|
| Estación | DevolucionOK ó SolicitudOK | Usuario | **currentBike = None o currentBike = Some((id_bike, timestamp))**|


[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/KujF6lFv)
