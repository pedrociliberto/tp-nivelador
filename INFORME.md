# Pedro Ciliberto - Informe de Solución: TP0 - Sistemas Distribuidos

A continuación explico la arquitectura general y la estructura del código que implementé para el TP0 de Sistemas Distribuidos.

## **1. Arquitectura General y Estructura del Código**

La estructura del proyecto se organiza de la siguiente manera:

- **Cliente (`src/client/`)**:
  - `client.go`: se encarga de conectarse al servidor con reintentos configurables, procesar el archivo de apuestas de la agencia por *batches*, enviar los datos al servidor y recibir las apuestas ganadoras para escribirlas en el archivo de salida.
  - `main.go`: inicializa la configuración desde variables de entorno y maneja la **captura de señales del sistema** para iniciar un *Graceful Shutdown*.

- **Servidor (`src/server/`)**:
  - `server.py`: implementa el servidor TCP multihilo. Gestiona el ciclo de vida de los hilos de atención a clientes (`threading.Thread`), la barrera de sincronización para el quórum de agencias listas (`threading.Barrier`), el almacenamiento *thread-safe* de apuestas y la notificación a ganadores.
  - `main.py`: punto de entrada del servidor. Instancia y ejecuta la clase `Server`.

- **Protocolo de Comunicación (`src/protocol/` en Go y `protocol.py` en Python)**:
  - Módulo encargado de la **encapsulación, serialización y deserialización** de mensajes entre Go y Python.

- **Sockets Seguros (`src/safe_socket/` en Go y `safe_socket.py` en Python)**:
  - Abstracción sobre los sockets TCP para garantizar el envío y recepción completa de buffers de datos (`SendAll` / `RecvAll`), previniendo *short-reads* y *short-writes*.

## **2. Protocolo de Comunicación**

Para el protocolo utilicé un esquema LV (Length-Value) utilizando representación binaria en Big-Endian (Network Byte Order) para los encabezados.

### **2.1. Estructura de Mensajes**

1. **Encabezado de Longitud/ID (`Header`)**:
   - **Tamaño**: 4 bytes (`uint32` fijo).
   - **Función**: Representa la longitud exacta en bytes del contenido (`payload`) que le sigue a continuación, o bien actúa como un identificador de control (para comenzar la comunicación).

2. **Contenido (`Payload`)**:
   - **Tamaño**: Variable, definido por el valor del Header.
   - **Función**: Cadena de texto codificada en `UTF-8` conteniendo una o más apuestas en formato CSV delimitadas por salto de línea (`\n`).

### **2.2. Flujo de Transmisión**

1. **Handshake / Identificación de Agencia**:
   - El cliente inicia la conexión enviando un *Header* de 4 bytes que transporta su `agency_id` numérico.

2. **Envío de Lotes de Apuestas (*Batches*)**:
   - El cliente lee y agrupa las apuestas del archivo CSV en memoria hasta alcanzar el límite configurado por `BATCH_SIZE`.
   - Transmite un *Header* con la longitud exacta en bytes del *batch* generado, seguido de su `Payload`.
   - El servidor procesa lo, almacenándolo de forma segura y responde con un **ACK** (encabezado binario de 4 bytes con valor `1`).

3. **Fin de Transmisión de Apuestas**:
   - Al finalizar la lectura del archivo de entrada, el cliente transmite un *Header* especial con valor `0` (`END_OF_BETS_HEADER_ID`) para informarle al servidor que finalizó el envío de apuestas. De esta manera, el servidor puede evaluar si se alcanzó el **quórum mínimo de agencias** para proceder con la selección de ganadores.

4. **Notificación de Ganadores**:
   - Una vez alcanzado el quórum, el servidor evalúa el sorteo y envía línea por línea las apuestas ganadoras pertenecientes a esa agencia específica mediante el formato `LV` (similar a lo anterior).
   - Para concluir el canal de transmisión, el servidor envía un mensaje con longitud `0` (`END_OF_WINNERS_DELIMITER`) indicando el fin de los resultados.

## **3. Mecanismos de Concurrencia y Sincronización**

### **3.1. Concurrencia en el Servidor (Python)**

- **Modelo Multihilo**: El servidor atiende las conexiones TCP de forma concurrente asignando un hilo independiente (`threading.Thread`) a cada cliente aceptado en el socket principal.

### **3.2. Primitivas de Sincronización**

- **Barrera de Quórum (`threading.Barrier`)**:
  - Garantiza que ninguna agencia reciba los resultados de los ganadores antes de que el conjunto mínimo de agencias (`AGENCY_QUORUM_MIN`) haya completado la carga de sus apuestas.
  - Cada hilo de cliente procesa sus apuestas y luego invoca `quorum_barrier.wait()`, quedando bloqueado hasta que la última agencia requerida alcanza el punto de sincronización.

- **Exclusión Mutua (`threading.Lock`)**:
  - **Persistencia de Apuestas (`lottery_lock`)**: Controla el acceso exclusivo al archivo de almacenamiento único (`server_bets.csv`), evitando *race conditions* o la mezcla corrupta de líneas escritas por diferentes hilos. En mi primera solución, utilizaba distintos archivos de almacenamiento (uno por cliente), en la que no se necesitaban mecanismos reales de sincronización. De todos modos, para aprovechar la persistencia de apuestas en un único archivo, ahí sí fue necesario implementar un *lock* para garantizar la **integridad de los datos entre cada agencia**.
  - **Registro de Clientes Activos (`clients_lock`)**: Permite llevar un rastreo *thread-safe* de los sockets activos para dar la posibilidad de cerrarlos durante el procedimiento de *shutdown*.

## **4. Manejo Eficiente de Memoria y Graceful Shutdown**

### **4.1. Optimización del Uso de Memoria en el Cliente (Go)**

- Se hace uso de `bufio.Scanner` sobre el archivo de apuestas para procesar las líneas por demanda y una por una, evitando cargar archivos pesados en memoria.
- El slice utilizado para armar el lote (`make([]string, 0, batchSize)`) se limpia mediante *reslicing* (`batch = batch[:0]`) tras cada envío exitoso, reutilizando la capacidad reservada subyacente.

### **4.2. Apagado Controlado (*Graceful Shutdown*)**

Tanto el cliente como el servidor implementan la captura explícita de señales del sistema operativo (`SIGTERM` / `SIGINT`):

- **Cliente (Go)**:
  - Escucha la cancelación del contexto (`signal.NotifyContext`) dentro de los bucles de envío y lectura.
  - Ante una señal de interrupción, cancela la ejecución inmediatamente, interrumpe bloqueos y ejecuta los bloques `defer` para cerrar sockets y descriptores de archivos de forma limpia.

- **Servidor (Python)**:
  - La función `_handle_signal` actualiza `running` a `False`.
  - Destraba la barrera de sincronización invocando `quorum_barrier.abort()` para **liberar los hilos de agencias** que hayan quedado bloqueados esperando el quórum.
  - Aplica `shutdown` y `close` sobre el socket principal de escucha y todos los sockets de clientes activos para **desbloquear operaciones** `accept` o `recv` pendientes.
  - Realiza un `join` acotado por un ***timeout* constante** (`SHUTDOWN_THREAD_TIMEOUT_SEC`) sobre cada hilo secundario para garantizar que el tiempo de cierre sea conocido y acotado antes de finalizar el proceso principal.
