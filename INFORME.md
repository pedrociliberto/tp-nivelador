# Pedro Ciliberto - Informe de Solución: TP0 - Sistemas Distribuidos

A continuación explico la **arquitectura general** y la estructura del código que implementé para el TP0 de Sistemas Distribuidos. Menciono cómo se organiza el proyecto, el **protocolo de comunicación** entre cliente y servidor, los mecanismos de **concurrencia y sincronización** utilizados, así como las estrategias implementadas para un manejo eficiente de memoria y un *Graceful Shutdown*.

## Cómo levantar el sistema

Para ejecutar el sistema completo, se utiliza el `Makefile` del proyecto, el cual abstrae la gestión de la infraestructura definida en Docker Compose. Al ejecutar el comando `make up`, se prepara el entorno local creando el directorio de salida `./output`, limpiando cualquier archivo previo y compilando las imágenes Docker del servidor y de los seis clientes (agencias 0 a 5). Posteriormente, levanta los contenedores en segundo plano (`--detach`) eliminando contenedores huérfanos anteriores. El servidor expone el puerto `5678`, y para mi ejecución personal, impone un quórum mínimo de seis agencias (`AGENCY_QUORUM_MIN=6`) para poder testear manualmente el funcionamiento de los ganadores de apuestas. Cada cliente se conecta de forma individual montando en modo lectura el directorio `./input` para procesar sus respectivas apuestas (`input-X.csv`) con un tamaño de lote configurable (`BATCH_SIZE=16`) y guardar los resultados generados en el volumen `./output`.

Para observar el progreso del envío de apuestas, sincronización de quórum y cierre *graceful*, se utiliza el comando `make logs`, el cual transmite la salida de todos los servicios mediante `docker compose logs --follow`. Una vez finalizado el procesamiento o si se requiere detener la ejecución de manera controlada, el comando `make down` envía un `SIGTERM` con un tiempo de gracia de 5 segundos (`-t 5`) para asegurar que tanto el servidor como los clientes liberen sockets y cierren descriptores de archivos adecuadamente antes de remover los contenedores. Adicionalmente, las pruebas se ejecutan con `make test`, la cual limpia los registros de pruebas anteriores y ejecuta el script de validación en Python (`tests/run.py`).

## **1. Arquitectura General y Estructura del Código**

La estructura del proyecto se organiza de la siguiente manera:

- **Cliente (`src/client/`)**:
  - `client.go`: se encarga de conectarse al servidor con reintentos configurables, procesar el archivo de apuestas de la agencia por *batches*, enviar los datos al servidor y recibir las apuestas ganadoras para escribirlas en el archivo de salida.
  - `main.go`: inicializa la configuración desde variables de entorno y maneja la **captura de señales del sistema** para iniciar un *Graceful Shutdown*.

- **Servidor (`src/server/`)**:
  - `server.py`: implementa el servidor TCP multihilo. Gestiona el ciclo de vida de los hilos de atención a clientes (`threading.Thread`), la barrera de sincronización para el quórum de agencias listas (`threading.Barrier`), el almacenamiento *thread-safe* de apuestas y la notificación a ganadores.
  - `formatting.py`: contiene funciones auxiliares para el formateo de apuestas y ganadores.
  - `main.py`: punto de entrada del servidor. Instancia y ejecuta la clase `Server`.

- **Protocolo de Comunicación (`src/protocol/` en Go y `protocol.py` en Python)**:
  - Módulo encargado de la **encapsulación, serialización y deserialización** de mensajes entre Go y Python.

- **Safe Sockets (`src/safe_socket/` en Go y `safe_socket.py` en Python)**:
  - Abstracción sobre los sockets TCP para garantizar el envío y recepción completa de buffers de datos (`SendAll` / `RecvAll`), previniendo *short-reads* y *short-writes*.

## **2. Protocolo de Comunicación**

Para el protocolo utilicé un **esquema LV (Length-Value)** utilizando representación binaria en Big-Endian (Network Byte Order) para los encabezados.

### **2.1. Estructura de Mensajes**

1. **Encabezado de Longitud/ID (`Header`)**:
   - **Tamaño**: 4 bytes (`uint32` fijo).
   - **Función**: Representa la longitud exacta en bytes del contenido (`payload`) que le sigue a continuación, o bien actúa como un identificador de control (para comenzar la comunicación).

2. **Contenido (`Payload`)**:
   - **Tamaño**: Variable, definido por el valor del Header.
   - **Función**: Cadena de texto conteniendo una o más apuestas en formato CSV delimitadas por salto de línea (`\n`).

### **2.2. Flujo de Transmisión**

1. **Handshake / Identificación de Agencia**:
   - El cliente inicia la conexión enviando un *Header* de 4 bytes que transporta su `agency_id` numérico.

2. **Envío de Lotes de Apuestas (*Batches*)**:
   - El cliente lee y agrupa las apuestas del archivo *.csv* en memoria hasta alcanzar el límite configurado por `BATCH_SIZE`.
   - Transmite un *Header* con la longitud exacta en bytes del *batch* generado, seguido de su `Payload`.
   - El servidor procesa el *batch* recibido, almacenándolo de forma segura y responde con un **ACK** (header binario de 4 bytes con valor `1`).

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
- El slice utilizado para armar el lote (`make([]string, 0, batchSize)`) se limpia mediante *reslicing* (`batch = batch[:0]`) tras cada envío exitoso, reutilizando la capacidad reservada anteriormente.

### **4.2. *Graceful Shutdown***

Tanto el cliente como el servidor implementan un mecanismo robusto de captura de señales del sistema operativo (`SIGTERM` / `SIGINT`) para garantizar una interrupción limpia, evitar la pérdida de datos y prevenir la existencia de recursos huérfanos o procesos colgados.

- **Cliente (Go)**:
  - En el punto de entrada (`main.go`) se utiliza `signal.NotifyContext` sobre el contexto. Ante una señal del sistema, el contexto va a propagar su cancelación a todas las subrutinas activas.
  - Debido a que llamadas como `conn.Read()` o `conn.Write()` son bloqueantes, solo un chequeo de `ctx.Err()` en un bucle es insuficiente si la goroutine está esperando respuesta de red. Para solucionar esto, se implementa un monitor de contexto que invoca `client.conn.Close()` inmediatamente al activarse la señal. Esto fuerza la destrucción del *FD*, destrabando las lecturas/escrituras pendientes (`net.ErrClosed`). Si llega un SIGTERM, `ctx.Done()` se activa, el monitor cierra la conexión y la goroutine principal puede salir de su bucle de envío/recepción de forma ordenada. En caso de que el cliente haya terminado con éxito o con un error normal, la función `Run()` llega al final y se ejecuta `defer close(ctxStopMonitor)`, lo que detiene el monitor de contexto y evita la goroutine quede activa.
  - En cada iteración de los bucles de envío de apuestas y recepción de ganadores, se verifica `if ctx.Err() != nil`. Si la señal fue recibida, la función interrumpe la ejecución de forma ordenada y retorna `ctx.Err()`.
  - Con las sentencias `defer`, se asegura el cierre de los *FDs* de entrada/salida (`inputFile.Close()`, `outputFile.Close()`) y del socket TCP (`conn.Close()`), garantizando que no queden handles abiertos en el sistema.

- **Servidor (Python)**:
  - La función `_handle_signal(signum, frame)` registra las señales del sistema. Al activarse, actualiza `running = False` para impedir que el servidor acepte nuevas conexiones o siga procesando batches de apuestas.
  - Se aplica `shutdown(socket.SHUT_RDWR)` y `close()` sobre `server_socket`. Esto destraba inmediatamente el bloqueo en la llamada `server_socket.accept()` del hilo principal, permitiéndole salir de forma limpia de su bucle.
  - Los hilos de agencias procesando clientes pueden encontrarse suspendidos esperando a otras agencias en `quorum_barrier.wait()`. El manejador invoca explícitamente `quorum_barrier.abort()`, forzando una excepción `threading.BrokenBarrierError` en todos los hilos en espera para liberarlos al instante sin requerir que se complete el quórum (si no quedarían bloqueados).
  - Iterando bajo `clients_lock`, el servidor invoca `shutdown` y `close` sobre la lista de sockets de clientes activos (`active_clients`). Esto aborta inmediatamente cualquier llamada `recv()` o `send()` bloqueante en los hilos secundarios.
  - En el bloque `finally` de `run()`, el hilo principal realiza una iteración sobre `active_threads`, ejecutando un `join` sobre cada thread activo. Esto asegura un tiempo de finalización acotado y determinístico, evitando procesos "zombie" y permitiendo que el servidor termine con código de retorno `0`.
