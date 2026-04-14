# Arquitectura Kafka - Proyecto Formación

## Visión General

Proyecto de formación Kafka que implementa una tienda virtual con:

- **usuarios-service** (Productor): Gestiona usuarios, publica eventos de compra
- **cupones-service** (Consumidor): Consume compras, genera cupones de descuento para pedidos > 500€

---

## Estructura del Proyecto

```text
microservicios/
├── usuarios-service/          # Productor
│   └── src/main/java/com/tienda/usuarios/
│       ├── UsuariosServiceApplication.java
│       ├── config/KafkaProducerConfig.java
│       ├── service/CompraService.java
│       └── controller/CompraController.java
│   └── src/main/resources/application.yml
│
└── cupones-service/           # Consumidor
    └── src/main/java/com/tienda/cupones/
        ├── CuponesServiceApplication.java
        ├── config/KafkaConsumerConfig.java
        ├── service/CompraListener.java
        └── service/CuponService.java
    └── src/main/resources/application.yml
```

---

## Flujo de Mensajes

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                          FLUJO DE MENSAJES                                  │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────┐      ┌─────────────────┐      ┌──────────────────────┐
│  usuarios-service   │      │     Kafka       │      │   cupones-service    │
│    (Productor)      │      │   Topic:        │      │     (Consumer)       │
│                     │      │   compras       │      │                      │
│ Puerto: 8082        │      │  Particiones    │      │   Puerto: 8083       │
│                     │      │     3           │      │                      │
└─────────────────────┘      └─────────────────┘      └──────────────────────┘
        │                             │                          │
   POST /compras                      │                          │
        │                           publica                      │
        │ ──────────────────────────> │                          │
        │                             │                          │
        │                             │                     @KafkaListener
        │                             │ <─────────────────────── │
        │                             │                          │
        │                             │                    CompraListener
        │                             │                          │
        │                             │                    CuponService
        │                             │                          │
        │                             │              ┌───────────┴───────────┐
        │                             │              │                       │
        │                             │              │    SI monto > 500€    │
        │                             │              │                       │
        │                             │         Kafka topic:                 │
        │                             │     "cupones-enviados"               │
        │                             │         (Avro)                       │
        │                             │ <───────────────────────────         │
        │                             │                                      │
        │                             │                               log: "CUPON GENERADO"


┌─────────────────────────────────────────────────────────────────────────────┐
│                         ESCENARIOS DE PRUEBA                                │
└─────────────────────────────────────────────────────────────────────────────┘

Escenario 1: Compra > 500€ (CON CUPÓN)
──────────────────────────────────────────
  POST /compras {"monto": 550}
          │
          ▼
  ┌───────────────────┐
  │ usuarios-service  │ ───> Kafka topic "compras" (Avro)
  └───────────────────┘
          │
          ▼
  ┌───────────────────┐
  │ cupones-service   │ ───> @KafkaListener recibe compra
  └───────────────────┘
          │
          ▼
  ┌───────────────────────────────┐
  │ CuponService.generarCupon()   │
  │ monto (550) > 500 ✓           │ 
  └───────────────────────────────┘
          │
          ▼
  ┌───────────────────────────────────┐
  │ Kafka topic "cupones-enviados"    │
  │ Cupon {codigo, userId, ...}       │
  └───────────────────────────────────┘
          │
          ▼
  LOG: "CUPON GENERADO: userId=..., codigo=CUPON-XXXXXXXX"


Escenario 2: Compra < 500€ (SIN CUPÓN)
──────────────────────────────────────────
  POST /compras {"monto": 300}
          │
          ▼
  ┌───────────────────┐
  │ usuarios-service  │ ───> Kafka topic "compras" (Avro)
  └───────────────────┘
          │
          ▼
  ┌───────────────────┐
  │ cupones-service   │ ───> @KafkaListener recibe compra
  └───────────────────┘
          │
          ▼
  ┌───────────────────────────────┐
  │ CuponService.generarCupon()   │
  │ monto (300) > 500 ✗          │
  └───────────────────────────────┘
          │
          ▼
  LOG: "SIN CUPON: monto=300.0 <= umbral=500.0"
```

---

## Componentes usuarios-service (Productor)

### 1. CompraController.java

```java
@RestController
@RequestMapping("/compras")
public class CompraController {

    private final CompraService compraService;

    @PostMapping
    public ResponseEntity<Map<String, Object>> crearCompra(@RequestBody Compra compra) {
        // 1. Recibe POST /compras con JSON
        // 2. Llama al servicio para publicar
        // 3. Retorna "Compra publicada correctamente"
    }
}
```

**Endpoint:** `POST http://localhost:8082/compras`

### 2. CompraService.java (Productor)

```java
@Service
public class CompraService {

    private final KafkaTemplate<String, Compra> kafkaTemplate;

    @Value("${app.kafka.topic}")
    private String topic;

    public void publicarCompra(Compra compra) {
        String key = compra.getUserId().toString();  // Key para particionado

        // Envío asíncrono con callback
        CompletableFuture<SendResult<String, Compra>> future = 
            kafkaTemplate.send(topic, key, compra);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Error al publicar", ex);
            } else {
                log.info("Publicado en partición {}, offset {}",
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
            }
        });
    }
}
```

**Puntos clave:**

- `key` = userId para particionado (mensajes del mismo usuario van a la misma partición)
- Envío **asíncrono** con `whenComplete` para manejo de errores
- Serialización Avro automática

### 3. KafkaProducerConfig.java

```java
@Configuration
@EnableKafka
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.producer.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean
    public ProducerFactory<String, Compra> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Serializer Avro para el valor
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
            KafkaAvroSerializer.class);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
            StringSerializer.class);
        // Schema Registry
        props.put("schema.registry.url", schemaRegistryUrl);
        
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, Compra> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
```

---

## Componentes cupones-service (Consumidor)

### 1. CompraListener.java (Consumer)

```java
@Service
@RequiredArgsConstructor
public class CompraListener {

    private final CuponService cuponService;

    @KafkaListener(
        topics = "${app.kafka.topic}",      // "compras"
        groupId = "${spring.kafka.consumer.group-id}"  // "cupones-group"
    )
    public void escucharCompra(Compra compra) {
        log.info("Mensaje recibido: userId={}, monto={}", 
            compra.getUserId(), compra.getMonto());
        
        cuponService.generarCupon(compra);
    }
}
```

**Puntos clave:**

- `@KafkaListener` suscribe al topic `compras`
- `groupId` define el grupo de consumidores
- Todos los listeners con el mismo `groupId` comparten las particiones

### 2. CuponService.java (Lógica de negocio)

```java
@Service
public class CuponService {

    private static final double UMBRAL_MONTO = 500.0;
    private static final double DESCUENTO_DEFAULT = 50.0;

    private final KafkaTemplate<String, Cupon> kafkaTemplate;

    @Value("${app.kafka.topic-cupones}")
    private String topicCupones;

    public void generarCupon(Compra compra) {
        if (compra.getMonto() > UMBRAL_MONTO) {
            // Generar cupón
            String codigoCupon = generarCodigo();
            Instant timestamp = Instant.now();

            Cupon cupon = new Cupon();
            cupon.setCodigo(codigoCupon);
            cupon.setUserId(compra.getUserId());
            cupon.setUsername(compra.getUsername());
            cupon.setEmail(compra.getEmail());
            cupon.setMontoCompra(compra.getMonto());
            cupon.setDescuento(DESCUENTO_DEFAULT);
            cupon.setTimestamp(timestamp);

            log.info("CUPON GENERADO: userId={}, codigo={}", ...);

            // Publicar al topic cupones-enviados
            String key = compra.getUserId().toString();
            kafkaTemplate.send(topicCupones, key, cupon);
        } else {
            log.info("SIN CUPON: monto={} <= umbral={}", ...);
        }
    }
}
```

### 3. KafkaConsumerConfig.java

```java
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Value("${app.kafka.group-id}")
    private String GROUP_ID;

    @Value("${spring.kafka.bootstrap-servers}")
    private String BOOTSTRAP_SERVERS_CONFIG;

    @Bean
    public ConsumerFactory<String, Compra> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Deserializer Avro para el valor
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
            KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
            StringDeserializer.class);
        // Schema Registry
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL_CONFIG);
        props.put("specific.avro.reader", "true");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Compra>> 
        kafkaListenerContainerFactory() {
        
        ConcurrentKafkaListenerContainerFactory<String, Compra> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        
        // Manejo de errores con reintentos
        DefaultErrorHandler errorHandler = 
            new DefaultErrorHandler(new FixedBackOff(1000L, 3));
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}
```

---

## application.yml - Explicación Línea a Línea

### usuarios-service (Productor)

```yaml
server:
  port: 8082                    # Puerto donde corre el servicio

spring:
  application:
    name: usuarios-service     # Nombre de la aplicación
  kafka:
    bootstrap-servers: localhost:9092  # Broker de Kafka
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
      properties:
        schema.registry.url: http://localhost:8081  # Schema Registry

app:
  kafka:
    topic: compras            # Topic donde se publican las compras

logging:
  level:
    com.tienda: DEBUG        # Nivel de logging para nuestro código
    org.apache.kafka: INFO   # Nivel de logging para Kafka
```

**Puntos clave:**

- `key-serializer`: Serializa la key (userId) a String
- `value-serializer`: Serializa el valor (Compra) usando Avro → Schema Registry
- `bootstrap-servers`: Dirección del broker Kafka
- `schema.registry.url`: Dirección del Schema Registry

### cupones-service (Consumidor)

```yaml
server:
  port: 8083                    # Puerto donde corre el servicio

spring:
  application:
    name: cupones-service     # Nombre de la aplicación
  kafka:
    bootstrap-servers: localhost:9092  # Broker de Kafka
    consumer:
      group-id: cupones-group          # Grupo de consumidores
      auto-offset-reset: earliest     # Desde el inicio si no hay offset
      properties:
        schema.registry.url: http://localhost:8081
        specific.avro.reader: "true"    # Usar clase específica (Compra)
    producer:                        # Productor para cupones-enviados
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
      properties:
        schema.registry.url: http://localhost:8081

app:
  kafka:
    topic: compras           # Topic de consumo
    topic-cupones: cupones-enviados  # Topic de publicación de cupones
    group-id: cupones-group  # Grupo (también en spring.kafka.consumer)

logging:
  level:
    com.tienda: DEBUG
    org.apache.kafka: INFO
```

**Puntos clave:**

- `group-id`: Identifica el grupo de consumidores
- `auto-offset-reset: earliest`: Si es la primera vez, leer desde el inicio
- `specific.avro.reader: true`: Generar clase Java automáticamente desde Avro
- `producer`: Configuración para enviar cupones al nuevo topic

---

## Schemas Avro

### compra.avsc

```json
{
  "type": "record",
  "name": "Compra",
  "namespace": "com.tienda.eventos",
  "fields": [
    {"name": "userId", "type": "string"},
    {"name": "username", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "monto", "type": "double"},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
```

### cupon.avsc

```json
{
  "type": "record",
  "name": "Cupon",
  "namespace": "com.tienda.eventos",
  "fields": [
    {"name": "codigo", "type": "string"},
    {"name": "userId", "type": "string"},
    {"name": "username", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "montoCompra", "type": "double"},
    {"name": "descuento", "type": "double", "default": 50.0},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
```

---

## Comandos Kafka

### Ver contenedores Docker

```bash
docker ps
```

### Ver topics

```bash
# Desde Kafka UI: http://localhost:8080
```

### Crear topic compras

```bash
docker exec <kafka-container> kafka-topics.sh \
  --create \
  --topic compras \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### Crear topic cupones-enviados

```bash
docker exec <kafka-container> kafka-topics.sh \
  --create \
  --topic cupones-enviados \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### Listar topics

```bash
docker exec <kafka-container> kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

### Describir topic

```bash
docker exec <kafka-container> kafka-topics.sh \
  --describe \
  --topic compras \
  --bootstrap-server localhost:9092
```

### Reiniciar consumer desde el inicio

```bash
docker exec <kafka-container> kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group cupones-group \
  --reset-offsets \
  --to-earliest \
  --topic compras \
  --execute
```

---

## Comandos para probar

### Iniciar servicios

```bash
# Terminal 1
cd microservicios/usuarios-service
mvn spring-boot:run

# Terminal 2
cd microservicios/cupones-service
mvn spring-boot:run
```

### Enviar compra (mayor a 500€)

```bash
curl -X POST http://localhost:8082/compras \
  -H "Content-Type: application/json" \
  -d "{\"userId\":\"123\",\"username\":\"juan\",\"email\":\"juan@email.com\",\"monto\":550.0}"
```

### Enviar compra (menor a 500€)

```bash
curl -X POST http://localhost:8082/compras \
  -H "Content-Type: application/json" \
  -d "{\"userId\":\"456\",\"username\":\"ana\",\"email\":\"ana@email.com\",\"monto\":300.0}"
```

### Verificar health

```bash
curl http://localhost:8082/health
curl http://localhost:8083/health
```

---

## Conceptos Clave de Kafka

| Concepto | Descripción |
|---------|-------------|
| **Topic** | Canal lógico donde se publican mensajes |
| **Partición** | División de un topic para paralelismo |
| **Offset** | Índice secuencial de cada mensaje en una partición |
| **Consumer Group** | Grupo de consumidores que comparten partitions |
| **Producer** | Envía mensajes a un topic |
| **Consumer** | Lee mensajes de un topic |
| **Serialization** | Conversión objeto → bytes (Avro) |
| **Deserialization** | Conversión bytes → objeto (Avro) |
| **Schema Registry** | Registro y versioning de schemas Avro |
| **Key** | Clave para particionado (misma key → misma partición) |

---

## Puntos Clave de las Comunicaciones

### Productor → Kafka

1. **Serialización**: Objeto `Compra` → Avro → bytes
2. **Schema Registry**: Registra y versiona el schema
3. **Key**: `userId` para asegurar orden por usuario
4. **ACK**: Confirmación de escritura (configurable)

### Kafka → Consumidor

1. **Deserialización**: bytes → Avro → objeto `Compra`
2. **Consumer Group**: Comparte particiones entre instancias
3. **Offset**: Controla qué mensajes se han procesado
4. **Error Handling**: Reintentos configurables

### Productor → Kafka (Cupones)

1. **Serialización**: Objeto `Cupon` → Avro → bytes
2. **Topic**: `cupones-enviados`
3. **Key**: `userId` para asegurar orden por usuario
