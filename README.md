# Kafka Formación - Tienda Virtual

Proyecto de formación sobre mensajería con Apache Kafka mediante una tienda virtual simplificada.

## Arquitectura

```
kafka-formacion/
├── docker-compose.yml          # Kafka + Schema Registry + UI
├── microservices/
│   ├── usuarios-service/       # Productor: usuarios + compras
│   └── cupones-service/        # Consumer: cupones descuento
├── schemas/                    # Schemas Avro
└── Documentacion/              # Plan de ejecución
```

## Tech Stack

| Tecnología | Versión | Uso |
|------------|---------|-----|
| **Java** | 17 | Lenguaje de los microservicios |
| **Spring Boot** | 3.x | Framework de los microservicios |
| **Apache Kafka** | 7.5.0 | Broker de mensajes |
| **KRaft** | - | Metadata (sin Zookeeper) |
| **Schema Registry** | 7.5.0 | Registro de esquemas Avro |
| **Avro** | - | Serialización de mensajes |
| **Docker** | - | Contenedores |
| **Docker Compose** | - | Orquestación de servicios |

## Conceptos Kafka Practicados

- Topics y particiones
- Productores y consumidores
- Consumer groups
- Offsets y commit
- Schema Registry + Avro
- Keys y particionado

## Cómo empezar

1. Arrancar servicios Kafka:
```bash
docker-compose up -d
```

2. Acceder a Kafka UI: http://localhost:8080

3. Ver Schema Registry: http://localhost:8081