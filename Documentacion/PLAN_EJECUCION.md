# Plan de Ejecución: Proyecto Kafka para Formación

## 1. Estructura del Proyecto

```
kafka-formacion/
├── docker-compose.yml          # Kafka + Schema Registry + UI
├── microservices/
│   ├── usuarios-service/       # Productor: usuarios + compras
│   └── cupones-service/        # Consumer: envía cupones
├── schemas/                    # Avro schemas
└── Documentacion/              # Este archivo
```

---

## 2. Componentes a Instalar (Docker Compose)

| Componente | Propósito | Puerto | Estado |
|------------|-----------|--------|--------|
| **Kafka** | Broker de mensajes | 9092 | ✅ |
| **Kafka KRaft** | Metadata (sin Zookeeper) | - | ✅ |
| **Schema Registry** | Registro de esquemas Avro | 8081 | ✅ |
| **Kafka UI** | Visualización | 8080 | ✅ |

---

## 3. Microservicio 1: usuarios-service (Productor)

**Responsabilidades:**

- Gestionar usuarios (CRUD)
- Registrar compras
- Publicar eventos `compra-realizada` a Kafka

**Eventos a producir:**

```json
{
  "userId": "123",
  "username": "juan",
  "email": "juan@email.com",
  "monto": 550.00,
  "timestamp": "2026-04-09T10:30:00Z"
}
```

---

## 4. Microservicio 2: cupones-service (Consumer)

**Responsabilidades:**

- Consumir topic `compras`
- Detectar compras > 500€
- Generar y "enviar" cupones (log simulado)

**Lógica:**

- Grupo de consumidores
- Commit automático de offsets
- Idempotencia (evitar duplicados)

---

## 5. Conceptos Kafka a Practicar

| Concepto | Dónde Practicarlo | Estado |
|----------|-------------------|--------|
| **Topics** | `compras`, `cupones-enviados` | ✅ |
| **Particiones** | 3 particiones para paralelismo | ✅ |
| **Offsets** | Consumer groups, resetear offsets | ⬜ |
| **Productor** | Envío asíncrono, acknowledgment | ⬜ |
| **Consumer** | Grupo de consumidores | ⬜ |
| **Avro + Schema Registry** | Serialización/deserialización | ⬜ |
| **Keys** | Particionado por userId | ⬜ |

---

## 6. Plan de Ejecución Paso a Paso

### Fase 1: Infraestructura (Docker)

- [x] 1.1 Crear `docker-compose.yml` con Kafka + Schema Registry
- [x] 1.2 Arrancar servicios y verificar conectividad
- [x] 1.3 Crear topics con particiones

### Fase 2: Schema Registry

- [ ] 2.1 Definir schema Avro para evento de compra
- [ ] 2.2 Registrar schema en Schema Registry
- [ ] 2.3 Validar compatibilidad (AVRO)

### Fase 3: usuarios-service (Productor)

- [ ] 3.1 Crear proyecto Spring Boot
- [ ] 3.2 Configurar cliente Kafka con Avro
- [ ] 3.3 Implementar endpoint `/compras`
- [ ] 3.4 Publicar a topic `compras`

### Fase 4: cupones-service (Consumer)

- [ ] 4.1 Crear proyecto Spring Boot
- [ ] 4.2 Configurar consumer con Avro
- [ ] 4.3 Implementar listener para compras > 500€
- [ ] 4.4 Generar cupones (log)

### Fase 5: Testing y Observación

- [ ] 5.1 Enviar compras < 500€ (sin cupón)
- [ ] 5.2 Enviar compras > 500€ (con cupón)
- [ ] 5.3 Usar Kafka UI para ver mensajes/particiones
- [ ] 5.4 Resetear offsets y re-procesar

---

## 7. Recomendaciones Adicionales para Formación

> **Nota:** Las siguientes opciones las añadiremos conforme las vayas aprendiendo.

| Añadir | Beneficio | Estado |
|--------|-----------|--------|
| **Monitorización** | Prometheus + Grafana (pendiente de aprender) | ⬜ |
| **Retry/Error handling** | Manejo de errores y reintentos | ⬜ |
| **Dead Letter Queue** | Cola para mensajes que fallan | ⬜ |
| **Logs estructurados** | JSON logging | ⬜ |
| **Tests unitarios** | `@EmbeddedKafka` | ⬜ |
| **Postman collection** | Para probar endpoints | ⬜ |

---

### 📚 Glosario para Formación

- **Retry/Error handling:** Si un mensaje falla al procesarse, el sistema intenta reenviarlo automáticamente varias veces antes de darlo por perdido.

- **Dead Letter Queue (DLQ):** Es una cola "basura" donde Van los mensajes que no se pudieron procesar después de todos los reintentos. Permite analizarlos después sin perderlos.

- **Idempotencia:** Garantizar que procesar el mismo mensaje varias veces no cause efectos secundarios (ej: no enviar 2 cupones por el mismo mensaje duplicado).

---

## 8. Tech Stack Final

- **Lenguaje:** Java 17 + Spring Boot 3.x
- **Kafka:** Confluent Platform (Docker)
- **Serialización:** Avro + Schema Registry
- **Build:** Maven
- **Contenedores:** Docker Compose
