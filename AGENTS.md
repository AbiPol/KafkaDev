# AGENTS.md - Proyecto Formación Kafka

> **Nota para agentes**: Al comenzar cada sesión, revisar la sección 6 del documento `Documentacion/PLAN_EJECUCION.md` para conocer la fase actual de desarrollo del proyecto.

Este archivo proporciona directrices para agentes de codificación que operan en este repositorio.

---

## Visión General del Proyecto

Proyecto de formación Kafka que implementa una tienda virtual con:

- **usuarios-service** (Productor): Gestiona usuarios, publica eventos de compra
- **cupones-service** (Consumidor): Consume compras, genera cupones de descuento para pedidos > 500€

## Stack Tecnológico

| Componente      | Versión |
| --------------- | ------- |
| Java            | 17      |
| Spring Boot     | 3.x     |
| Apache Kafka    | 3.8.0   |
| Schema Registry | 7.5.0   |
| Avro            | -       |
| Maven           | -       |

---

## Comandos de Build y Ejecución

### Docker Compose (Infraestructura Kafka)

```bash
# Iniciar todos los servicios de infraestructura
docker-compose up -d

# Detener todos los servicios
docker-compose down

# Ver logs
docker-compose logs -f

# Reiniciar un servicio específico
docker-compose restart kafka
```

### Temas de Kafka

```bash
# Crear tema con particiones
docker exec -it kafka kafka-topics.sh --create --topic compras --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Listar temas
docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092

# Describir tema
docker exec -it kafka kafka-topics.sh --describe --topic compras --bootstrap-server localhost:9092
```

### Puntos de Acceso

| Servicio        | URL                     |
| --------------- | ----------------------- |
| Kafka UI        | <http://localhost:8080> |
| Schema Registry | <http://localhost:8081> |

---

## Directrices de Estilo de Código

### Principios Generales

- **Simplicidad**: Favorecer la legibilidad sobre soluciones ingeniosas
- **Responsabilidad única**: Cada clase/método hace una sola cosa
- **Fallar rápido**: Validar entradas temprano, lanzar excepciones significativas

### Convenciones Java

#### Convenciones de Nombres

| Elemento            | Convención            | Ejemplo               |
| ------------------- | --------------------- | --------------------- |
| Clases              | PascalCase            | `UsuarioService`      |
| Métodos             | camelCase             | `buscarUsuario`       |
| Variables           | camelCase             | `nombreUsuario`       |
| Constantes          | MAYUSCULAS_SNAKE_CASE | `MAX_REINTENTOS`      |
| Paquetes            | minúsculas            | `com.tienda.usuarios` |
| Constantes en enums | MAYUSCULAS_SNAKE_CASE | `ACTIVO`              |

#### Organización de Archivos

```text
src/
├── main/
│   ├── java/com/tienda/<servicio>/
│   │   ├── config/
│   │   ├── controller/
│   │   ├── service/
│   │   ├── model/
│   │   ├── repository/
│   │   └── <servicio>Application.java
│   └── resources/
│       └── application.yml
└── test/
    └── java/...
```

#### Imports

- Usar nombres completos para java.lang.* (opcional)
- Orden: static, java, javax, libs externas, internas
- Evitar imports con asterisco excepto para constantes estáticas
- Agrupar por paquete, separar con línea en blanco

```java
import static org.junit.Assert.*;
import java.util.List;
import org.springframework.kafka.annotation.KafkaListener;
import com.tienda.usuarios.model.Compra;
```

#### Formateo

- Usar 4 espacios para indentación (sin tabs)
- Longitud máxima de línea: 120 caracteres
- Llave de apertura en la misma línea
- Una línea en blanco entre elementos de primer nivel
- Sin espacios en blanco al final

#### Tipos

- Usar interfaces para declaraciones cuando sea posible: `List<String>` sobre `ArrayList<String>`
- Preferir tipos inmutables cuando sea práctico
- Usar Optional para retornos que pueden ser nulos
- Evitar tipos crudos con genéricos

#### Manejo de Errores

- Usar tipos de excepciones específicas (no Exception/RuntimeException genéricas)
- Incluir mensajes significativos en las excepciones
- Registrar excepciones antes de relanzar
- Usar excepciones personalizadas para errores de negocio
- Nunca tragar excepciones silenciosamente

```java
// Correcto
public Usuario buscarOError(Long id) {
    return usuarioRepository.findById(id)
        .orElseThrow(() -> new UsuarioNoEncontradoException(
            "Usuario no encontrado: " + id));
}

// Incorrecto - atrapa todo
try {
    // código
} catch (Exception e) {
    // tragar silenciosamente
}
```

---

## Directrices Específicas de Kafka

### Productores

- Usar envío asíncrono con callbacks
- Siempre incluir key para particionado ordenado
- Configurar `acks=all` para eventos críticos
- Manejar excepciones reintentables por separado

```java
CompletableFuture<SendResult<String, Compra>> future = producer.send(topic, key, evento);
future.whenComplete((result, ex) -> {
    if (ex != null) {
        log.error("Error al enviar", ex);
    } else {
        log.info("Enviado a partición {}", result.getRecordMetadata().partition());
    }
});
```

### Consumidores

- Usar grupos de consumidores para escalado
- Confirmar offsets confiablemente (enable.auto.commit=false para manual)
- Implementar procesamiento idempotente
- Manejar errores de deserialización con gracia

```java
@KafkaListener(topics = "compras", groupId = "cupones-group")
public void procesarCompra(ConsumerRecord<String, Compra> registro) {
    // Procesar con verificaciones de idempotencia
}
```

### Schemas Avro

- Registrar schemas en Schema Registry
- Usar compatibilidad hacia atrás (AVRO)
- Nunca eliminar campos requeridos
- Agregar valores por defecto para nuevos campos opcionales

---

## Directrices de Pruebas

### Pruebas Unitarias

- Probar una cosa por prueba
- Usar nombres descriptivos: `debeRetornarUsuario_CuandoIdExiste()`
- Estructura Given-When-Then
- Mockear dependencias externas

### Pruebas de Integración

- Usar `@EmbeddedKafka` para pruebas de Kafka
- Limpiar estado entre pruebas
- Usar temas/particiones de prueba

### Ejecutar Pruebas

```bash
# Ejecutar una clase de prueba
mvn test -Dtest=UsuarioServiceTest

# Ejecutar un método de prueba
mvn test -Dtest=UsuarioServiceTest#deveBuscarUsuario

# Ejecutar todas las pruebas
mvn test

# Ejecutar con cobertura
mvn test jacoco:report
```

---

## Convenciones Git

### Mensajes de Commit

```text
<tipo>(<alcance>): <descripción>

Tipos: feat, fix, docs, style, refactor, test, chore
```

### Nombres de Ramas

```text
feature/usuarios-crud
fix/correccion-offsets
hotfix/cupon-duplicado
```

---

## Referencias

- [Spring for Apache Kafka](https://docs.spring.io/spring-kafka/reference/)
- [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serialize-deserialize.html)
- [Avro Specification](https://avro.apache.org/docs/current/spec.html)
- [Documentacion/PLAN_EJECUCION.md](Documentacion/PLAN_EJECUCION.md)
