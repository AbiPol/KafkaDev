# 🚀 Kafka Formación - Tienda Virtual

Proyecto de formación avanzado sobre mensajería con **Apache Kafka** implementando el flujo de una tienda virtual distribuida mediante microservicios.

---

## 🏗️ Arquitectura del Sistema

El proyecto sigue un patrón de arquitectura dirigida por eventos (EDA):

```text
kafka-formacion/
├── docker-compose.yml          # Infraestructura (Kafka, Monitoring, etc.)
├── microservices/
│   ├── usuarios-service/       # Productor: Gestión de usuarios y compras (Puerto 8082)
│   └── cupones-service/        # Consumer: Lógica de cupones descuento (Puerto 8083)
├── schemas/                    # Contratos de datos (Avro Schemas)
├── prometheus/                 # Configuración de recolección de métricas
├── grafana/                    # Provisioning de dashboards y data sources
└── Documentacion/              # Guías detalladas y plan de ejecución
```

---

## 🛠️ Tech Stack

| Componente          | Tecnología       | Versión | Propósito                          |
| :------------------ | :--------------- | :------ | :----------------------------------|
| **Core**            | Java             | 17      | Lenguaje base                      |
| **Framework**       | Spring Boot      | 3.2.x   | Desarrollo de microservicios       |
| **Messaging**       | Apache Kafka     | 3.8.0   | Broker de mensajería (KRaft mode)  |
| **Registry**        | Schema Registry  | 7.5.0   | Gestión de esquemas Avro           |
| **Monitoring**      | Prometheus       | Latest  | Recolección de métricas (Actuator) |
| **Visualization**   | Grafana          | Latest  | Dashboards de observabilidad       |
| **Containerization**| Docker           | -       | Despliegue de infraestructura      |

---

## 🚀 Guía de Inicio Rápido

### 1. Iniciar Infraestructura

Desde la raíz del proyecto, levanta todos los contenedores necesarios:

```bash
docker-compose up -d
```

### 2. Ejecutar Microservicios

Arranca los proyectos desde tu IDE preferido (IntelliJ/Eclipse) o mediante línea de comandos:

* **Usuarios Service**: `cd microservices/usuarios-service && mvn spring-boot:run`
* **Cupones Service**: `cd microservices/cupones-service && mvn spring-boot:run`

---

## 📊 Monitorización y Observabilidad

Hemos implementado un stack de observabilidad completo basado en **Spring Boot Actuator + Micrometer + Prometheus + Grafana**.

### Cómo visualizar métricas

1. Accede a **Grafana** ([http://localhost:3000](http://localhost:3000)).
2. Credenciales por defecto: `admin` / `admin`.
3. **Importar Dashboard**:
    * Ve a `Dashboards` -> `New` -> `Import`.
    * Introduce el ID **`4701`** (JVM Micrometer) y haz clic en `Load`.
    * Selecciona el data source `Prometheus` (ya pre-configurado).
    * Haz clic en `Import`.

> [!NOTE]
> El archivo `grafana/provisioning/datasources/datasource.yml` configura automáticamente la conexión entre Grafana y Prometheus.

---

## 🔗 Enlaces Útiles

| Herramienta          | URL Local                                                        | Descripción                                  |
| :------------------- | :--------------------------------------------------------------- | :------------------------------------------- |
| **Kafka UI**         | [http://localhost:8080](http://localhost:8080)                   | Gestión y visualización de Topics/Mensajes   |
| **Grafana**          | [http://localhost:3000](http://localhost:3000)                   | Dashboards y visualización de métricas       |
| **Prometheus**       | [http://localhost:9090](http://localhost:9090)                   | Servidor de métricas y consultas PromQL      |
| **Schema Registry**  | [http://localhost:8081](http://localhost:8081)                   | Repositorio de esquemas Avro                 |
| **Actuator Usuarios**| [http://localhost:8082/actuator](http://localhost:8082/actuator) | Endpoint de monitorización de Usuarios       |
| **Actuator Cupones** | [http://localhost:8083/actuator](http://localhost:8083/actuator) | Endpoint de monitorización de Cupones        |

---

## 🧪 Conceptos Kafka Practicados

* **Topics y Particiones**: Paralelismo y escalabilidad.
* **Avro & Schema Registry**: Contratos de datos robustos y compatibilidad.
* **Consumer Groups**: Reparto de carga entre instancias.
* **Observabilidad**: Monitorización en tiempo real del rendimiento de la JVM.
