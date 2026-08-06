# RabbitMQ JMS Connector with JNDI `.bindings`

A Java application demonstrating how to connect to **RabbitMQ** using the standard **Jakarta JMS API** (`rabbitmq-jms-client`) and **JNDI File System Context (`fscontext`)** configured via a `.bindings` configuration file.

---

## 📌 Features

- **JNDI FSContext Integration**: Resolves `ConnectionFactory` dynamically from a `.bindings` file using `com.sun.jndi.fscontext.RefFSContextFactory`.
- **Jakarta JMS Support**: Uses `jakarta.jms` API with `RMQConnectionFactory`.
- **Configurable Producer & Consumer**: Publishes persistent text messages to a queue and consumes them asynchronously using a `MessageListener`.
- **Interactive Monitoring Delay**: Configurable pause between publishing and consuming to inspect queue depth and message metrics in the RabbitMQ Management UI.
- **Docker Ready**: Includes `docker-compose.yml` for spinning up a local RabbitMQ broker with Management Console enabled.

---

## 🛠️ Project Structure

```
├── config/
│   └── jndi/
│       ├── .bindings          # Active JNDI binding properties
│       └── .bindings.example  # Reference template for binding configuration
├── src/
│   └── main/
│       └── java/
│           └── com/example/rabbitmq/
│               └── RabbitMqConnector.java  # Main application logic
├── docker-compose.yml         # Local RabbitMQ service definition
├── pom.xml                    # Maven configuration (Java 17)
└── README.md                  # Project documentation
```

---

## ⚙️ Configuration (`.bindings`)

The application loads connection credentials and settings from `config/jndi/.bindings`.

### Example Configuration

```properties
ConnectionFactory/ClassName=com.rabbitmq.jms.admin.RMQConnectionFactory
ConnectionFactory/FactoryName=com.rabbitmq.jms.admin.RMQObjectFactory

ConnectionFactory/RefAddr/0/Type=host
ConnectionFactory/RefAddr/0/Content=localhost
ConnectionFactory/RefAddr/0/String=true

ConnectionFactory/RefAddr/1/Type=port
ConnectionFactory/RefAddr/1/Content=5672
ConnectionFactory/RefAddr/1/String=true

ConnectionFactory/RefAddr/2/Type=virtualHost
ConnectionFactory/RefAddr/2/Content=/
ConnectionFactory/RefAddr/2/String=true

ConnectionFactory/RefAddr/3/Type=username
ConnectionFactory/RefAddr/3/Content=guest
ConnectionFactory/RefAddr/3/String=true

ConnectionFactory/RefAddr/4/Type=password
ConnectionFactory/RefAddr/4/Content=guest
ConnectionFactory/RefAddr/4/String=true
```

### Supported Properties

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `host` | String | **Yes** | — | RabbitMQ broker hostname or IP |
| `username` | String | **Yes** | — | Authentication username |
| `password` | String | **Yes** | — | Authentication password |
| `port` | Integer | No | `5672` | AMQP port |
| `virtualHost` | String | No | `/` | Target vhost |
| `ssl` | Boolean | No | `false` | Enable TLS/SSL connection (`true` or `false`) |

---

## 🚀 Quick Start

### 1. Requirements

- **Java JDK**: 17 or higher
- **Apache Maven**: 3.8+
- **Docker / Docker Compose**: Optional (for running RabbitMQ locally)

### 2. Start RabbitMQ Broker

Launch a local RabbitMQ server with the Management Plugin using Docker Compose:

```bash
docker compose up -d
```

- **AMQP Broker Port**: `5672`
- **Management Web UI**: [http://localhost:15672](http://localhost:15672) (Credentials: `guest` / `guest`)

### 3. Run the Application

Compile and run the client via Maven:

```bash
mvn compile exec:java
```

---

## ⚙️ JVM System Properties

You can customize the application behavior at runtime using system properties (`-Dkey=value`):

```bash
mvn compile exec:java \
  -Djndi.provider.url=file:./config/jndi/ \
  -Dsample.message.count=100 \
  -Dsample.subscription.visible.seconds=10 \
  -Dsample.wait.timeout.seconds=60
```

| System Property | Default | Description |
|---|---|---|
| `jndi.provider.url` | `file:./config/jndi/` | Path to the directory containing `.bindings` |
| `sample.message.count` | `5` | Total number of messages to produce |
| `sample.subscription.visible.seconds` | `20` | Pause duration (seconds) before consuming messages |
| `sample.wait.timeout.seconds` | `120` | Timeout (seconds) for receiving all expected messages |

---

## 🔄 Execution Flow

1. **JNDI Initialization**: Loads properties from `config/jndi/.bindings`, constructs a JNDI `Reference`, binds it to `RefFSContextFactory`, and retrieves the JMS `ConnectionFactory`.
2. **Publishing**: Creates persistent text messages (`sample-message-1` ... `sample-message-N`) and posts them to `sample.queue`.
3. **Observation Window**: Pauses execution for `sample.subscription.visible.seconds` so you can verify queue contents in the [RabbitMQ Management Console](http://localhost:15672/#/queues).
4. **Consumption**: Attaches an asynchronous `MessageListener` to receive, process, and acknowledge all published messages.
5. **Clean Shutdown**: Closes JMS connection and JNDI context gracefully.
