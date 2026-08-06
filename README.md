# RabbitMQ JMS client with JNDI `.bindings`

This project uses **rabbitmq-jms-client** with JNDI FSContext and loads `ConnectionFactory` from a `.bindings` file.

## Configuration

Edit `config/jndi/.bindings`:

```properties
ConnectionFactory/ClassName=com.rabbitmq.jms.admin.RMQConnectionFactory
ConnectionFactory/FactoryName=com.rabbitmq.jms.admin.RMQObjectFactory

ConnectionFactory/RefAddr/0/Type=host
ConnectionFactory/RefAddr/0/Content=localhost
ConnectionFactory/RefAddr/0/String=true

ConnectionFactory/RefAddr/1/Content=5672
ConnectionFactory/RefAddr/1/Type=port
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

Required `Type` values:
- `host`
- `username`
- `password`

Optional `Type` values:
- `port` (default `5672`)
- `virtualHost` (default `/`)
- `ssl` (`true` or `false`)

## Run

Start local RabbitMQ (if not already running):

```bash
docker compose up -d
```

Then run the Java client:

```bash
mvn compile exec:java
```

What the client does:
- creates/uses queue `sample.queue`
- starts an async consumer (`MessageListener`) so it is visible in RabbitMQ UI
- sends N text messages (`sample-message-1` ... `sample-message-N`)
- consumes and prints progress while receiving messages

The code uses:
- `Context.INITIAL_CONTEXT_FACTORY = com.sun.jndi.fscontext.RefFSContextFactory`
- `Context.PROVIDER_URL = file:./config/jndi/`
- `ctx.lookup("ConnectionFactory")`

You can override provider URL at runtime:

```bash
mvn compile exec:java -Djndi.provider.url=file:/config/jndi/
```

Optional JVM properties:

```bash
mvn compile exec:java \
  -Dsample.message.count=50000 \
  -Dsample.subscription.visible.seconds=30 \
  -Dsample.wait.timeout.seconds=300
```
