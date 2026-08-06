package com.example.rabbitmq;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.DeliveryMode;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.Reference;
import javax.naming.StringRefAddr;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class RabbitMqConnector {
    private static final String LOGICAL_NAME = "ConnectionFactory";
    private static final Path BINDINGS_FILE = Path.of("config/jndi/.bindings");
    private static final String SAMPLE_QUEUE = "sample.queue";
    private static final int SAMPLE_MESSAGE_COUNT = Integer.getInteger("sample.message.count", 5);
    private static final int WAIT_TIMEOUT_SECONDS = Integer.getInteger("sample.wait.timeout.seconds", 120);
    private static final int SUBSCRIPTION_VISIBLE_SECONDS =
            Integer.getInteger("sample.subscription.visible.seconds", 20);
    private static final Pattern REF_TYPE_PATTERN =
            Pattern.compile("^" + LOGICAL_NAME + "/RefAddr/(\\d+)/Type$");

    private RabbitMqConnector() {
    }

    public static void main(String[] args) {
        Context ctx = null;
        Connection connection = null;
        try {
            Properties bindings = loadBindings(BINDINGS_FILE);
            Reference reference = buildReference(bindings);

            Hashtable<String, String> env = new Hashtable<>();
            env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.fscontext.RefFSContextFactory");
            env.put(Context.PROVIDER_URL, System.getProperty("jndi.provider.url", "file:./config/jndi/"));

            ctx = new InitialContext(env);
            ctx.rebind(LOGICAL_NAME, reference);
            ConnectionFactory connectionFactory = (ConnectionFactory) ctx.lookup(LOGICAL_NAME);

            connection = connectionFactory.createConnection();
            connection.start();
            runQueueSample(connection);
            System.out.println("Connected using rabbitmq-jms via JNDI lookup 'ConnectionFactory'");
        } catch (Exception e) {
            System.err.println("Failed to connect to RabbitMQ: " + e.getMessage());
            System.exit(1);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception closeError) {
                    System.err.println("Failed to close JMS connection: " + closeError.getMessage());
                }
            }
            if (ctx != null) {
                try {
                    ctx.close();
                } catch (Exception closeError) {
                    System.err.println("Failed to close JNDI context: " + closeError.getMessage());
                }
            }
        }
    }

    private static void runQueueSample(Connection connection) throws Exception {
        try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            Queue queue = session.createQueue(SAMPLE_QUEUE);

            // 1. Publish all messages first
            System.out.println("Publishing " + SAMPLE_MESSAGE_COUNT + " messages to '" + SAMPLE_QUEUE + "'...");
            try (MessageProducer producer = session.createProducer(queue)) {
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);
                for (int i = 1; i <= SAMPLE_MESSAGE_COUNT; i++) {
                    String payload = "sample-message-" + i;
                    TextMessage message = session.createTextMessage(payload);
                    producer.send(message);
                    if (i <= 20 || i % 1000 == 0 || i == SAMPLE_MESSAGE_COUNT) {
                        System.out.println("Sent: " + payload + " (" + i + "/" + SAMPLE_MESSAGE_COUNT + ")");
                    }
                }
            }
            System.out.println("All " + SAMPLE_MESSAGE_COUNT + " messages published.");

            if (SUBSCRIPTION_VISIBLE_SECONDS > 0) {
                System.out.println(
                        "Waiting " + SUBSCRIPTION_VISIBLE_SECONDS
                                + "s before consuming (messages visible in RabbitMQ UI)...");
                Thread.sleep(SUBSCRIPTION_VISIBLE_SECONDS * 1000L);
            }

            // 2. Read all published messages
            CountDownLatch latch = new CountDownLatch(SAMPLE_MESSAGE_COUNT);
            AtomicInteger receivedCounter = new AtomicInteger();
            AtomicReference<Throwable> listenerError = new AtomicReference<>();

            System.out.println("Starting consumer to read messages from '" + SAMPLE_QUEUE + "'...");
            try (MessageConsumer consumer = session.createConsumer(queue)) {
                consumer.setMessageListener(message -> {
                    try {
                        if (!(message instanceof TextMessage textMessage)) {
                            throw new IllegalStateException("Unexpected message type: " + message.getClass().getName());
                        }

                        int received = receivedCounter.incrementAndGet();
                        if (received <= 20 || received % 1000 == 0 || received == SAMPLE_MESSAGE_COUNT) {
                            System.out.println(
                                    "Received: " + textMessage.getText() + " (" + received + "/" + SAMPLE_MESSAGE_COUNT + ")");
                        }
                        latch.countDown();
                    } catch (Throwable t) {
                        listenerError.compareAndSet(null, t);
                    }
                });

                boolean allReceived = latch.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (listenerError.get() != null) {
                    throw new RuntimeException("Message listener failed", listenerError.get());
                }
                if (!allReceived) {
                    throw new IllegalStateException(
                            "Timed out waiting for messages. Received "
                                    + receivedCounter.get() + "/" + SAMPLE_MESSAGE_COUNT);
                }
            }
        }
    }

    private static Properties loadBindings(Path path) throws IOException {
        if (!Files.exists(path)) {
            throw new IOException("Missing bindings file: " + path);
        }

        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(path)) {
            properties.load(reader);
        }
        return properties;
    }

    private static Reference buildReference(Properties bindings) {
        String className = required(bindings, LOGICAL_NAME + "/ClassName");
        String factoryName = required(bindings, LOGICAL_NAME + "/FactoryName");
        Reference reference = new Reference(className, factoryName, null);

        List<Integer> indexes = new ArrayList<>();
        for (String key : bindings.stringPropertyNames()) {
            Matcher matcher = REF_TYPE_PATTERN.matcher(key);
            if (matcher.matches()) {
                indexes.add(Integer.parseInt(matcher.group(1)));
            }
        }
        indexes.sort(Integer::compareTo);

        for (int index : indexes) {
            String type = required(bindings, LOGICAL_NAME + "/RefAddr/" + index + "/Type");
            String content = required(bindings, LOGICAL_NAME + "/RefAddr/" + index + "/Content");
            reference.add(new StringRefAddr(type, content));
        }
        return reference;
    }

    private static String required(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required .bindings key: " + key);
        }
        return value;
    }
}
