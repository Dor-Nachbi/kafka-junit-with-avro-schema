package kafka;

import com.google.common.collect.Maps;

import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

public class EphemeralKafkaBroker {
    private static final Logger LOGGER = LoggerFactory.getLogger(EphemeralKafkaBroker.class);
    private static final int ALLOCATE_RANDOM_PORT = -1;
    private static final String LOCALHOST = "localhost";

    private int kafkaPort;
    private int zookeeperPort;
    private Properties overrideBrokerProperties;

    private TestingServer zookeeper;
    private KafkaServerStartable kafkaServer;
    private Path kafkaLogDir;

    private volatile boolean brokerStarted = false;

    public static EphemeralKafkaBroker create(int kafkaPort, int zookeeperPort) {
        return new EphemeralKafkaBroker(kafkaPort, zookeeperPort, null);
    }

    EphemeralKafkaBroker(int kafkaPort, int zookeeperPort, Properties overrideBrokerProperties) {
        this.kafkaPort = kafkaPort;
        this.zookeeperPort = zookeeperPort;
        this.overrideBrokerProperties = overrideBrokerProperties;
    }

    public CompletableFuture<Void> start() throws Exception {
        if (!brokerStarted) {
            synchronized (this) {
                if (!brokerStarted) {
                    return startBroker();
                }
            }
        }

        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> startBroker() throws Exception {
        if (zookeeperPort == ALLOCATE_RANDOM_PORT) {
            zookeeper = new TestingServer(true);
            zookeeperPort = zookeeper.getPort();
        } else {
            zookeeper = new TestingServer(zookeeperPort, true);
        }

        kafkaPort = kafkaPort == ALLOCATE_RANDOM_PORT ? InstanceSpec.getRandomPort() : kafkaPort;
        String zookeeperConnectionString = zookeeper.getConnectString();
        KafkaConfig kafkaConfig = buildKafkaConfig(zookeeperConnectionString);

        LOGGER.info("Starting Kafka server with config: {}", kafkaConfig.props());
        kafkaServer = new KafkaServerStartable(kafkaConfig);
        brokerStarted = true;
        return CompletableFuture.runAsync(() -> kafkaServer.startup());
    }

    public void stop() {
        if (brokerStarted) {
            synchronized (this) {
                if (brokerStarted) {
                    stopBroker();
                    brokerStarted = false;
                }
            }
        }
    }

    private void stopBroker() {
        try {
            if (kafkaServer != null) {
                LOGGER.info("Shutting down Kafka Server");
                kafkaServer.shutdown();
            }

            if (zookeeper != null) {
                LOGGER.info("Shutting down Zookeeper");
                zookeeper.close();
            }

            if (Files.exists(kafkaLogDir)) {
                LOGGER.info("Deleting the log dir:  {}", kafkaLogDir);
                Files.walkFileTree(kafkaLogDir, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        Files.deleteIfExists(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        Files.deleteIfExists(dir);
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
        } catch (Exception e) {
            LOGGER.error("Failed to clean-up Kafka", e);
        }
    }

    private KafkaConfig buildKafkaConfig(String zookeeperQuorum) throws IOException {
        kafkaLogDir = Files.createTempDirectory("kafka_junit");

        Properties props = new Properties();
        props.put("advertised.listeners", "PLAINTEXT://" + LOCALHOST + ":" + kafkaPort);
        props.put("listeners", "PLAINTEXT://0.0.0.0:" + kafkaPort);
        props.put("port", kafkaPort + "");
        props.put("broker.id", "1");
        props.put("log.dirs", kafkaLogDir.toAbsolutePath().toString());
        props.put("zookeeper.connect", zookeeperQuorum);
        props.put("leader.imbalance.check.interval.seconds", "1");
        props.put("offsets.topic.num.partitions", "1");
        props.put("offsets.topic.replication.factor", "1");
        props.put("default.replication.factor", "1");
        props.put("num.partitions", "3");
        props.put("group.min.session.timeout.ms", "100");

        if (overrideBrokerProperties != null) {
            props.putAll(overrideBrokerProperties);
        }

        return new KafkaConfig(props);
    }

    public Properties producerConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", LOCALHOST + ":" + kafkaPort);
        props.put("acks", "1");
        props.put("batch.size", "10");
        props.put("client.id", "kafka-junit");
        props.put("request.timeout.ms", "500");

        return props;
    }

    public Properties consumerConfig() {
        return consumerConfig(true);
    }

    public Properties consumerConfig(boolean enableAutoCommit) {
        Properties props = new Properties();
        props.put("bootstrap.servers", LOCALHOST + ":" + kafkaPort);
        props.put("group.id", "kafka-junit-consumer");
        props.put("enable.auto.commit", String.valueOf(enableAutoCommit));
        props.put("auto.commit.interval.ms", "10");
        props.put("auto.offset.reset", "earliest");
        props.put("heartbeat.interval.ms", "100");
        props.put("session.timeout.ms", "200");
        props.put("fetch.max.wait.ms", "200");
        props.put("metadata.max.age.ms", "100");

        return props;
    }

    public <K, V> KafkaProducer<K, V> createProducer(Serializer<K> keySerializer, Serializer<V> valueSerializer,
                                                     Properties overrideConfig) {
        Properties conf = producerConfig();
        if (overrideConfig != null) {
            conf.putAll(overrideConfig);
        }
        keySerializer.configure(Maps.fromProperties(conf), true);
        valueSerializer.configure(Maps.fromProperties(conf), false);
        return new KafkaProducer<>(conf, keySerializer, valueSerializer);
    }

    public <K, V> KafkaConsumer<K, V> createConsumer(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer,
                                                     Properties overrideConfig) {
        Properties conf = consumerConfig();
        if (overrideConfig != null) {
            conf.putAll(overrideConfig);
        }
        keyDeserializer.configure(Maps.fromProperties(conf), true);
        valueDeserializer.configure(Maps.fromProperties(conf), false);
        return new KafkaConsumer<>(conf, keyDeserializer, valueDeserializer);
    }

    public boolean isRunning() {
        return brokerStarted;
    }
}
