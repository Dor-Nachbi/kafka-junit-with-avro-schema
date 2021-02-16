package kafka;

import com.example.Customer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import serialize.AvroToBytesSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class MyProducerWithAvro {

    Producer<String, Customer> producer;

    public MyProducerWithAvro() {
        producer = new KafkaProducer<>(getProperties());
    }

    public Properties getProperties() {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "10");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroToBytesSerializer.class.getName());
        return properties;
    }

    public Future<RecordMetadata> send(String topic, String key, Customer customer) {
        ProducerRecord<String, Customer> record =
                new ProducerRecord<String, Customer>(topic,key, customer);
        // send data - asynchronous
        Future<RecordMetadata> metadata=producer.send(record);
        // flush data
        producer.flush();
        // flush and close producer
        producer.close();
        return metadata;
    }
}
