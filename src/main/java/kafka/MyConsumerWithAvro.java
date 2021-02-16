package kafka;

import com.example.Customer;

import deserialize.AvroCustomerDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class MyConsumerWithAvro {

    KafkaConsumer<String, Customer> consumer;

    public MyConsumerWithAvro(String groupId) {
        consumer = new KafkaConsumer<>(getProperties(groupId));
    }

    public Properties getProperties(String groupId) {
        String bootstrapServers = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //properties.put("auto.commit.enable", "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // avro part (deserializer)
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroCustomerDeserializer.class.getName());
        //properties.setProperty("specific.avro.reader", "true");
        return properties;
    }

    public void subscribe(String topic) {
        consumer.subscribe(Arrays.asList(topic));
    }

    public ConsumerRecords<String, Customer> poll(int duration) {
        ConsumerRecords<String, Customer> records =
                consumer.poll(Duration.ofMillis(duration));
        return records;
    }
}
