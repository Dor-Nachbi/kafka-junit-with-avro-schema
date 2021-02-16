import com.example.Customer;
import com.google.common.collect.Lists;
import deserialize.AvroCustomerDeserializer;
import kafka.EphemeralKafkaBroker;
import kafka.MyConsumerWithAvro;
import kafka.MyProducerWithAvro;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class ProducerAndConsumerWithAvroTest {

    public static final String TEST_TOPIC = "test-topic";

    @Test
    public void testProducerWithAvro() throws Exception {
        final EphemeralKafkaBroker broker = EphemeralKafkaBroker.create(9092, 2181);
        CompletableFuture<Void> res = broker.start();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            //Ignore
        }

        assertThat(broker.isRunning()).isTrue();

        MyProducerWithAvro avroProducer = new MyProducerWithAvro();
        Customer customer = Customer.newBuilder()
                .setAge(34)
                .setAutomatedEmail(false)
                .setFirstName("John")
                .setLastName("Doe")
                .setHeight(178f)
                .setWeight(75f)
                .build();

        Future<RecordMetadata> result = avroProducer.send(TEST_TOPIC, "key", customer);
        RecordMetadata metadata = result.get(500L, TimeUnit.MILLISECONDS);
        assertThat(metadata).isNotNull();
        assertThat(metadata.topic()).isEqualTo(TEST_TOPIC);

        try (KafkaConsumer<String, Customer> consumer =
                     broker.createConsumer(new StringDeserializer(), new AvroCustomerDeserializer(), null)) {

            consumer.subscribe(Lists.newArrayList(TEST_TOPIC));
            ConsumerRecords<String, Customer> records;
            records = consumer.poll(10000);
            assertThat(records).isNotNull();
            assertThat(records.isEmpty()).isFalse();

            ConsumerRecord<String, Customer> msg = records.iterator().next();
            assertThat(msg).isNotNull();
            GenericRecord avroCustomer = msg.value();
            assertThat(avroCustomer.getClass().getName().equals(Customer.class.getName()));
            assertThat(msg.key()).isEqualTo("key");
            assertThat(msg.value().getFirstName().equals("John"));
            assertThat(msg.value().getLastName().equals("Doe"));
        }

        broker.stop();
    }

    @Test
    public void testProducerAndConsumerWithAvro() throws Exception {
        final EphemeralKafkaBroker broker = EphemeralKafkaBroker.create(9092, 2181);
        CompletableFuture<Void> res = broker.start();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            //Ignore
        }

        assertThat(broker.isRunning()).isTrue();

        MyProducerWithAvro avroProducer = new MyProducerWithAvro();
        Customer customer = Customer.newBuilder()
                .setAge(34)
                .setAutomatedEmail(false)
                .setFirstName("John")
                .setLastName("Doe")
                .setHeight(178f)
                .setWeight(75f)
                .build();

        Future<RecordMetadata> result = avroProducer.send(TEST_TOPIC, "key", customer);
        RecordMetadata metadata = result.get(500L, TimeUnit.MILLISECONDS);
        assertThat(metadata).isNotNull();
        assertThat(metadata.topic()).isEqualTo(TEST_TOPIC);

        MyConsumerWithAvro consumerDemo = new MyConsumerWithAvro("group");

        consumerDemo.subscribe(TEST_TOPIC);
        ConsumerRecords<String, Customer> records;
        records = consumerDemo.poll(10000);
        assertThat(records).isNotNull();
        assertThat(records.isEmpty()).isFalse();

        ConsumerRecord<String, Customer> msg = records.iterator().next();
        assertThat(msg).isNotNull();
        assertThat(msg.key()).isEqualTo("key");
        assertThat(msg.value().getFirstName().equals("John"));
        assertThat(msg.value().getLastName().equals("Doe"));

        broker.stop();
    }
}
