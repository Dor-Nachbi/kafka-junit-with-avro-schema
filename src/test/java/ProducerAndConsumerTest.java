import com.google.common.collect.Lists;
import kafka.EphemeralKafkaBroker;
import kafka.MyConsumer;
import kafka.MyProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class ProducerAndConsumerTest {

    public static final String TEST_TOPIC = "test-topic";

    @Test
    public void testProducer() throws Exception {
        final EphemeralKafkaBroker broker = EphemeralKafkaBroker.create(9092, 2181);
        CompletableFuture<Void> res = broker.start();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            //Ignore
        }

        assertThat(broker.isRunning()).isTrue();

        MyProducer myProducer = new MyProducer();
        Future<RecordMetadata> result = myProducer.send(TEST_TOPIC, "key", "value");
        RecordMetadata metadata = result.get(500L, TimeUnit.MILLISECONDS);
        assertThat(metadata).isNotNull();
        assertThat(metadata.topic()).isEqualTo(TEST_TOPIC);

        try (KafkaConsumer<String, String> consumer =
                     broker.createConsumer(new StringDeserializer(), new StringDeserializer(), null)) {

            consumer.subscribe(Lists.newArrayList(TEST_TOPIC));
            ConsumerRecords<String, String> records;
            records = consumer.poll(10000);
            assertThat(records).isNotNull();
            assertThat(records.isEmpty()).isFalse();

            ConsumerRecord<String, String> msg = records.iterator().next();
            assertThat(msg).isNotNull();
            assertThat(msg.key()).isEqualTo("key");
            assertThat(msg.value()).isEqualTo("value");
        }
        broker.stop();
    }

    @Test
    public void testConsumer() throws Exception {
        final EphemeralKafkaBroker broker = EphemeralKafkaBroker.create(9092, 1281);
        CompletableFuture<Void> res = broker.start();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            //Ignore
        }

        assertThat(broker.isRunning()).isTrue();

        try (KafkaProducer<String, String> producer =
                     broker.createProducer(new StringSerializer(), new StringSerializer(), null)) {
            Future<RecordMetadata> result =
                    producer.send(new ProducerRecord<>(TEST_TOPIC, "key", "value"));

            RecordMetadata metadata = result.get(500L, TimeUnit.MILLISECONDS);
            assertThat(metadata).isNotNull();
            assertThat(metadata.topic()).isEqualTo(TEST_TOPIC);
        }

        MyConsumer myConsumer = new MyConsumer("group");

        myConsumer.subscribe(TEST_TOPIC);
        ConsumerRecords<String, String> records;
        records = myConsumer.poll(10000);
        assertThat(records).isNotNull();
        assertThat(records.isEmpty()).isFalse();

        ConsumerRecord<String, String> msg = records.iterator().next();
        assertThat(msg).isNotNull();
        assertThat(msg.key()).isEqualTo("key");
        assertThat(msg.value()).isEqualTo("value");


        broker.stop();
    }

    @Test
    public void testProducerAndConsumer() throws Exception {
        final EphemeralKafkaBroker broker = EphemeralKafkaBroker.create(9092, 2181);
        CompletableFuture<Void> res = broker.start();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            //Ignore
        }

        assertThat(broker.isRunning()).isTrue();

        MyProducer myProducer = new MyProducer();
        Future<RecordMetadata> result = myProducer.send(TEST_TOPIC, "key", "value");
        RecordMetadata metadata = result.get(500L, TimeUnit.MILLISECONDS);
        assertThat(metadata).isNotNull();
        assertThat(metadata.topic()).isEqualTo(TEST_TOPIC);

        MyConsumer myConsumer = new MyConsumer("group");

        myConsumer.subscribe(TEST_TOPIC);
        ConsumerRecords<String, String> records;
        records = myConsumer.poll(10000);
        assertThat(records).isNotNull();
        assertThat(records.isEmpty()).isFalse();

        ConsumerRecord<String, String> msg = records.iterator().next();
        assertThat(msg).isNotNull();
        assertThat(msg.key()).isEqualTo("key");
        assertThat(msg.value()).isEqualTo("value");

        broker.stop();
    }
}
