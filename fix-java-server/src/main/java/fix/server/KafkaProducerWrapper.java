package fix.server;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;

import fix.config.BasicConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import pfe_broker.avro.Order;

public class KafkaProducerWrapper {
    private final String bootstrapServers;
    private final String topic;
    private final Producer<Integer, Order> producer;

    private Callback callback = new Callback() {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e == null) {
                System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n", recordMetadata.topic(),
                        recordMetadata.partition(), recordMetadata.offset());
            } else {
                e.printStackTrace();
            }
        }
    };

    public KafkaProducerWrapper(String bootstrapServers, String topic) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.producer = createKafkaProducer();
    }

    private Producer<Integer, Order> createKafkaProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, BasicConfig.SCHEMA_REGISTRY_URL);

        return new KafkaProducer<>(properties);
    }

    public void publishMessage(Order value, Integer key) {
        ProducerRecord<Integer, Order> record = new ProducerRecord<>(topic, key, value);
        producer.send(record, callback);
        producer.flush();
    }

    public void close() {
        producer.close();
    }
}
