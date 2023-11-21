package fix.server;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Properties;
import fix.avro.Order;
import fix.config.BasicConfig;

public class KafkaProducerWrapper {
    private final String bootstrapServers;
    private final String topic;
    private final Producer<Integer, Order> producer;

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
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, BasicConfig.SCHEMA_REGISTRY_URL);
    
        return new org.apache.kafka.clients.producer.KafkaProducer<>(properties);
    }

    public void publishMessage(Order value) {
        ProducerRecord<Integer, Order> record = new ProducerRecord<>(topic, value);
        producer.send(record);
        producer.flush();
    }

    public void close() {
        producer.close();
    }
}
