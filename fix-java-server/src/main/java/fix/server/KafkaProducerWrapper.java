package fix.server;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerWrapper {
    private final String bootstrapServers;
    private final String topic;
    private final Producer<String, String> producer;

    public KafkaProducerWrapper(String bootstrapServers, String topic) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.producer = createKafkaProducer();
    }

    private Producer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new org.apache.kafka.clients.producer.KafkaProducer<>(properties);
    }

    public void publishMessage(String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
        producer.send(record);
        producer.flush();
    }

    public void close() {
        producer.close();
    }
}
