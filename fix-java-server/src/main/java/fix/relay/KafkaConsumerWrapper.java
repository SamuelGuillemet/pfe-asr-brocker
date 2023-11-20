package fix.relay;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import fix.relay.processors.*;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerWrapper {
    private final String bootstrapServers;
    private final List<String> topics;
    private final Application application;
    private final String groupId;

    public KafkaConsumerWrapper(String bootstrapServers, List<String> topics, Application application, String groupId) {
        this.bootstrapServers = bootstrapServers;
        this.topics = topics;
        this.application = application;
        this.groupId = groupId;
    }

    public void startConsuming() {
        System.out.println("Starting consumption...");
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        Consumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(topics);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record -> processRecord(record.value(), record.topic()));
        }
    }

    private void processRecord(String recordValue, String topic) {
        RecordProcessor processor;
        try {
            
            JSONObject jsonObject = new JSONObject(recordValue);
            switch (topic) {
                case "accepted-orders":
                    processor = new AcceptedOrderProcessor();
                    processor.processRecord(jsonObject, application);
                    break;
                case "rejected-orders":
                    processor = new RejectedOrderProcessor();
                    processor.processRecord(jsonObject, application);
                    break;
                case "accepted-trades":
                    processor = new AcceptedTradeProcessor();
                    processor.processRecord(jsonObject, application);
                    break;
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
