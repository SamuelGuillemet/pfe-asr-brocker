package fix.relay;

import java.util.Arrays;
import java.util.List;

import fix.config.BasicConfig;
import quickfix.*;

public class KafkaRelay {
    public static void main(String[] args) {
        try {
            SessionSettings settings = new SessionSettings(BasicConfig.KAFKA_RELAY_CONFIG_PATH);
            Application application = new Application();
            MessageStoreFactory messageStoreFactory = new FileStoreFactory(settings);
            LogFactory logFactory = new ScreenLogFactory(true, true, true);
            MessageFactory messageFactory = new DefaultMessageFactory();

            // Start the QuickFIXJ initiator
            SocketInitiator initiator = new SocketInitiator(application, messageStoreFactory, settings, logFactory,
                    messageFactory);
            initiator.start();

            System.out.println("QuickFIXJ relay is running...");

            // Start the consumer
            List<String> topics = Arrays.asList(BasicConfig.ACCEPTED_TRADES_TOPIC_NAME);
            KafkaConsumerWrapper consumer = new KafkaConsumerWrapper(BasicConfig.BOOTSTRAP_SERVERS, topics, application,
                    "relay");
            consumer.startConsuming();
            initiator.stop();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
