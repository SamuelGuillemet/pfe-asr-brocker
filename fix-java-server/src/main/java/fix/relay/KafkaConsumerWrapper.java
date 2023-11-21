package fix.relay;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import quickfix.fix42.ExecutionReport;
import quickfix.field.*;
import quickfix.field.Side;

import org.apache.kafka.common.serialization.IntegerDeserializer;


import java.time.Duration;
import java.util.List;
import java.util.Properties;
import fix.avro.*;
import fix.config.BasicConfig;


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
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, BasicConfig.SCHEMA_REGISTRY_URL);


        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        Consumer<Integer, Order> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(topics);

        while (true) {
            ConsumerRecords<Integer, Order> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record -> processRecord(record.value()));
        }
    }


    public void processRecord(Order order) {
        try {
            String symbol = order.getSymbol().toString();
            String execId = "execId";
            String clOrdID = "clOrdID";
            double leavesQty = 10;
            fix.avro.Side sideAvro = order.getSide();
            char side=quickfix.field.Side.SELL;
            switch (sideAvro) {
                case BUY:
                    side = quickfix.field.Side.BUY;
                    break;
                case SELL:
                    side = quickfix.field.Side.SELL;
            }
            int quantity = order.getQuantity();

            ExecutionReport executionReport = new ExecutionReport(
            new OrderID(clOrdID),
            new ExecID(execId),
            new ExecTransType(ExecTransType.NEW),
            new ExecType(ExecType.NEW),
            new OrdStatus(OrdStatus.NEW),
            new Symbol(symbol),
            new Side(side),
            new LeavesQty(leavesQty),
            new CumQty(0),
            new AvgPx(0)
            );
            executionReport.set(new OrderQty(quantity));
            application.sendExecutionReport(executionReport);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void processRecord(RejectedOrder rejectedOrder) {
        try {
            Order order = rejectedOrder.getOrder();
            String symbol = order.getSymbol().toString();
            String execId = "execId";
            String clOrdID = "clOrdID";
            double leavesQty = 10;
            fix.avro.Side sideAvro = order.getSide();
            char side=Side.SELL;
            switch (sideAvro) {
                case BUY:
                    side = Side.BUY;
                    break;
                case SELL:
                    side = Side.SELL;
            }
            int quantity = order.getQuantity();
            int rejectReason;
            OrderRejectReason reason = rejectedOrder.getReason();
            
            ExecutionReport executionReport = new ExecutionReport(
            new OrderID(clOrdID),
            new ExecID(execId),
            new ExecTransType(ExecTransType.CANCEL),
            new ExecType(ExecType.REJECTED),
            new OrdStatus(OrdStatus.REJECTED),
            new Symbol(symbol),
            new Side(side),
            new LeavesQty(leavesQty),
            new CumQty(0),
            new AvgPx(0)
            );
            executionReport.set(new OrderQty(quantity));
            executionReport.setInt(OrdRejReason.FIELD, 1);
            application.sendExecutionReport(executionReport);

        } catch (Exception e) {
            e.printStackTrace();
        }
        }
    }
