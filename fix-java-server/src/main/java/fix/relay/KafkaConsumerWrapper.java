package fix.relay;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import fix.config.BasicConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import pfe_broker.avro.Order;
import pfe_broker.avro.OrderRejectReason;
import pfe_broker.avro.RejectedOrder;
import pfe_broker.avro.Trade;
import quickfix.field.AvgPx;
import quickfix.field.CumQty;
import quickfix.field.ExecID;
import quickfix.field.ExecTransType;
import quickfix.field.ExecType;
import quickfix.field.LeavesQty;
import quickfix.field.OrdRejReason;
import quickfix.field.OrdStatus;
import quickfix.field.OrderID;
import quickfix.field.OrderQty;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.fix42.ExecutionReport;

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
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, BasicConfig.SCHEMA_REGISTRY_URL);

        Consumer<Integer, Trade> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(topics);

        while (true) {
            ConsumerRecords<Integer, Trade> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record -> processTrade(record.value()));
        }
    }

    public void processTrade(Trade trade) {
        try {
            Order order = trade.getOrder();
            String symbol = order.getSymbol().toString();
            String execId = "execId";
            String clOrdID = "clOrdID";
            pfe_broker.avro.Side sideAvro = order.getSide();
            char side = quickfix.field.Side.SELL;
            switch (sideAvro) {
                case BUY:
                    side = quickfix.field.Side.BUY;
                    break;
                case SELL:
                    side = quickfix.field.Side.SELL;
            }
            int tradeQuantity = trade.getQuantity();
            int baseQuantity = order.getQuantity();
            double price = trade.getPrice();

            ExecutionReport executionReport = new ExecutionReport(
                    new OrderID(clOrdID),
                    new ExecID(execId),
                    new ExecTransType(ExecTransType.NEW),
                    new ExecType(ExecType.NEW),
                    new OrdStatus(OrdStatus.NEW),
                    new Symbol(symbol),
                    new Side(side),
                    new LeavesQty(baseQuantity - tradeQuantity),
                    new CumQty(0),
                    new AvgPx(price));
            executionReport.set(new OrderQty(tradeQuantity));
            application.sendExecutionReport(executionReport);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void processRecord(Order order) {
        try {
            String symbol = order.getSymbol().toString();
            String execId = "execId";
            String clOrdID = "clOrdID";
            double leavesQty = 10;
            pfe_broker.avro.Side sideAvro = order.getSide();
            char side = quickfix.field.Side.SELL;
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
                    new AvgPx(0));
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
            pfe_broker.avro.Side sideAvro = order.getSide();
            char side = Side.SELL;
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
                    new AvgPx(0));
            executionReport.set(new OrderQty(quantity));
            executionReport.setInt(OrdRejReason.FIELD, 1);
            application.sendExecutionReport(executionReport);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
