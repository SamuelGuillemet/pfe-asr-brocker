package fix.server;

import fix.config.BasicConfig;
import pfe_broker.avro.Order;
import quickfix.DoNotSend;
import quickfix.FieldNotFound;
import quickfix.IncorrectDataFormat;
import quickfix.IncorrectTagValue;
import quickfix.Message;
import quickfix.MessageCracker;
import quickfix.RejectLogon;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionNotFound;
import quickfix.UnsupportedMessageType;
import quickfix.field.MDEntryPx;
import quickfix.field.MDEntrySize;
import quickfix.field.MDEntryType;
import quickfix.field.MDReqID;
import quickfix.field.NoRelatedSym;
import quickfix.field.OrderQty;
import quickfix.field.SenderCompID;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.TargetCompID;
import quickfix.fix42.ExecutionReport;
import quickfix.fix42.MarketDataRequest;
import quickfix.fix42.MarketDataSnapshotFullRefresh;
import quickfix.fix42.NewOrderSingle;

public class Application extends MessageCracker implements quickfix.Application {
    private int m_orderID = 0;
    private int m_execID = 0;
    private KafkaProducerWrapper kafkaProducer;

    public Application() {
        this.kafkaProducer = new KafkaProducerWrapper(BasicConfig.BOOTSTRAP_SERVERS, BasicConfig.ORDERS_TOPIC_NAME);
    }

    @Override
    public void onCreate(SessionID sessionId) {
    }

    @Override
    public void onLogon(SessionID sessionId) {
    }

    @Override
    public void onLogout(SessionID sessionId) {
    }

    @Override
    public void toAdmin(Message message, SessionID sessionId) {
    }

    @Override
    public void fromAdmin(Message message, SessionID sessionId)
            throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, RejectLogon {
    }

    @Override
    public void toApp(Message message, SessionID sessionId) throws DoNotSend {
    }

    @Override
    public void fromApp(Message message, SessionID sessionId)
            throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {
        crack(message, sessionId);
    }

    public void onMessage(NewOrderSingle message, SessionID sessionID) throws FieldNotFound,
            UnsupportedMessageType, IncorrectTagValue {
        try {
            System.out.println("Received new Single Order");
            pfe_broker.avro.Side side;
            if (message.getString(Side.FIELD).charAt(0) == quickfix.field.Side.BUY) {
                side = pfe_broker.avro.Side.BUY;
            } else {
                side = pfe_broker.avro.Side.SELL;
            }
            pfe_broker.avro.Order avroOrder = new Order(message.getHeader().getString(SenderCompID.FIELD),
                    message.getString(Symbol.FIELD), message.getInt(OrderQty.FIELD), side);
            publishToKafka(avroOrder, m_orderID++);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void onMessage(MarketDataRequest message, SessionID sessionID) {
        try {
            sendMarketDataSnapshot(message);
        } catch (quickfix.FieldNotFound e) {
            e.printStackTrace();
        }
    }

    public void onMessage(ExecutionReport message, SessionID sessionID) {
        try {
            String senderCompID = "SERVER";
            String targetCompID = "user1";

            sessionID = new SessionID("FIX.4.2", senderCompID, targetCompID);
            Session.sendToTarget(message, sessionID);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendMarketDataSnapshot(MarketDataRequest message) throws FieldNotFound {
        MarketDataRequest.NoRelatedSym noRelatedSyms = new MarketDataRequest.NoRelatedSym();

        // Extract the related symbols from the MarketDataRequest message
        int relatedSymbolCount = message.getInt(NoRelatedSym.FIELD);

        MarketDataSnapshotFullRefresh fixMD = new MarketDataSnapshotFullRefresh();
        fixMD.setString(MDReqID.FIELD, message.getString(MDReqID.FIELD));

        for (int i = 1; i <= relatedSymbolCount; ++i) {
            message.getGroup(i, noRelatedSyms);
            String symbol = noRelatedSyms.getString(Symbol.FIELD);
            fixMD.setString(Symbol.FIELD, symbol);

            double symbolPrice = 0.0;
            int symbolVolume = 0;

            if (symbol.equals("GOOGL")) {
                symbolPrice = 123.45;
                symbolVolume = 1000;
            } else if (symbol.equals("AAPL")) {
                symbolPrice = 456.78;
                symbolVolume = 1000;
            }

            MarketDataSnapshotFullRefresh.NoMDEntries noMDEntries = new MarketDataSnapshotFullRefresh.NoMDEntries();
            //noMDEntries.setString(Symbol.FIELD, symbol);
            noMDEntries.setChar(MDEntryType.FIELD, '0');
            noMDEntries.setDouble(MDEntryPx.FIELD, symbolPrice);
            noMDEntries.setInt(MDEntrySize.FIELD, symbolVolume);
            fixMD.addGroup(noMDEntries);
        }

        String senderCompId = message.getHeader().getString(SenderCompID.FIELD);
        String targetCompId = message.getHeader().getString(TargetCompID.FIELD);
        fixMD.getHeader().setString(SenderCompID.FIELD, targetCompId);
        fixMD.getHeader().setString(TargetCompID.FIELD, senderCompId);

        try {
            Session.sendToTarget(fixMD, targetCompId, senderCompId);
        } catch (SessionNotFound e) {
            e.printStackTrace();
        }
    }

    private void publishToKafka(Order order, Integer key) {
        try {
            System.out.println(
                    "Publishing new order " + order.toString() + " to topic: " + BasicConfig.ORDERS_TOPIC_NAME);
            kafkaProducer.publishMessage(order, key);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
