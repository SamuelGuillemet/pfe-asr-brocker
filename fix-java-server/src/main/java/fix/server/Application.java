package fix.server;

import org.json.JSONObject;

import com.fasterxml.jackson.databind.introspect.TypeResolutionContext.Basic;

import quickfix.*;
import quickfix.field.*;
import quickfix.fix42.ExecutionReport;
import quickfix.fix42.MarketDataRequest;
import quickfix.fix42.MarketDataSnapshotFullRefresh;
import quickfix.fix42.NewOrderSingle;
import fix.avro.Order;
import fix.config.BasicConfig;

public class Application extends MessageCracker implements quickfix.Application {
    private int m_orderID = 0;
    private int m_execID = 0;

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
    public void fromAdmin(Message message, SessionID sessionId) throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, RejectLogon {
    }

    @Override
    public void toApp(Message message, SessionID sessionId) throws DoNotSend {
    }

    @Override
    public void fromApp(Message message, SessionID sessionId) throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {
        crack(message,sessionId);
    }

    public void onMessage(NewOrderSingle message, SessionID sessionID) throws FieldNotFound,
    UnsupportedMessageType, IncorrectTagValue {
        try {
            System.out.println("Received new Single Order");
            fix.avro.Side side;
            if(message.getString(Side.FIELD)=="1"){
                side = fix.avro.Side.BUY;
            }else{
                side = fix.avro.Side.SELL;
            }
            fix.avro.Order avroOrder = new Order(message.getHeader().getString(SenderCompID.FIELD), message.getString(Symbol.FIELD), message.getInt(OrderQty.FIELD), side);
            publishToKafka(avroOrder);
            

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
            String targetCompID = "CLIENT";

            sessionID = new SessionID("FIX.4.2", senderCompID, targetCompID);
            Session.sendToTarget(message,sessionID);
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


    private void publishToKafka(Order order) {
        try {
            System.out.println("Publishing new order "+order.toString()+" to topic: "+BasicConfig.ORDERS_TOPIC_NAME);
            KafkaProducerWrapper kafkaProducer = new KafkaProducerWrapper(BasicConfig.BOOTSTRAP_SERVERS, BasicConfig.ORDERS_TOPIC_NAME);
            kafkaProducer.publishMessage(order);
            System.out.println("Successfully published new order "+order.toString()+" to topic: "+BasicConfig.ORDERS_TOPIC_NAME);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
