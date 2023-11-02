package fix.server;

import quickfix.*;
import quickfix.field.MDEntryPx;
import quickfix.field.MDEntrySize;
import quickfix.field.MDEntryType;
import quickfix.field.MDReqID;
import quickfix.field.MsgType;
import quickfix.field.SecurityTradingStatus;
import quickfix.field.SenderCompID;
import quickfix.field.SubscriptionRequestType;
import quickfix.field.Symbol;
import quickfix.field.TargetCompID;
import quickfix.field.NoRelatedSym;
import quickfix.fix42.MarketDataRequest;
import quickfix.fix42.MarketDataSnapshotFullRefresh;
import quickfix.fix42.NewOrderSingle;
import quickfix.fix42.MarketDataIncrementalRefresh.NoMDEntries;

public class Application extends MessageCracker implements quickfix.Application {
    private static SessionID sessionId;

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
                String senderCompID = message.getHeader().getString(49);
                String targetCompID = message.getHeader().getString(56);
                String msgType = message.getHeader().getString(35);
                String handlInst = message.getString(21);
                String ordType = message.getString(40);
                String clOrdID = message.getString(11);
                String transactTime = message.getString(60);
                String symbol = message.getString(55);
                String side = message.getString(54);
                String price = message.getString(44);
                String orderQty = message.getString(38);

                System.out.println("Received message:");
                System.out.println("SenderCompID: " + senderCompID);
                System.out.println("TargetCompID: " + targetCompID);
                System.out.println("MsgType: " + msgType);
                System.out.println("HandlInst: " + handlInst);
                System.out.println("OrdType: " + ordType);
                System.out.println("ClOrdID: " + clOrdID);
                System.out.println("TransactTime: " + transactTime);
                System.out.println("Symbol: " + symbol);
                System.out.println("Side: " + side);
                System.out.println("Price: " + price);
                System.out.println("OrderQty: " + orderQty);
        } catch (quickfix.FieldNotFound e) {
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

}
