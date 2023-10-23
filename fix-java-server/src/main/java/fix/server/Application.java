package fix.server;

import quickfix.*;

public class Application extends MessageCracker implements quickfix.Application {

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

}
