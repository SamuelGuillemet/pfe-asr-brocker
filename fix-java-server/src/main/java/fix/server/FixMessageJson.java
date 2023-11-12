package fix.server;
import org.json.JSONObject;

public class FixMessageJson {
    private String senderCompID;
    private String targetCompID;
    private String msgType;
    private String handlInst;
    private String ordType;
    private String clOrdID;
    private String transactTime;
    private String symbol;
    private String side;
    private String price;
    private String orderQty;
    private String origClOrdID;
    private String execType;
    private String cumQty;


    public FixMessageJson(String senderCompID, String targetCompID, String msgType,
                     String handlInst, String ordType, String clOrdID,
                     String transactTime, String symbol, String side,
                     String price, String orderQty) {
        this.senderCompID = senderCompID;
        this.targetCompID = targetCompID;
        this.msgType = msgType;
        this.handlInst = handlInst;
        this.ordType = ordType;
        this.clOrdID = clOrdID;
        this.transactTime = transactTime;
        this.symbol = symbol;
        this.side = side;
        this.price = price;
        this.orderQty = orderQty;
        this.origClOrdID = origClOrdID;
        this.execType = execType;
        this.cumQty = cumQty;
    }

    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        json.put("SenderCompID", senderCompID);
        json.put("TargetCompID", targetCompID);
        json.put("MsgType", msgType);
        json.put("HandlInst", handlInst);
        json.put("OrdType", ordType);
        json.put("ClOrdID", clOrdID);
        json.put("TransactTime", transactTime);
        json.put("Symbol", symbol);
        json.put("Side", side);
        json.put("Price", price);
        json.put("OrderQty", orderQty);
        json.put("OrigClOrdID", origClOrdID);
        json.put("ExecType", execType);
        json.put("CumQty", cumQty);

        return json;
    }
}
