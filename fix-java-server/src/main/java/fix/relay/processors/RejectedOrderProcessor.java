package fix.relay.processors;

import org.json.JSONObject;

import fix.relay.Application;
import quickfix.fix42.ExecutionReport;
import quickfix.fix42.NewOrderSingle;
import quickfix.field.*;

public class RejectedOrderProcessor implements RecordProcessor {
    @Override
    public void processRecord(JSONObject jsonObject, Application application) {
        try {
            // Extract fields from JSON
            String symbol = jsonObject.getString("symbol");
            char side = jsonObject.getString("side").charAt(0);
            double price = jsonObject.getDouble("price");
            int quantity = jsonObject.getInt("quantity");
            String reason = jsonObject.getString("reason");

            // Recreate the order
            NewOrderSingle order = new NewOrderSingle();
            order.set(new Symbol(symbol));
            order.set(new quickfix.field.Side(side));
            order.set(new quickfix.field.Price(price));
            order.set(new quickfix.field.OrderQty(quantity));

            // Send execution report
            ExecutionReport executionReport = new ExecutionReport();
            executionReport.set(new ExecID("your_exec_id"));
            executionReport.set(new ExecType(ExecType.REJECTED));
            executionReport.set(new OrdStatus(OrdStatus.REJECTED));
            executionReport.set(order.getSymbol());
            executionReport.set(new Text(reason));
            
            application.sendExecutionReport(executionReport);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
