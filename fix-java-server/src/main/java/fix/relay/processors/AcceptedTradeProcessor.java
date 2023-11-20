package fix.relay.processors;

import org.json.JSONObject;

import fix.relay.Application;
import quickfix.fix42.ExecutionReport;
import quickfix.field.*;

public class AcceptedTradeProcessor implements RecordProcessor {
    @Override
    public void processRecord(JSONObject jsonObject, Application application) {
        try {
            // Extract fields from JSON
            String symbol = jsonObject.getString("symbol");
            char side = jsonObject.getString("side").charAt(0);
            double price = jsonObject.getDouble("price");
            int quantity = jsonObject.getInt("quantity");

            // Send execution report
            ExecutionReport executionReport = new ExecutionReport();
            executionReport.set(new ExecID("your_exec_id"));
            executionReport.set(new ExecType(ExecType.TRADE));
            executionReport.set(new OrdStatus(OrdStatus.FILLED));

            application.sendExecutionReport(executionReport);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
