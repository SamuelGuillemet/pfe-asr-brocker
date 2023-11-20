package fix.relay.processors;

import org.json.JSONObject;

import fix.relay.Application;

public interface RecordProcessor {
    void processRecord(JSONObject jsonObject, Application application);
}
