package fix.relay;

import quickfix.*;
import quickfix.fix42.ExecutionReport;


public class Application extends MessageCracker implements quickfix.Application {
        private SessionID sessionId;
        
        @Override
        public void onCreate(SessionID sessionId) {
        }

        @Override
        public void onLogon(SessionID sessionId) {
            this.sessionId = sessionId;
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
        }

        public void sendExecutionReport(ExecutionReport executionReport) {
            try {
                Session.sendToTarget(executionReport, sessionId);
            } catch (SessionNotFound sessionNotFound) {
                sessionNotFound.printStackTrace();
            }
        }
    }
