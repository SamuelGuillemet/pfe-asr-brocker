import faulthandler
faulthandler.enable()
import quickfix as fix
import time
from time import sleep

class ClientApplication(fix.Application):
    sessionID = None
    def onCreate(self, sessionID):
        return

    def onLogon(self, sessionID):
        print("Session %s successfully logged in" % sessionID)
        self.sessionID = sessionID
        return

    def onLogout(self, sessionID):
        print("Session %s logged out" % sessionID)
        return

    def toAdmin(self, message, sessionID):
        print("Sending to Admin %s" % message)
        return

    def fromAdmin(self, message, sessionID):
        print("Received from Admin %s" % message)
        return

    def toApp(self, message, sessionID):
        print("Sent message: ", end='')
        print(message)
        return

    def fromApp(self, message, sessionID):
        print("Received message: ", end='')
        print(message)
        return


sender_comp_id = 'CLIENT1'
target_comp_id = 'SERVER'

# FIX message with fake GOOGL stock data
googl_message = fix.Message()
googl_message.getHeader().setField(fix.SenderCompID(sender_comp_id))
googl_message.getHeader().setField(fix.TargetCompID(target_comp_id))
googl_message.getHeader().setField(fix.MsgType(fix.MsgType_NewOrderSingle))
googl_message.setField(fix.HandlInst('2'))
googl_message.setField(fix.OrdType('1'))
googl_message.setField(fix.ClOrdID('1'))
googl_message.setField(fix.TransactTime())
googl_message.setField(fix.Symbol('GOOGL'))
googl_message.setField(fix.Side(fix.Side_BUY))
googl_message.setField(fix.Price(1500.00))
googl_message.setField(fix.OrderQty(100))

settings = fix.SessionSettings('client.cfg')
store_factory = fix.FileStoreFactory(settings)
log_factory = fix.FileLogFactory(settings)
application = ClientApplication()

initiator = fix.SocketInitiator(application, store_factory,settings,log_factory)
initiator.start()
sleep(1)
fix.Session.sendToTarget(googl_message, application.sessionID)
sleep(1)
initiator.stop()
