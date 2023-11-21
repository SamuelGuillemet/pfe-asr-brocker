import faulthandler
faulthandler.enable()
import quickfix as fix
import quickfix42 as fix42
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
        try:
            msgType = fix.MsgType()
            message.getHeader().getField(msgType)
            if msgType.getValue() == fix.MsgType_MarketDataSnapshotFullRefresh:
                symbol = fix.Symbol()
                message.getField(symbol)
                depth = {}
                print(f"Received Market Data Snapshot for Symbol: {symbol.getValue()}")
                group = fix42.MarketDataSnapshotFullRefresh.NoMDEntries()
                nb_entries = int(message.getField(fix.NoMDEntries()).getString())
                for i in range(1, nb_entries + 1):
                    message.getGroup(i, group)
                    md_type = group.getField(fix.MDEntryType()).getString()
                    md_price = group.getField(fix.MDEntryPx()).getString()
                    md_amount = group.getField(fix.MDEntrySize()).getString()
                    if not md_amount:
                        md_amount = 0
                    order = {'price': float(md_price), 'amount': float(md_amount)}
                    if md_type == fix.MDEntryType_OFFER:
                        if 'asks' not in depth:
                            depth['asks'] = []
                        depth['asks'].append(order)
                    if md_type == fix.MDEntryType_BID:
                        if 'bids' not in depth:
                            depth['bids'] = []
                        depth['bids'].append(order)
                print("Received a full market data snapshot: %s" % (depth))
            elif msgType.getValue() == fix.MsgType_ExecutionReport:
                symbol = fix.Symbol()
                side = fix.Side()
                orderQty = fix.OrderQty()
                ordStatusField = fix.OrdStatus()

                message.getField(symbol)
                message.getField(side)
                message.getField(orderQty)
                message.getField(ordStatusField)

                if ordStatusField.getValue() == fix.OrdStatus_NEW:
                    print("Received Execution Report: Accept Order")
                elif ordStatusField.getValue() == fix.OrdStatus_REJECTED:
                    rejectReason = fix.OrdRejReason()
                    message.getField(rejectReason)
                    print(f"Received Execution Report: Reject Order, Reason: {rejectReason.getValue()}")
                elif ordStatusField.getValue() == fix.OrdStatus_FILLED:
                    print("Received Execution Report: Order Filled")

                
                
                print(f"Symbol: {symbol.getValue()}")
                print(f"Side: {side.getValue()}")
                print(f"Quantity: {orderQty.getValue()}")

                


            else:
                pass
        except fix.FieldNotFound as e:
            print(f"FieldNotFound: {e}")

        return

    def place_buy_order(self,sender_comp_id,target_comp_id):
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
        fix.Session.sendToTarget(googl_message, self.sessionID)

    def request_market_data(self,symbols, sender_comp_id,target_comp_id):
        market_data_request = fix.Message()
        market_data_request.getHeader().setField(fix.MDReqID('1'))
        market_data_request.getHeader().setField(fix.SubscriptionRequestType(fix.SubscriptionRequestType_SNAPSHOT_PLUS_UPDATES))
        market_data_request.getHeader().setField(fix.SenderCompID(sender_comp_id))
        market_data_request.getHeader().setField(fix.TargetCompID(target_comp_id))
        market_data_request.getHeader().setField(fix.MsgType(fix.MsgType_MarketDataRequest))
        market_data_request.setField(fix.MDUpdateType(fix.MDUpdateType_FULL_REFRESH))
        market_data_request.getHeader().setField(fix.MarketDepth(0))
        
        related_symbol = fix42.MarketDataRequest.NoRelatedSym()
        related_symbol.setField(fix.Symbol(symbols[0]))
        market_data_request.addGroup(related_symbol)

        group = fix42.MarketDataRequest.NoMDEntryTypes()
        group.setField(fix.MDEntryType(fix.MDEntryType_BID))
        market_data_request.addGroup(group)

        related_symbol = fix42.MarketDataRequest.NoRelatedSym()
        related_symbol.setField(fix.Symbol(symbols[1]))
        market_data_request.addGroup(related_symbol)

        group = fix42.MarketDataRequest.NoMDEntryTypes()
        group.setField(fix.MDEntryType(fix.MDEntryType_BID))
        market_data_request.addGroup(group)
        # related_symbols_group = fix42.MarketDataRequest.NoRelatedSym()

        # for symbol in symbols:
        #     related_symbol = fix.NoRelatedSym.NoRelatedSymEntry()
        #     related_symbol.setField(fix.Symbol(symbol))
        #     related_symbols_group.addGroup(related_symbol)

        # market_data_request.addGroup(related_symbols_group)
        fix.Session.sendToTarget(market_data_request, self.sessionID)


def main():
    sender_comp_id = 'CLIENT1'
    target_comp_id = 'SERVER'
    settings = fix.SessionSettings('client.cfg')
    store_factory = fix.FileStoreFactory(settings)
    log_factory = fix.FileLogFactory(settings)
    application = ClientApplication()

    initiator = fix.SocketInitiator(application, store_factory,settings,log_factory)
    initiator.start()
    sleep(1)
    symbols_to_request = ['GOOGL', 'AAPL']
    application.request_market_data(symbols_to_request,sender_comp_id,target_comp_id)
    # for i in range(10):
    #     sleep(10)
    #     application.place_buy_order(sender_comp_id,target_comp_id)
    sleep(10)
    application.place_buy_order(sender_comp_id,target_comp_id)
    sleep(1000)
    initiator.stop()

if __name__ == "__main__":
    main()