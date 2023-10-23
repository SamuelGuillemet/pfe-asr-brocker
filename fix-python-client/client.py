import quickfix as fix
import quickfix42 as fix42
import configparser
import time

import fixutils


ORDERS = {}


class BaseApplication(fix.Application):
    def onCreate(self, sessionID):
        return

    def onLogon(self, sessionID):
        return

    def onLogout(self, sessionID):
        return

    def toAdmin(self, message, sessionID):
        self.sessionID = sessionID
        return

    def fromAdmin(self, message, sessionID):
        return

    def toApp(self, message, sessionID):
        return

    def fromApp(self, message, sessionID):
        return


ORDER_TABLE = {}


class Client(BaseApplication):
    def set_logging(self, logger):
        self.logger = logger

    def onCreate(self, sessionID):
        self.logger.info(f"Successfully created session {sessionID}.")
        return

    def onLogon(self, sessionID):
        self.logger.info(f"{sessionID} session successfully logged in.")
        return

    def onLogout(self, sessionID):
        self.logger.info(f"{sessionID} session successfully logged out.")
        return

    def toApp(self, message, sessionID):
        self.logger.debug(f"Sending {message} session {sessionID}")

    def fromApp(self, message, sessionID):
        self.logger.info(f"Got message {message} for {sessionID}.")
        self.process(message, sessionID)

    def process(self, message, sessionID):
        self.logger.debug("Processing message.")
        msgtype = fix.MsgType()
        exectype = fix.ExecType()
        message.getHeader().getField(msgtype)
        message.getField(exectype)

        if msgtype.getValue() == "8":
            if exectype.getValue() == "2":
                self.logger.info("Trade received.")
                (
                    symbol,
                    price,
                    quantity,
                    side,
                    client_order_id,
                    trade_exec_id,
                    order_status,
                ) = self.__get_attributes(message)
                self.logger.info(
                    f"Trade: {trade_exec_id}, {client_order_id} {symbol}"
                    f" {quantity}@{price} {side}"
                )
            elif exectype.getValue() == "0":
                self.logger.info("Order placed successfully.")
                (
                    symbol,
                    price,
                    quantity,
                    side,
                    client_order_id,
                    exec_id,
                    order_status,
                ) = self.__get_attributes(message)

                ORDERS[client_order_id.getValue()] = [symbol, price, quantity, side]

                self.logger.info(
                    f"Order: {exec_id}, {client_order_id} {symbol}"
                    f" {quantity}@{price} {side}"
                )
            elif exectype.getValue() == "5":
                self.logger.info("Order replaced successfully.")
                (
                    symbol,
                    price,
                    quantity,
                    side,
                    client_order_id,
                    exec_id,
                    order_status,
                ) = self.__get_attributes(message)

                ORDERS[client_order_id.getValue()] = [symbol, price, quantity, side]

                self.logger.info(
                    f"Order: {exec_id}, {client_order_id} {symbol}"
                    f" {quantity}@{price} {side}"
                )

    def __get_attributes(self, message):
        price = fix.LastPx()
        quantity = fix.LastQty()
        symbol = fix.Symbol()
        side = fix.Side()
        client_order_id = fix.ClOrdID()
        exec_id = fix.ExecID()
        order_status = fix.OrdStatus()

        message.getField(client_order_id)
        message.getField(side)
        message.getField(symbol)
        message.getField(price)
        message.getField(quantity)
        message.getField(order_status)
        message.getField(exec_id)

        return (symbol, price, quantity, side, client_order_id, exec_id, order_status)


def get_order_id(sender_comp_id, symbol):
    if symbol in ORDER_TABLE:
        _id = ORDER_TABLE[symbol]
    else:
        _id = 1

    order_id = sender_comp_id + symbol + str(_id)
    ORDER_TABLE[symbol] = _id + 1

    return order_id


def new_order(sender_comp_id, target_comp_id, symbol, quantity, price, side, order_type):
    if side.lower() == "buy":
        side = fix.Side(fix.Side_BUY)
    else:
        side = fix.Side(fix.Side_SELL)

    message = fix.Message()
    header = message.getHeader()
    header.setField(fix.BeginString("FIX.4.2"))
    header.setField(fix.SenderCompID(sender_comp_id))
    header.setField(fix.TargetCompID(target_comp_id))
    header.setField(fix.MsgType("D"))

    clordid = fix.ClOrdID() 
    message.setField(clordid)
    message.setField(fix.Symbol(symbol))
    message.setField(side)
    message.setField(fix.Price(float(price)))
    message.setField(fix.OrdType(fix.OrdType_LIMIT))
    message.setField(fix.HandlInst(fix.HandlInst_MANUAL_ORDER_BEST_EXECUTION))
    message.setField(fix.TransactTime())
    message.setField(fix.OrderQty(float(quantity)))
    message.setField(fix.Text(f"{side} {symbol} {quantity}@{price}"))
    if order_type.lower() == "market":
        message.setField(fix.OrdType(fix.OrdType_MARKET))
    else:
        message.setField(fix.OrdType(fix.OrdType_LIMIT))

    print(message)
    time.sleep(1)
    send(message)


def send(message):
    try:
        fix.Session.sendToTarget(message,app.sessionID)
    except fix.SessionNotFound:
        raise Exception(f"No session found {message}, exiting...")


def main():
    client_config = "client.cfg"
    config = configparser.ConfigParser()
    config.read(client_config)

    sender_compid = config["SESSION"]["SenderCompID"]
    target_compid = config["SESSION"]["TargetCompID"]

    settings = fix.SessionSettings(client_config)
    store = fix.FileStoreFactory(settings)
    app = Client()

    app.set_logging(logger)

    initiator = fix.SocketInitiator(app, store, settings)

    initiator.start()

    symbol = input("Symbol: ")
    price = input("Price: ")
    quantity = input("Quantity: ")
    side = input("Side: ")
    order_type = input("Type: ")

    message = new_order(
        sender_compid,
        target_compid,
        symbol,
        quantity,
        price,
        side,
        order_type,
    )

    print("Sending new order...")
    send(message)

if __name__ == "__main__":
    logger = fixutils.setup_logging("logs/", "client")
    main()