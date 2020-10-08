import pandas as pd
import logging

module_logger = logging.getLogger('xProject')
module_logger.setLevel(logging.DEBUG)  # config


def set_logger(mode='a'):
    # DEBUG
    fh = logging.FileHandler('xproject.log', mode=mode)
    fh.setLevel(logging.DEBUG)
    # ERROR
    sh = logging.StreamHandler()
    sh.setLevel(logging.ERROR)
    # log format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    sh.setFormatter(formatter)
    #
    module_logger.addHandler(fh)
    module_logger.addHandler(sh)


class TickPrice:
    LastPrice = 4
    AskPrice = 2
    BidPrice = 1


IP_ADDRESS = '127.0.0.1'
IP_PORT = 7497
GROUP_NAME = 'IPO'

# портфель - то, что куплено в клиенте
# правила для торговли из БД

# Tick Price list: https://interactivebrokers.github.io/tws-api/tick_types.html

# TODO: dictionary|enum for fields names

db_data = None
tws_data = None


# request time to "wake-up" IB's API
def tws_time(client_id=0, time_sleep=0.5):
    from datetime import datetime
    from threading import Thread
    import time

    from ibapi.client import EClient
    from ibapi.wrapper import EWrapper
    from ibapi.common import TickerId

    class ib_class(EWrapper, EClient):
        def __init__(self, addr, port, client_id):
            EClient.__init__(self, self)
            self.connect(addr, port, client_id)  # Connect to TWS
            module_logger.info(f"connection to {addr}:{port} client = {client_id}")
            thread = Thread(target=self.run)  # Launch the client thread
            thread.start()

        def currentTime(self, cur_time):
            t = datetime.fromtimestamp(cur_time)
            module_logger.debug('Current TWS date/time: {}'.format(t))

        def error(self, reqId: TickerId, errorCode: int, errorString: str):
            if reqId > -1:
                module_logger.error("Error. Id: ", reqId, " Code: ", errorCode, " Msg: ", errorString)

    ib_api = ib_class(IP_ADDRESS, IP_PORT, client_id)
    ib_api.reqCurrentTime()  # associated callback: currentTime
    time.sleep(time_sleep)
    ib_api.disconnect()


def read_positions(client_id=10, time_sleep=3.0):  # read all accounts positions and return DataFrame with information

    from ibapi.client import EClient
    from ibapi.wrapper import EWrapper
    from ibapi.common import TickerId
    from threading import Thread

    import time

    class ib_class(EWrapper, EClient):

        def __init__(self, addr, port, client_id):
            EClient.__init__(self, self)

            self.connect(addr, port, client_id)  # Connect to TWS
            thread = Thread(target=self.run)  # Launch the client thread
            thread.start()

            self.all_positions = pd.DataFrame([], columns=['Account', 'Symbol', 'Quantity', 'Average Cost', 'Sec Type'])
            self.all_multipositions = pd.DataFrame([], columns=['Symbol', 'Quantity', 'Average Cost'])

        def error(self, reqId: TickerId, errorCode: int, errorString: str):
            if reqId > -1:
                module_logger.error(f"Error. Id: {reqId} Code: {errorCode} Msg: {errorString}")

        def position(self, account, contract, pos, avgCost):
            index = str(account) + str(contract.symbol)
            self.all_positions.loc[index] = account, contract.symbol, pos, avgCost, contract.secType

        def positionEnd(self):
            super().positionEnd()
            module_logger.debug("PositionEnd")

        def positionMulti(self, reqId, account, modelCode, contract, pos, avgCost):
            super().positionMulti(reqId, account, modelCode, contract, pos, avgCost)
            module_logger.debug(f"PositionMulti. RequestId: {reqId} Account: {account} ModelCode: {modelCode} Symbol: {contract.symbol} SecType: {contract.secType} Currency: {contract.currency} Position: {pos} AvgCost: {avgCost}")
            if contract.secType == 'CASH':
                module_logger.debug(f'Skip CASH')
            else:
                index = str(account) + str(contract.symbol)
                self.all_multipositions.loc[index] = contract.symbol, pos, avgCost

        def positionMultiEnd(self, reqId: int):
            super().positionMultiEnd(reqId)
            module_logger.debug(f"PositionMultiEnd. RequestId: {reqId}")

    ib_api = ib_class(IP_ADDRESS, IP_PORT, client_id)
    # ib_api.reqPositions() # associated callback: position
    ib_api.reqPositionsMulti(9006, "IPO", "")

    module_logger.info("Waiting for IB's API response for accounts positions requests...")
    time.sleep(time_sleep)
    current_positions = ib_api.all_multipositions
    current_positions.set_index('Symbol', inplace=True, drop=True)  # set all_positions DataFrame index to "Account"

    module_logger.debug("read_positions -> Disconnect")
    ib_api.disconnect()

    return current_positions


def read_navs(client_id=10, time_sleep=3.0, group_name="IPO"):  # read all accounts NAVs

    from ibapi.client import EClient
    from ibapi.wrapper import EWrapper
    from ibapi.common import TickerId
    from threading import Thread

    import pandas as pd
    import time

    class ib_class(EWrapper, EClient):

        def __init__(self, addr, port, client_id):
            EClient.__init__(self, self)

            self.connect(addr, port, client_id)  # Connect to TWS
            thread = Thread(target=self.run)  # Launch the client thread
            thread.start()

            self.all_accounts = pd.DataFrame([], columns=['reqId', 'Account', 'Tag', 'Value', 'Currency'])

        def error(self, reqId: TickerId, errorCode: int, errorString: str):
            if reqId > -1:
                module_logger.error("Error. Id: ", reqId, " Code: ", errorCode, " Msg: ", errorString)

        def accountSummary(self, reqId, account, tag, value, currency):
            index = str(account)
            self.all_accounts.loc[index] = reqId, account, tag, value, currency

    ib_api = ib_class(IP_ADDRESS, IP_PORT, client_id)
    ib_api.reqAccountSummary(0, group_name, "NetLiquidation")  # associated callback: accountSummary
    module_logger.info("Waiting for IB's API response for NAVs requests...")
    time.sleep(time_sleep)
    current_nav = ib_api.all_accounts

    module_logger.debug("read_navs -> Disconnect")
    ib_api.disconnect()

    return current_nav


def get_portfolio():
    global tws_data

    def agg_multipositions(row):
        d = {'Quantity': row['Quantity'].sum(), 'AverageCost': row['Average Cost'].mean()}
        return pd.Series(d, index=['Quantity', 'AverageCost'])

    positions = read_positions()
    tws_data = positions.groupby(['Symbol']).apply(agg_multipositions)


def get_groupnavs(group_name="IPO"):
    navs = read_navs(group_name=group_name)
    return navs['Value'].astype('float').sum()


def get_db():
    global db_data
    db_data = pd.read_csv("companies.csv", sep=';', header=0).set_index('ticker_name')


def get_list_for_buy():
    pass


def place_order(client_id=123, ticker='SPY', sAction='SELL', iTotalQuantity=1000, sOrderType='LMT', sLmtPrice='1.10'):
    from ibapi.client import EClient
    from ibapi.wrapper import EWrapper
    from ibapi.contract import Contract
    from ibapi.order import Order

    import threading
    import time

    class ib_class(EWrapper, EClient):
        def __init__(self):
            EClient.__init__(self, self)

        def nextValidId(self, orderId: int):
            super().nextValidId(orderId)
            self.nextorderId = orderId
            module_logger.info(f'The next valid order id is: {self.nextorderId}')

        def orderStatus(self, orderId, status, filled, remaining, avgFullPrice, permId, parentId, lastFillPrice,
                        clientId, whyHeld, mktCapPrice):
            module_logger.info(f'orderStatus - orderid: {orderId} status: {status} filled: {filled} remaining: {remaining} lastFillPrice: {lastFillPrice}')

        def openOrder(self, orderId, contract, order, orderState):
            module_logger.info(f'openOrder id: {orderId} {contract.symbol} {contract.secType}  @ {contract.exchange} : {order.action} {order.orderType} {order.totalQuantity} {orderState.status}')

        def execDetails(self, reqId, contract, execution):
            module_logger.info(f'Order Executed: {reqId} {contract.symbol} {contract.secType} {contract.currency} {execution.execId}  {execution.orderId} {execution.shares} {execution.lastLiquidity}')

        def error(self, reqId, errorCode, errorString):
            if errorCode == 202:
                module_logger.error('order canceled')

        def contractDetails(self, reqId: int, contractDetails):
            self.contract_details[reqId] = contractDetails

        def get_contract_details(self, reqId, contract):
            self.contract_details[reqId] = None
            self.reqContractDetails(reqId, contract)
            # Error checking loop - breaks from loop once contract details are obtained
            for i in range(50):
                if not self.contract_details[reqId]:
                    time.sleep(0.1)
                else:
                    break
            # Raise if error checking loop count maxed out (contract details not obtained)
            if i == 49:
                raise Exception('error getting contract details')
            # Return contract details otherwise
            return app.contract_details[reqId].contract

    def stock_contract(symbol, secType='STK', exchange='SMART', currency='USD'):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = secType
        contract.exchange = exchange
        contract.currency = currency
        return contract

    def create_order(sAction, iTotalQuantity, sOrderType, sLmtPrice):
        order = Order()
        order.action = sAction
        order.totalQuantity = iTotalQuantity
        order.orderType = sOrderType
        order.lmtPrice = sLmtPrice
        return order

    def create_order_pct(sAction="SELL", iPercent="-100", sOrderType="MKT", iTotalQuantity=0.0, bTransmit=True):
        order = Order()
        order.action = sAction
        order.faGroup = GROUP_NAME
        order.faMethod = "PctChange"
        order.faPercentage = iPercent
        order.orderType = sOrderType
        order.totalQuantity = iTotalQuantity
        order.transmit = bTransmit
        return order

    def run_loop():
        app.run()

    result = None
    app = ib_class()
    app.connect(IP_ADDRESS, IP_PORT, client_id)

    app.nextorderId = None

    # Start the socket in a thread
    api_thread = threading.Thread(target=run_loop, daemon=True)
    api_thread.start()

    # Check if the API is connected via orderid
    while True:
        if isinstance(app.nextorderId, int):
            module_logger.debug('API connected')
            break
        else:
            module_logger.debug('waiting for connection')
            time.sleep(1)

    ticker_row = get_ticker_by_name(ticker)

    if ticker_row is not None:
        # order = create_order(sAction, iTotalQuantity, sOrderType, sLmtPrice)
        order = create_order_pct(sAction=sAction)
        contract = stock_contract(ticker, currency=ticker_row['currency'])
        # contract = app.get_contract_details(ticker_row['request_id'], order)
        app.placeOrder(app.nextorderId, contract, order)
        result = True
    else:  # error
        module_logger.error(f'Ticker was not found by name {ticker}')

    time.sleep(3)
    app.disconnect()

    return result


def callback_processing(client_id=123, ticker='AAPL', sec_type='STK', exchange='SMART', currency='USD'):
    from ibapi.client import EClient
    from ibapi.wrapper import EWrapper
    from ibapi.contract import Contract
    # from ibapi.order import Order

    import threading
    import time

    class ib_class(EWrapper, EClient):
        def __init__(self):
            EClient.__init__(self, self)

        """
        def nextValidId(self, orderId: int):
            super().nextValidId(orderId)
            self.nextorderId = orderId
            module_logger.info(f'The next valid order id is: {self.nextorderId}')
       

        def orderStatus(self, orderId, status, filled, remaining, avgFullPrice, permId, parentId, lastFillPrice,
                        clientId, whyHeld, mktCapPrice):
            module_logger.info(f'orderStatus - orderid: {orderId} status: {status} filled: {filled} remaining: {remaining} lastFillPrice: {lastFillPrice}')

        def openOrder(self, orderId, contract, order, orderState):
            module_logger.info(" ".join(['openOrder id:', orderId, contract.symbol, contract.secType, '@', contract.exchange, ':',
                  order.action, order.orderType, order.totalQuantity, orderState.status]))

        def execDetails(self, reqId, contract, execution):
            module_logger.info(" ".join(['Order Executed: ', reqId, contract.symbol, contract.secType, contract.currency, execution.execId,
                  execution.orderId, execution.shares, execution.lastLiquidity]))

        def stock_contract(symbol, secType='STK', exchange='SMART', currency='USD'):
            contract = Contract()
            contract.symbol = symbol
            contract.secType = secType
            contract.exchange = exchange
            contract.currency = currency
            return contract
        """
        def tickPrice(self, reqId, tickType, price, attrib):
            if tickType == TickPrice.LastPrice:
                module_logger.info(f'The current ask price for request_id = {reqId} is: {price}')

                for ticker_name, db_row in get_ticker_by_reqid(reqId).iterrows():
                    break

                try:
                    tws_row = tws_data.loc[ticker_name]
                except:
                    return

                module_logger.debug(f'Ticker name : {ticker_name}')

                initial_cost = tws_row['Quantity'] * tws_row['AverageCost']
                current_cost = tws_row['Quantity'] * price
                delta_cost = initial_cost - current_cost

                try:
                    group_nav = get_groupnavs(GROUP_NAME)
                except Exception as e:
                    module_logger.error(f'Error getting groupnav for {GROUP_NAME}: {e}')

                risk_value = group_nav * float(db_row['risk_check'])
                module_logger.debug(f'Calculated delta_cost: {delta_cost}, risk_value: {risk_value}')

                if (True or delta_cost < -1. * risk_value) and (tws_row['Quantity'] > 0):
                    # закрываем тикер
                    try:
                        if place_order(client_id=client_id+reqId, ticker=ticker_name, sAction='SELL'):
                            module_logger.info(f'place order for ticker: {ticker_name}')
                            #TODO set_flag
                            get_portfolio()
                            app.disconnect()
                        else:
                            module_logger.error(f"order ticker: {ticker_name} couldn't buy")
                    except Exception as e:
                        module_logger.error(f'Error buying ticker for {ticker_name}: {e}')

    def run_loop():
        app.run()

    app = ib_class()
    app.connect(IP_ADDRESS, IP_PORT, client_id)

    # Start the socket in a thread
    api_thread = threading.Thread(target=run_loop, daemon=True)
    api_thread.start()

    time.sleep(1)  # Sleep interval to allow time for connection to server

    # Create contract object
    contract = Contract()
    contract.symbol = ticker
    contract.secType = sec_type
    contract.exchange = exchange
    contract.currency = currency

    # Request Market Data
    req_id = get_reqid(ticker)
    if not req_id is None:
        app.reqMktData(req_id, contract, '', False, False, [])
    else:
        module_logger.error('error request market data')
        app.disconnect()


"""
    look at rows in data base and search row with ticker_name
    if it will find then will return
    else   error
"""


def get_reqid(ticker_name):
    ticker = get_ticker_by_name(ticker_name)
    if ticker is None or ticker.empty:
        module_logger.error(f'Ticker {ticker_name} is not found')
    else:
        return ticker['request_id']


def get_ticker_by_reqid(reqid):
    if db_data is not None and not db_data.empty:
        return db_data.loc[db_data['request_id'] == reqid]
    else:
        module_logger.error(f'error getting ticker by request id {reqid}')
        return None


def get_row_by_ticker(ticker_name):
    if tws_data is not None and not tws_data.empty:
        return tws_data[ticker_name]
    else:
        module_logger.error(f'error getting row by ticker name {ticker_name}')
        return None


def get_ticker_by_name(ticker_name):
    if db_data is not None and not db_data.empty and ticker_name in db_data.index:
        return db_data.loc[ticker_name]
    else:
        module_logger.error(f'error getting ticker by ticker name {ticker_name}')
        return None


# print( tws_data.loc['SVXY']['Quantity'] )
# print( get_row_by_ticker('SVXY') )

def main():
    global db_data, tws_data

    # 1. Получить портфель и БД
    get_db()
    get_portfolio()
    # print(tws_data.loc['SPY'])
    # print(d.iloc[0])


    # 1.1 Для всех из портфеля повесить обработчик
    i = 10
    for index, row in tws_data.iterrows():
        callback_processing(client_id=i, ticker=index)
        i += 1

    # 2. Получить список на покупку - те, которые есть в БД, но нет в TWS и для которых STOP_PRICE = False
    join_data = db_data.join(tws_data)
    i = 10
    for index, row in join_data[((join_data['Quantity'].isnull()) & (join_data['stop_price_bool'] == 0))].iterrows():
        callback_processing(client_id=i, ticker=index)
        i += 1
        """
        # 3. Для этого списка реализовать покупку для каждой позиции
        if place_order(ticker=index):
            # 3.1 Для каждой, которую получилось купить повесить обработчик
            callback_processing(ticker=index)
        """


if __name__ == '__main__':
    set_logger('w')
    main()
