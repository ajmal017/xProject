# IB API
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.common import TickerId
from ibapi.contract import Contract
from ibapi.order_condition import Create, OrderCondition
from ibapi.order import Order
from ibapi.tag_value import TagValue
# MY
from logger import set_logger, log
from db_data import DBData
# COMMON
from threading import Thread, Lock
from enum import Enum
from configparser import ConfigParser
import pandas as pd
import numpy as np
import time


__config__ = ConfigParser()
__dbdata__ = DBData()


class TickPrice(Enum):
    BID = 1
    ASK = 2
    LAST = 4


def stock_contract(symbol, sec_type='STK', exchange='SMART', currency='USD'):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = sec_type
    contract.exchange = exchange
    contract.currency = currency
    return contract


def create_order(action, total_quantity, method, group_name):
    order = Order()
    order.action = action
    order.totalQuantity = total_quantity
    order.faGroup = group_name
    order.faMethod = method
    order.transmit = True
    return order


def create_order_buy(total_quantity, group_name):
    order = create_order('BUY', total_quantity, "NetLiq", group_name)
    order.tif = 'DAY'
    order.outsideRth = True
    return order


def create_order_lmt(action, total_quantity, method, lmt_price, group_name):
    order = create_order(action, total_quantity, method, group_name)
    order.orderType = "LMT"
    order.lmtPrice = lmt_price
    return order


def create_order_pct(action="SELL", total_quantity=0, percent="-100", tif='DAY', group_name="IPO"):
    order = create_order(action, total_quantity, "PctChange", group_name)
    order.orderType = "MKT"
    order.faPercentage = percent
    order.totalQuantity = total_quantity
    order.tif = tif
    return make_adaptive(order)


def make_adaptive(order, priority="Normal"):
    order.algoStrategy = "Adaptive"
    order.algoParams = [TagValue("adaptivePriority", priority)]
    return order


def place_order(ib_app, ticker, action, total_quantity, order_type, limit_price):
    order = None
    ticker_data = __dbdata__.ticker(ticker)

    if order_type == "MKT":
        order = make_adaptive(create_order(action, total_quantity, 'NetLiq', ticker_data['group_name']))
        order.orderType = order_type

    contract = stock_contract(ticker, currency=ticker_data['currency'])
    contract = ib_app.get_contract_details(ticker_data['request_id'], contract)

    ib_app.placeOrder(ib_app.nextValidId(), contract, order)


class IBApp(EWrapper, EClient):
    def __init__(self, ip_address, ip_port, id_client):
        EClient.__init__(self, self)
        # init fields
        self.is_connect = True
        self.__tws_data__ = pd.DataFrame([], columns=['Symbol', 'Quantity', 'Average Cost'])
        self.__acc_summary__ = pd.DataFrame([], columns=['reqId', 'Account', 'Tag', 'Value', 'Currency'])
        self.next_order_id = None
        self.contract_details = {}
        self.lock = Lock()

        # connect to TWS and launch client thread
        self.connect(ip_address, ip_port, id_client)
        log(f"connection to {ip_address}:{ip_port} client = {id_client}", "INFO")
        self.thread = Thread(target=self.run, daemon=True)
        self.thread.start()

        # Check if the API is connected via next_order_id
        while True:
            if isinstance(self.next_order_id, int):
                log('API connected')
                break
            else:
                log('Waiting for connection...')
                time.sleep(0.5)

    def error(self, req_id, code, message):
        if code == 202:
            log('Order canceled', 'ERROR')
        elif req_id > -1:
            log(f"Error. Id: {req_id}, code: {code}, message: {message}", "ERROR")

    def accountSummary(self, req_id, account, tag, value, currency):
        index = str(account)
        self.__acc_summary__.loc[index] = req_id, account, tag, value, currency
        log(f"{req_id} {account} {value}")

    def positionMulti(self, req_id, account, model_code, contract, pos, avg_cost):
        super().positionMulti(req_id, account, model_code, contract, pos, avg_cost)
        log(f"PositionMulti. RequestId: {req_id} Account: {account} ModelCode: {model_code} Symbol: {contract.symbol} SecType: {contract.secType} Currency: {contract.currency} Position: {pos} AvgCost: {avg_cost}")
        if contract.secType == 'CASH':
            log(f'Skip CASH')
        else:
            index = str(account) + str(contract.symbol)
            self.__tws_data__.loc[index] = contract.symbol, pos, avg_cost

    def tickPrice(self, req_id, tick_type, price, attrib):
        if tick_type == TickPrice.LAST:
            log(f'The last price for request_id = {req_id} is: {price}')
            # TODO processing price changing
            pass

    def orderStatus(self, orderId, status, filled, remaining, avgFullPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        log(f'orderStatus - orderid: {orderId} status: {status} filled: {filled} remaining: {remaining} lastFillPrice: {lastFillPrice}')

    def openOrder(self, orderId, contract, order, orderState):
        log(f'openOrder id: {orderId} {contract.symbol} {contract.secType}  @ {contract.exchange} : {order.action} {order.orderType} {order.totalQuantity} {orderState.status}')

    def execDetails(self, reqId, contract, execution):
        super().execDetails(reqId, contract, execution)
        log(f'Order Executed: {reqId} {contract.symbol} {contract.secType} {contract.currency} {execution.execId}  {execution.orderId} {execution.shares} {execution.lastLiquidity}')
        ticker = contract.symbol

        if (__dbdata__.data.loc[ticker, 'first_order_quantity'] == execution.cumQty
                and execution.orderId == __dbdata__.data.loc[ticker, 'first_order_id']):
            # place order for buy if first order is filled
            limit_price = __dbdata__.data.loc[ticker, 'first_price']
            group_nav = self.navs()
            total_quantity = int(
                round(group_nav *
                    __dbdata__.data.loc[ticker, 'allocation'] *
                    __dbdata__.data.loc[ticker, 'v1_buy_alloc'] /
                    limit_price, 0)
            ) - execution.cumQty
            #
            order = make_adaptive(
                create_order_lmt(
                    action='BUY',
                    total_quantity=total_quantity,
                    method="NetLiq",
                    order_type='MKT',
                    lmt_price=limit_price)
            )
            order.outsideRth = False

            log(f'Buy order for {ticker} totalQuantity={total_quantity} LmtPrice={limit_price}')
            contract = stock_contract(symbol=ticker, currency=__dbdata__.data.loc[ticker, 'currency'])
            self.placeOrder(self.next_order_id, contract, order)
            self.next_order_id += 1

    def nextValidId(self, order_id=None):
        lock = Lock()
        lock.acquire()
        try:
            if order_id is None:
                order_id = self.next_order_id
            super().nextValidId(order_id)
            self.next_order_id = order_id
            log(f"The next valid order id is: {self.next_order_id}")
        finally:
            lock.release()
        return order_id

    def contractDetails(self, req_id, contract_details):
        self.contract_details[req_id] = contract_details

    def get_contract_details(self, req_id, contract):
        self.contract_details[req_id] = None
        self.reqContractDetails(req_id, contract)
        # Error checking loop - breaks from loop once contract details are obtained
        for i in range(50):
            if not self.contract_details[req_id]:
                time.sleep(0.1)
            else:
                break
        # Raise if error checking loop count maxed out (contract details not obtained)
        if i == 49:
            log('error getting contract details', 'ERROR')
        # Return contract details otherwise
        return self.contract_details[req_id].contract

    def tws_data(self):
        data = self.__tws_data__
        data.set_index('Symbol', inplace=True, drop=True)

        def agg_positions(row):
            d = {'Quantity': row['Quantity'].sum(), 'AverageCost': row['Average Cost'].mean()}
            return pd.Series(d, index=['Quantity', 'AverageCost'])

        return data.groupby(['Symbol']).apply(agg_positions)

    def navs(self):
        data = self.__acc_summary__
        sum_nav = data['Value'].astype('float').sum()
        log(f'Calculate NAVS = {sum_nav}')
        return sum_nav


def start():
    app = IBApp(
        ip_address=__config__['connect']['ip_address'],
        ip_port=int(__config__['connect']['ip_port']),
        id_client=int(__config__['connect']['id_client'])
    )

    log(f"Handle for account summary")
    app.reqAccountSummary(0, __config__['account']['group_name'], __config__['account']['tags'])
    log(f"Handle for account positions")
    app.reqPositionsMulti(9006, __config__['account']['group_name'], __config__['account']['model_code'])

    time.sleep(float(__config__['others']['sleep']))

    return app


def stop(app):
    try:
        app.disconnect()
        log('Disconnect', 'INFO')
    except Exception as e:
        log(f'Getting error while disconnecting: {e}')


def init():
    __config__.read('config.ini')
    set_logger(
        'xProject.log',
        mode=__config__['logger']['mode'],
        level=__config__['logger']['level']
    )
    __dbdata__.csv_load(
        path=__config__['db_csv']['path'],
        index=__config__['db_csv']['index']
    )


def main():
    init()
    app = start()

    """
    place_order(
        ib_app=app,
        action='BUY',
        total_quantity=1000,
        order_type='MKT',
        limit_price=0,
        ticker="SPY"
    )
    """
    while True:
        pass # do something

    stop(app)


if __name__ == '__main__':
    main()
