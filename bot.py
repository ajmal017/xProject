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
from configparser import ConfigParser
from string import printable
from copy import deepcopy
import pandas as pd
import numpy as np
import time
import datetime
import queue


# TODO comments

__config__ = ConfigParser()
__dbdata__ = DBData()


class TickPrice:
    BID = 1
    ASK = 2
    LAST = 4


class Stock:
    Exchange = {'EN': 'SMART', 'HK': 'SEHK', 'JP': 'SMART'}
    Money = {'EN': 'USD', 'HK': 'HKD', 'JP': 'JPY'}


## marker for when queue is finished
FINISHED = object()
STARTED = object()
TIME_OUT = object()


class FinishableQueue(object):
    """
    Creates a queue which will finish at some point
    """

    def __init__(self, queue_to_finish):
        self._queue = queue_to_finish
        self.status = STARTED

    def get(self, timeout):
        """
        Returns a list of queue elements once timeout is finished, or a FINISHED flag is received in the queue

        :param timeout: how long to wait before giving up
        :return: list of queue elements
        """
        contents_of_queue = []
        finished = False

        while not finished:
            try:
                current_element = self._queue.get(timeout=timeout)
                if current_element is FINISHED:
                    finished = True
                    self.status = FINISHED
                else:
                    contents_of_queue.append(current_element)
                    ## keep going and try and get more data

            except queue.Empty:
                ## If we hit a time out it's most probable we're not getting a finished element any time soon
                ## give up and return what we have
                finished = True
                self.status = TIME_OUT

        return contents_of_queue

    def timed_out(self):
        return self.status is TIME_OUT


## marker to show a mergable object hasn't got any attributes
NO_ATTRIBUTES_SET=object()

class mergableObject(object):
    """
    Generic object to make it easier to munge together incomplete information about orders and executions
    """

    def __init__(self, id, **kwargs):
        """

        :param id: master reference, has to be an immutable type
        :param kwargs: other attributes which will appear in list returned by attributes() method
        """

        self.id=id
        attr_to_use=self.attributes()

        for argname in kwargs:
            if argname in attr_to_use:
                setattr(self, argname, kwargs[argname])
            else:
                print("Ignoring argument passed %s: is this the right kind of object? If so, add to .attributes() method" % argname)

    def attributes(self):
        ## should return a list of str here
        ## eg return ["thingone", "thingtwo"]
        return NO_ATTRIBUTES_SET

    def _name(self):
        return "Generic Mergable object - "

    def __repr__(self):
        attr_list = self.attributes()
        if attr_list is NO_ATTRIBUTES_SET:
            return self._name()

        return self._name()+" ".join([ "%s: %s" % (attrname, str(getattr(self, attrname))) for attrname in attr_list
                                                  if getattr(self, attrname, None) is not None])

    def merge(self, details_to_merge, overwrite=True):
        """
        Merge two things

        self.id must match

        :param details_to_merge: thing to merge into current one
        :param overwrite: if True then overwrite current values, otherwise keep current values
        :return: merged thing
        """

        if self.id!=details_to_merge.id:
            raise Exception("Can't merge details with different IDS %d and %d!" % (self.id, details_to_merge.id))

        arg_list = self.attributes()
        if arg_list is NO_ATTRIBUTES_SET:
            ## self is a generic, empty, object.
            ## I can just replace it wholesale with the new object

            new_object = details_to_merge

            return new_object

        new_object = deepcopy(self)

        for argname in arg_list:
            my_arg_value = getattr(self, argname, None)
            new_arg_value = getattr(details_to_merge, argname, None)

            if new_arg_value is not None:
                ## have something to merge
                if my_arg_value is not None and not overwrite:
                    ## conflict with current value, don't want to overwrite, skip
                    pass
                else:
                    setattr(new_object, argname, new_arg_value)

        return new_object


class list_of_mergables(list):
    """
    A list of mergable objects, like execution details or order information
    """
    def merged_dict(self):
        """
        Merge and remove duplicates of a stack of mergable objects with unique ID

        Essentially creates the union of the objects in the stack

        :return: dict of mergableObjects, keynames .id
        """

        ## We create a new stack of order details which will contain merged order or execution details
        new_stack_dict = {}

        for stack_member in self:
            id = stack_member.id

            if id not in new_stack_dict.keys():
                ## not in new stack yet, create a 'blank' object
                ## Note this will have no attributes, so will be replaced when merged with a proper object
                new_stack_dict[id] = mergableObject(id)

            existing_stack_member = new_stack_dict[id]

            ## add on the new information by merging
            ## if this was an empty 'blank' object it will just be replaced with stack_member
            new_stack_dict[id] = existing_stack_member.merge(stack_member)

        return new_stack_dict


    def blended_dict(self, stack_to_merge):
        """
        Merges any objects in new_stack with the same ID as those in the original_stack

        :param self: list of mergableObject or inheritors thereof
        :param stack_to_merge: list of mergableObject or inheritors thereof

        :return: dict of mergableObjects, keynames .id
        """

        ## We create a new dict stack of order details which will contain merged details

        new_stack = {}

        ## convert the thing we're merging into a dictionary
        stack_to_merge_dict = stack_to_merge.merged_dict()

        for stack_member in self:
            id = stack_member.id
            new_stack[id] = deepcopy(stack_member)

            if id in stack_to_merge_dict.keys():
                ## add on the new information by merging without overwriting
                new_stack[id] = stack_member.merge(stack_to_merge_dict[id], overwrite=False)

        return new_stack


class orderInformation(mergableObject):
    """
    Collect information about orders

    master ID will be the orderID

    eg you'd do order_details = orderInformation(orderID, contract=....)
    """

    def _name(self):
        return "Order - "

    def attributes(self):
        return ['contract','order','orderstate','status',
                 'filled', 'remaining', 'avgFillPrice', 'permid',
                 'parentId', 'lastFillPrice', 'clientId', 'whyHeld']


def stock_contract(symbol, sec_type='STK', stock='EN'):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = sec_type
    contract.exchange = Stock.Exchange[stock]
    contract.currency = Stock.Money[stock]
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


def create_order_lmt(action, percent, lmt_price, group_name="IPO"):
    order = create_order(action, 0, "PctChange", group_name)
    order.orderType = "LMT"
    order.faPercentage = percent
    order.lmtPrice = lmt_price
    order.tif = 'GTC'
    return order


def create_order_stp(action, percent, aux_price, group_name="IPO"):
    order = create_order(action, 0, "PctChange", group_name)
    order.orderType = "STP"
    order.faPercentage = percent
    order.auxPrice = aux_price
    order.tif = 'GTC'
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


class IBApp(EWrapper, EClient):
    def __init__(self, ip_address, ip_port, id_client):
        EClient.__init__(self, self)
        # init fields
        self.is_connect = True
        self.__tws_data__ = pd.DataFrame([], columns=['Symbol', 'Quantity', 'Average Cost'])
        self.__acc_summary__ = pd.DataFrame([], columns=['reqId', 'Account', 'Tag', 'Value', 'Currency'])
        self.__tws_data_agg__ = None
        self.closed_tickers = {}
        self.next_order_id = None
        self.contract_details = {}
        self.reqid_ticker = {}
        self.ticker_reqid = {}
        self._my_open_orders = queue.Queue()
        self.lock = Lock()
        self.init_error()

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

    def init_error(self):
        error_queue = queue.Queue()
        self._my_errors = error_queue

    def get_error(self, timeout=5):
        if self.is_error():
            try:
                return self._my_errors.get(timeout=timeout)
            except queue.Empty:
                return None
        return None

    def is_error(self):
        an_error_if = not self._my_errors.empty()
        return an_error_if

    def error(self, req_id, code, message):
        if code == 202:
            log('Order canceled', 'ERROR')
        elif req_id > -1:
            message = ''.join(filter(lambda x: x in set(printable), message))
            log(f"Error. Id: {req_id}, code: {code} message: {message}", "ERROR")
            self._my_errors.put(message)

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
            self.__tws_data_agg__ = None

    def check_risk(self, ticker):
        ticker_row = __dbdata__.data.loc[ticker]
        contract = stock_contract(ticker, stock=ticker_row['stock'])
        contract = self.get_contract_details(ticker_row['request_id'], contract)
        self.reqid_ticker[ticker_row['request_id']] = ticker
        self.ticker_reqid[ticker] = ticker_row['request_id']
        self.reqMktData(ticker_row['request_id'], contract, '', False, False, [])

    def cancel_orders_for_ticker(self, ticker):
        open_orders = self.get_open_orders()
        for order_id in open_orders:
            if open_orders[order_id].contract.symbol == ticker:
                self.cancel_order(order_id)

    def udpate_levels(self, ticker, avg_price: float, last_price):
        # request all open orders for current client id
        self.cancel_orders_for_ticker(ticker)

        db_row = __dbdata__.data.loc[ticker]

        # set new orders
        # UP
        for level in range(1, 5):
            if db_row['level_is_active_%d' % level]:
                lmt_price = round(avg_price * (1 + float(db_row['trigger_profit_up%d' % level])), 2)
                up_order = create_order_lmt(
                    action='SELL',
                    percent=-100*db_row['v_trig_up%d' % level],
                    lmt_price=lmt_price,
                    group_name=db_row['group_name']
                )
                self.place_order(ticker=ticker, order=up_order, stock=db_row['stock'])
        # DOWN
        stop_price = round(avg_price * (1 - float(db_row['stop_price_start'])), 2)
        up_order = create_order_stp(
            action='SELL',
            percent=-100,
            aux_price=stop_price,
            group_name=db_row['group_name']
        )
        self.place_order(ticker=ticker, order=up_order, stock=db_row['stock'])


    def tickPrice(self, req_id, tick_type, price, attrib):
        # TODO cancel orders for not active ticker
        if tick_type == TickPrice.LAST:
            ticker = self.reqid_ticker[req_id]
            log(f'The last price for {ticker} request_id = {req_id} is: {price}')
            tws_data = self.get_tws_data()

            tws_row = tws_data.loc[ticker]
            initial_cost = tws_row['Quantity'] * tws_row['AverageCost']
            current_cost = tws_row['Quantity'] * price
            delta_cost = current_cost - initial_cost

            db_row = __dbdata__.data.loc[ticker]
            group_nav = self.navs()
            risk_value = group_nav * float(db_row['risk_check'])
            log(f'For ticker {ticker} calculated delta_cost: {delta_cost}, risk_value: {risk_value}')

            # is risk exceed?
            if delta_cost > -1. * risk_value:
                self.lock.acquire()
                try:
                    if ticker not in self.closed_tickers and (tws_row['Quantity'] > 0):
                        order = make_adaptive(create_order_pct(group_name=db_row['group_name']))
                        order.orderType = "MKT"
                        self.closed_tickers[ticker] = self.place_order(ticker=ticker, order=order, stock=db_row['stock'])
                        log(f'Place order for close ticker {ticker}')
                finally:
                    log(f'Released a lock {ticker}')
                    self.lock.release()
            else:
                self.udpate_levels(ticker, tws_row['AverageCost'], price)

    # orders section
    def place_order(self, ticker, order, stock):
        contract = stock_contract(ticker, stock=stock)
        """
        req_id = self.next_order_id
        self.next_order_id += 1
        try:
            contract = self.get_contract_details(req_id, contract)
        except Exception as e:
            print(contract)
            log(f'message: {e} contract: {contract}')
        """
        # order_id = ib_app.nextValidId() # TODO Check nextValidID
        order_id = self.next_order_id
        self.next_order_id += 1
        self.placeOrder(order_id, contract, order)
        return order_id

    def get_open_orders(self):
        """
        Returns a list of any open orders
        """
        open_orders_queue = FinishableQueue(self.init_open_orders()) # store the orders somewhere
        self.reqAllOpenOrders()
        MAX_WAIT_SECONDS = 5 # Run until we get a terimination or get bored waiting
        open_orders_list = list_of_mergables(open_orders_queue.get(timeout=MAX_WAIT_SECONDS))

        while self.wrapper.is_error():
            print(self.get_error())

        if open_orders_queue.timed_out():
            print("Exceeded maximum wait for wrapper to confirm finished whilst getting orders")
        ## open orders queue will be a jumble of order details, turn into a tidy dict with no duplicates
        open_orders_dict = open_orders_list.merged_dict()
        return open_orders_dict

    def init_open_orders(self):
        open_orders_queue = self._my_open_orders = queue.Queue()
        return open_orders_queue

    def cancel_order(self, orderid):
        ## Has to be an order placed by this client. I don't check this here -
        ## If you have multiple IDs then you you need to check this yourself.
        self.cancelOrder(orderid)
        ## Wait until order is cancelled
        start_time = datetime.datetime.now()
        MAX_WAIT_TIME_SECONDS = 10
        finished = False
        while not finished:
            if orderid not in self.get_open_orders():
                ## finally cancelled
                finished = True
            if (datetime.datetime.now() - start_time).seconds > MAX_WAIT_TIME_SECONDS:
                print("Wrapper didn't come back with confirmation that order was cancelled!")
                finished = True
        ## return nothing

    def cancel_all_orders(self):
        ## Cancels all orders, from all client ids.
        ## if you don't want to do this, then instead run .cancel_order over named IDs
        self.reqGlobalCancel()
        start_time = datetime.datetime.now()
        MAX_WAIT_TIME_SECONDS = 10
        finished = False
        while not finished:
            if not self.any_open_orders():
                ## all orders finally cancelled
                finished = True
            if (datetime.datetime.now() - start_time).seconds > MAX_WAIT_TIME_SECONDS:
                print("Wrapper didn't come back with confirmation that all orders were cancelled!")
                finished = True
        ## return nothing

    def any_open_orders(self):
        """
        Simple wrapper to tell us if we have any open orders
        """
        return len(self.get_open_orders()) > 0

    def orderStatus(self, orderId, status: str, filled: float,
                    remaining: float, avgFillPrice: float, permId: int,
                    parentId: int, lastFillPrice: float, clientId: int,
                    whyHeld: str, mktCapPrice: float):
        log(f'orderStatus - orderid: {orderId} status: {status} filled: {filled} remaining: {remaining} lastFillPrice: {lastFillPrice}')
        order_details = orderInformation(orderId, status=status, filled=filled,
                                         avgFillPrice=avgFillPrice, permid=permId,
                                         parentId=parentId, lastFillPrice=lastFillPrice, clientId=clientId,
                                         whyHeld=whyHeld)
        self._my_open_orders.put(order_details)

    def openOrder(self, orderId, contract, order, orderState):
        log(f'openOrder id: {orderId} {contract.symbol} {contract.secType}  @ {contract.exchange} : {order.action} {order.orderType} {order.totalQuantity} {orderState.status}')
        order_details = orderInformation(orderId, contract=contract, order=order, orderstate=orderState)
        self._my_open_orders.put(order_details)

    def openOrderEnd(self):
        self._my_open_orders.put(FINISHED)

    def execDetails(self, reqId, contract, execution):
        super().execDetails(reqId, contract, execution)
        log(f'Order Executed: {reqId} {contract.symbol} {contract.secType} {contract.currency} {execution.execId}  {execution.orderId} {execution.shares} {execution.lastLiquidity}')
        ticker = contract.symbol

        # TODO закрытый тикер установить is_active => false

        """
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
                    lmt_price=limit_price)
            )
            order.outsideRth = False

            log(f'Buy order for {ticker} totalQuantity={total_quantity} LmtPrice={limit_price}')
            contract = stock_contract(symbol=ticker, stock=__dbdata__.data.loc[ticker, 'stock'])
            self.placeOrder(self.next_order_id, contract, order)
            self.next_order_id += 1
        """

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
            log(f'error getting contract details {req_id}', 'ERROR')
        else:
            self.contract_details[req_id].contract = contract
        # Return contract details otherwise
        return self.contract_details[req_id].contract

    def get_tws_data(self):

        def agg_positions(row):
            d = {'Quantity': row['Quantity'].sum(), 'AverageCost': row['Average Cost'].mean()}
            return pd.Series(d, index=['Quantity', 'AverageCost'])

        if self.__tws_data_agg__ is None:
            self.__tws_data_agg__ = self.__tws_data__.copy(deep=True)
            self.__tws_data_agg__.set_index('Symbol', inplace=True, drop=True)

        return self.__tws_data_agg__.groupby(['Symbol']).apply(agg_positions)

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


def save_csv():
    __dbdata__.csv_save()


def load_csv():
    __dbdata__.csv_load(
        path=__config__['db_csv']['path'],
        index=__config__['db_csv']['index']
    )


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
    load_csv()


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

    tws_data = app.get_tws_data()
    for ticker, row in tws_data.iterrows():
        app.check_risk(ticker)

    tws_row = tws_data.loc[ticker]
    app.udpate_levels('SPY', tws_row['AverageCost'], 0)

    while False:
        # time.sleep(5)
        pass
    stop(app)


if __name__ == '__main__':
    main()
