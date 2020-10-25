from ibapi.order import Order
from ibapi.tag_value import TagValue
from ibapi.contract import Contract


class Stock:
    """
        Stock dictionary-class
    """
    Exchange = {'EN': 'SMART', 'HK': 'SEHK', 'JP': 'SMART'}
    Money = {'EN': 'USD', 'HK': 'HKD', 'JP': 'JPY'}


class SampleOrder:
    """
        Samples of orders
    """
    @staticmethod
    def stock_contract(symbol, sec_type='STK', stock='EN'):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = sec_type
        contract.exchange = Stock.Exchange[stock]
        contract.currency = Stock.Money[stock]
        return contract

    @staticmethod
    def create_order(action, total_quantity, method, group_name):
        order = Order()
        order.action = action
        order.totalQuantity = total_quantity
        order.faGroup = group_name
        order.faMethod = method
        order.transmit = True
        return order

    @staticmethod
    def create_order_buy(total_quantity, lmt_price, group_name):
        order = SampleOrder.create_order('BUY', total_quantity, "NetLiq", group_name)
        order.tif = 'DAY'
        order.orderType = 'LMT'
        order.outsideRth = True
        order.lmtPrice = lmt_price
        return order

    @staticmethod
    def create_order_lmt(action, percent, lmt_price, group_name="IPO"):
        order = SampleOrder.create_order(action, 0, "PctChange", group_name)
        order.orderType = "LMT"
        order.faPercentage = percent
        order.lmtPrice = lmt_price
        order.tif = 'GTC'
        return order

    @staticmethod
    def create_order_stp(action, percent, aux_price, group_name="IPO"):
        order = SampleOrder.create_order(action, 0, "PctChange", group_name)
        order.orderType = "STP"
        order.faPercentage = percent
        order.auxPrice = aux_price
        order.tif = 'GTC'
        return order

    @staticmethod
    def create_order_pct(action="SELL", total_quantity=0, percent="-100", tif='DAY', group_name="IPO"):
        order = SampleOrder.create_order(action, total_quantity, "PctChange", group_name)
        order.orderType = "MKT"
        order.faPercentage = percent
        order.totalQuantity = total_quantity
        order.tif = tif
        return SampleOrder.make_adaptive(order)

    @staticmethod
    def make_adaptive(order, priority="Normal"):
        order.algoStrategy = "Adaptive"
        order.algoParams = [TagValue("adaptivePriority", priority)]
        return order

