# MY
from logger import set_logger, log
from db_data import DBData
from sampleorder import SampleOrder
from ibapiobject import IBAPIWrapper, IBAPIClient
from scaffolding import TickPrice, FILL_CODE

# COMMON

from threading import Thread, Lock
from configparser import ConfigParser

import pandas as pd
import time
import datetime


__config__ = ConfigParser()
__dbdata__ = DBData()


def is_time_coming(db_row: pd.Series):
    """
        Check condition if time is coming
    """
    import numpy as np
    return datetime.datetime.now() < db_row['first_price_time'] + np.timedelta64(db_row['time_to_cond2'], 's')


class IBApp(IBAPIWrapper, IBAPIClient):
    """
        Class for running application
    """
    def __init__(self, ip_address, ip_port, id_client):
        IBAPIWrapper.__init__(self)
        IBAPIClient.__init__(self, wrapper=self)

        # Init fields
        self.contract_details = {}
        self.reqid_ticker = {}
        self.ticker_reqid = {}
        self.lock = Lock()
        self.closed_tickers = {}
        self.next_order_id = None

        # Connect to TWS and launch client thread
        self.connect(ip_address, ip_port, id_client)
        log(f"connection to {ip_address}:{ip_port} client = {id_client}", "INFO")
        self.thread = Thread(target=self.run, daemon=True)
        self.thread.start()
        setattr(self, "_thread", self.thread)
        self.init_error()

        # Check if the API is connected via next_order_id
        while not self.isConnected():
            log('Waiting for connection...')
            time.sleep(0.5)
        log('API connected')

    def get_tws_data(self):
        """
            Aggregate multi positions
        """
        def agg_positions(row):
            d = {'Quantity': row['Quantity'].sum(), 'AverageCost': row['Average Cost'].mean()}
            return pd.Series(d, index=['Quantity', 'AverageCost'])

        if self.__tws_data_agg__ is None:
            self.__tws_data_agg__ = self.__tws_data__.copy(deep=True)
            self.__tws_data_agg__.set_index('Symbol', inplace=True, drop=True)

        return self.__tws_data_agg__.groupby(['Symbol']).apply(agg_positions)

    def navs(self):
        """
            Get account summary
        """
        data = self.__acc_summary__
        sum_nav = data['Value'].astype('float').sum()
        log(f'Calculate NAVS = {sum_nav}')
        return sum_nav

    def buy_or_sell(self, ticker, total_quantity):
        """
            Buy (or sell) total quantity for ticker as MKT, adaptive strategy
        """
        db_row = __dbdata__.ticker(ticker)

        order = SampleOrder.create_order_buy(
            total_quantity=total_quantity,
            group_name=db_row['group_name']
        )
        order.orderType = 'MKT'
        order = SampleOrder.make_adaptive(order, "Normal")
        order.outsideRth = False

        if total_quantity < 0:
            order.action = 'SELL'

        log(f'{order.action} order for {ticker} totalQuantity={total_quantity}')

        contract = SampleOrder.stock_contract(
            symbol=ticker,
            stock=db_row['stock']
        )

        return self.place_order(contract, order)

    def execDetails(self, reqId, contract, execution):
        """
            Fill has come back for order_id
        """
        super().execDetails(reqId, contract, execution)

        ticker = contract.symbol
        if ticker not in __dbdata__.data.index:
            return

        reqId = int(reqId)
        db_row = __dbdata__.ticker(ticker)

        # check if fill first order
        if (    reqId == FILL_CODE
                and db_row['first_order_quantity'] == execution.cumQty
                and execution.orderId == db_row['first_order_id'] ):

            group_nav = self.navs()
            total_quantity = group_nav * db_row['allocation'] * db_row['v1_buy_alloc'] / db_row['first_price']
            total_quantity = int(round(total_quantity, 0)) - execution.cumQty

            self.buy_or_sell(
                ticker=ticker,
                total_quantity=total_quantity
            )
            self.update_levels(ticker, db_row['first_price'])

        elif reqId == FILL_CODE and execution.orderRef == 'STOP':
            __dbdata__.set_value(ticker, "is_active", 0)
            
        elif reqId == FILL_CODE and execution.orderRef == 'STAGE3':
            self.update_levels(ticker, db_row['first_price'])

    def tickPrice(self, req_id, tick_type, price, attrib):
        """
            Processing price changing
        """
        if tick_type == TickPrice.LAST:
            ticker = self.reqid_ticker[req_id]
            log(f'The last price for {ticker} request_id = {req_id} is: {price}')
            tws_data = self.get_tws_data()

            tws_row = tws_data.loc[ticker]
            initial_cost = tws_row['Quantity'] * tws_row['AverageCost']
            current_cost = tws_row['Quantity'] * price
            delta_cost = current_cost - initial_cost

            group_nav = self.navs()

            db_row = __dbdata__.ticker(ticker)

            if db_row['first_price'] == 0:
                __dbdata__.set_value(ticker, 'first_price', price)
                __dbdata__.set_value(ticker, 'first_price_time', datetime.datetime.now())
                __dbdata__.set_value(ticker, 'max_price', price)
            elif not is_time_coming(db_row):
                if db_row['max_price'] < price:
                    __dbdata__.set_value(ticker, 'max_price', price)
            else:
                if price <= 1.4 * db_row['first_price']:
                    # second stage
                    if price > db_row['first_price']:
                        total_quantity = group_nav * db_row['allocation'] * db_row['v2_buy_alloc'] / price
                        self.buy_or_sell(
                            ticker=ticker,
                            total_quantity=total_quantity
                        )
                        self.update_levels(ticker, db_row['first_price'])

                    # third stage
                    order = SampleOrder.make_adaptive(
                        SampleOrder.create_order_stp(
                            action='BUY',
                            percent=0,
                            aux_price=db_row['max_price']
                        )
                    )
                    order.faMethod = "NetLiq"
                    order.totalQuantity = group_nav*db_row['allocation']*db_row['v3_buy_alloc']/price
                    contract = SampleOrder.stock_contract(ticker, stock=db_row['stock'])
                    order.orderRef = "STAGE3"
                    self.place_order(ibcontract=contract, order=order)

            risk_value = group_nav * float(db_row['risk_check'])
            log(f'For ticker {ticker} calculated delta_cost: {delta_cost}, risk_value: {risk_value}')

            # check flags for triggers
            for level in range(4):
                is_active_name = f'stop_is_active_{level}'
                trigger_stop = f'trigger_stop_up_{level}'
                if db_row[is_active_name] and price > tws_row['AverageCost'] * (1 + db_row[trigger_stop]):
                    __dbdata__.set_value(ticker, is_active_name, 0)

            # is risk exceed?
            if delta_cost > -1. * risk_value:
                self.lock.acquire()
                try:
                    if ticker not in self.closed_tickers and (tws_row['Quantity'] > 0):
                        order = SampleOrder.create_order_pct(group_name=db_row['group_name'])
                        contract = SampleOrder.stock_contract(ticker, stock=db_row['stock'])
                        self.closed_tickers[ticker] = self.place_order(contract, order)
                        log(f'Place order for close ticker {ticker}')
                finally:
                    log(f'Released a lock {ticker}')
                    self.lock.release()
            else:
                self.update_levels(ticker, db_row['first_price'])

    def send_request_market_data(self, ticker):
        ticker_row = __dbdata__.ticker(ticker)
        contract = SampleOrder.stock_contract(ticker, stock=ticker_row['stock'])
        contract = self.resolve_ib_contract(contract)
        self.reqid_ticker[ticker_row['request_id']] = ticker
        self.ticker_reqid[ticker] = ticker_row['request_id']
        self.reqMktData(ticker_row['request_id'], contract, '', False, False, [])

    def initial_buy(self, ticker):
        db_row = __dbdata__.ticker(ticker)
        contract = SampleOrder.stock_contract(ticker, stock=db_row['stock'])
        contract = self.resolve_ib_contract(contract)
        group_nav = self.navs()

        lmt_price = round(float(db_row['book_price'] * 2), 2)
        order = SampleOrder.create_order_buy(
            total_quantity=int(group_nav * db_row['allocation'] * db_row['v1_buy_alloc'] / lmt_price),
            lmt_price=lmt_price,
            group_name=db_row['group_name']
        )
        order_id = self.place_order(contract, order)
        if order_id is not None:
            __dbdata__.set_value(ticker, 'first_order_id', order_id)

    def cancel_orders_for_ticker(self, ticker):
        open_orders = self.get_open_orders()
        for order_id in open_orders:
            if open_orders[order_id].contract.symbol == ticker:
                self.cancel_order(order_id)

    def update_levels(self, ticker, first_price: float):
        # request all open orders for current client id
        self.cancel_orders_for_ticker(ticker)

        db_row = __dbdata__.ticker(ticker)

        # set new orders
        # UP
        contract = SampleOrder.stock_contract(ticker, stock=db_row['stock'])
        for level in range(1, 5):
            if db_row['level_is_active_%d' % level]:
                lmt_price = round(first_price * (1 + float(db_row['trigger_profit_up%d' % level])), 2)
                up_order = SampleOrder.create_order_lmt(
                    action='SELL',
                    percent=-100*db_row['v_trig_up%d' % level],
                    lmt_price=lmt_price,
                    group_name=db_row['group_name']
                )
                self.place_order(ibcontract=contract, order=up_order)
        # DOWN
        for level in range(5):
            if db_row[f'stop_is_active_{level}']:
                stop_price = round(first_price * (1 + float(db_row[f'stop_price_{level}'])), 2)
                down_order = SampleOrder.create_order_stp(
                    action='SELL',
                    percent=-100,
                    aux_price=stop_price,
                    group_name=db_row['group_name']
                )
                down_order.orderRef = 'STOP'
                self.place_order(ibcontract=contract, order=down_order)
                break


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


def save_db():
    __dbdata__.csv_save()
    log("DB saved")


def load_db():
    __dbdata__.csv_load(
        path=__config__['db_csv']['path'],
        index=__config__['db_csv']['index']
    )


def update_db(timeout=5):
    """
        Auto check if db-object was changed and run save method by timeout
    """

    def check_for_save():
        while True:
            time.sleep(timeout)
            if __dbdata__.is_changed:
                __dbdata__.is_changed = False
                save_db()

    thread = Thread(target=check_for_save, daemon=True)
    thread.start()


def stop(app):
    try:
        app.disconnect()
        log('Disconnect', 'INFO')
        time.sleep(2 * int(__config__['db_csv']['timeout']))
    except Exception as e:
        log(f'Getting error while disconnecting: {e}')


def init():
    """
        Activate object for working: __config__, __db_data__
    """
    __config__.read('config.ini')
    set_logger(
        'xProject.log',
        mode=__config__['logger']['mode'],
        level=__config__['logger']['level']
    )
    load_db()
    update_db(int(__config__['db_csv']['timeout']))


def main():
    init()
    # start IB application
    app = start()

    # check risk for positions from tws
    tws_data = app.get_tws_data()
    for ticker, tws_row in tws_data.iterrows:
        app.send_request_market_data(ticker)

    # processing positions from DB
    for ticker, db_row in __dbdata__.iterrows:
        if db_row['is_active']:
            if ticker not in tws_data.index:
                # first buy
                app.initial_buy(ticker)
                # set handle
                app.send_request_market_data(ticker)
        else:
            app.cancel_orders_for_ticker(ticker)

    input('Press Enter for stop')
    stop(app)


if __name__ == '__main__':
    main()
