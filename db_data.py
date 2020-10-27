from logger import log
import pandas as pd
import numpy as np


__row_types__ = {
    'ticker_name': str,
    'group_name': str,
    'stock': str,

    'request_id': int,
    'first_order_id': int,
    'first_order_quantity': int,
    'is_active': int,

    'level_is_active1': int,
    'level_is_active2': int,
    'level_is_active3': int,
    'level_is_active4': int,

    'book_price': float,
    'currency_value': float,
    'allocation': float,
    'risk_check': float,
    'first_price': float,
    'max_price': float,

    'v1_buy_alloc': float,
    'v2_buy_alloc': float,
    'v3_buy_alloc': float,

    'trigger_profit_up1': float,
    'trigger_profit_up2': float,
    'trigger_profit_up3': float,
    'trigger_profit_up4': float,

    'trigger_stop_up_1': float,
    'trigger_stop_up_2': float,
    'trigger_stop_up_3': float,
    'trigger_stop_up_4': float,

    'v_trig_up1': float,
    'v_trig_up2': float,
    'v_trig_up3': float,
    'v_trig_up4': float,

    'day_to_auto_close': int,
    'time_to_cond2': int,

    'stop_price_0': float,
    'stop_price_1': float,
    'stop_price_2': float,
    'stop_price_3': float,
    'stop_price_4': float,

    'stop_is_active_0': int,
    'stop_is_active_1': int,
    'stop_is_active_2': int,
    'stop_is_active_3': int,
    'stop_is_active_4': int,
}


class DBData:
    """
        Load data from external source (e.g. CSV, SQL)
    """
    def __init__(self):
        self.data = None
        self.__csv_path__ = ""
        self.is_changed = False

    def ticker(self, name):
        if self.data is not None and name in self.data.index:
            return self.data.loc[name]
        else:
            log(f'Error getting row by ticker name {name}', 'ERROR')

    def iterrows(self):
        return self.data.iterrows()

    def set_value(self, ticker, field, value):
        self.is_changed = True
        self.data.at[ticker, field] = value
        log(f'Change DB value {field} = {value} for {ticker}')

    def csv_load(self, path, index):
        self.data = pd.read_csv(
            path,
            sep=';',
            header=0,
            dtype=__row_types__
        ).set_index(index)
        self.__csv_path__ = path
        self.data['first_price_time'] = pd.to_datetime(self.data['first_price_time'])
        log(f'Load from csv, {len(self.data.index)} rows', 'INFO')

    def csv_save(self):
        self.data.to_csv(self.__csv_path__, index=True, header=True, sep=';')
        log(f'Save to csv, {len(self.data.index)} rows', 'INFO')

    def sql_load(self):
        # TODO SQL LOAD
        pass

    def sql_save(self):
        # TODO SQL SAVE
        pass


if __name__ == '__main__':
    print('Do nothing')