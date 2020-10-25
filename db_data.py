from logger import log
import pandas as pd


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

    @property
    def iterrows(self):
        return self.data.iterrows

    def set_value(self, ticker, field, value):
        self.is_changed = True
        self.data.at[ticker, field] = value
        log(f'Change DB value {field} = {value} for {ticker}')

    def csv_load(self, path, index):
        self.data = pd.read_csv(path, sep=';', header=0).set_index(index)
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