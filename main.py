import pandas as pd

# портфель - то, что куплено в клиенте
# правила для торговли из БД

# TODO: logger

# request time to "wake-up" IB's API
def tws_time(client_id = 0, time_sleep = 0.5):
    from datetime import datetime
    from threading import Thread
    import time

    from ibapi.client import EClient
    from ibapi.wrapper import EWrapper
    from ibapi.common import TickerId

    class ib_class(EWrapper, EClient):
        def __init__(self, addr, port, client_id):
            EClient. __init__(self, self)
            self.connect(addr, port, client_id) # Connect to TWS
            print(f"connection to {addr}:{port} client = {client_id}")
            thread = Thread(target=self.run)  # Launch the client thread
            thread.start()

        def currentTime(self, cur_time):
            t = datetime.fromtimestamp(cur_time)
            print('Current TWS date/time: {}\n'.format(t))

        def error(self, reqId:TickerId, errorCode:int, errorString:str):
            if reqId > -1:
                print("Error. Id: " , reqId, " Code: " , errorCode , " Msg: " , errorString)

    ib_api = ib_class('127.0.0.1', 7497, client_id)
    ib_api.reqCurrentTime() # associated callback: currentTime
    time.sleep(time_sleep)
    ib_api.disconnect()

def read_positions(client_id = 10, time_sleep = 3.0): #read all accounts positions and return DataFrame with information

    from ibapi.client import EClient
    from ibapi.wrapper import EWrapper
    from ibapi.common import TickerId
    from threading import Thread

    import time

    class ib_class(EWrapper, EClient):

        def __init__(self, addr, port, client_id):
            EClient.__init__(self, self)

            self.connect(addr, port, client_id) # Connect to TWS
            thread = Thread(target=self.run)  # Launch the client thread
            thread.start()

            self.all_positions = pd.DataFrame([], columns = ['Account','Symbol', 'Quantity', 'Average Cost', 'Sec Type'])
            self.all_multipositions = pd.DataFrame([], columns=['Symbol', 'Quantity', 'Average Cost'])

        def error(self, reqId:TickerId, errorCode:int, errorString:str):
            if reqId > -1:
                print("Error. Id: " , reqId, " Code: " , errorCode , " Msg: " , errorString)

        def position(self, account, contract, pos, avgCost):
            index = str(account)+str(contract.symbol)
            self.all_positions.loc[index] = account, contract.symbol, pos, avgCost, contract.secType

        def positionEnd(self):
            super().positionEnd()
            print("PositionEnd")

        def positionMulti(self, reqId, account, modelCode, contract, pos, avgCost):
            super().positionMulti(reqId, account, modelCode, contract, pos, avgCost)
            print("PositionMulti. RequestId:", reqId, "Account:", account,
                  "ModelCode:", modelCode, "Symbol:", contract.symbol, "SecType:",
                  contract.secType, "Currency:", contract.currency, ",Position:",
                  pos, "AvgCost:", avgCost)
            index = str(account)+str(contract.symbol)
            self.all_multipositions.loc[index] = contract.symbol, pos, avgCost

        def positionMultiEnd(self, reqId: int):
            super().positionMultiEnd(reqId)
            print("PositionMultiEnd. RequestId:", reqId)

    ib_api = ib_class("127.0.0.1", 7497, client_id)
    # ib_api.reqPositions() # associated callback: position
    ib_api.reqPositionsMulti(9006, "IPO", "")

    print("Waiting for IB's API response for accounts positions requests...\n")
    time.sleep(time_sleep)
    current_positions = ib_api.all_multipositions
    current_positions.set_index('Symbol',inplace=True,drop=True) #set all_positions DataFrame index to "Account"

    ib_api.disconnect()

    return(current_positions)

def read_navs(client_id = 10, time_sleep = 3.0): #read all accounts NAVs

    from ibapi.client import EClient
    from ibapi.wrapper import EWrapper
    from ibapi.common import TickerId
    from threading import Thread

    import pandas as pd
    import time

    class ib_class(EWrapper, EClient):

        def __init__(self, addr, port, client_id):
            EClient.__init__(self, self)

            self.connect(addr, port, client_id) # Connect to TWS
            thread = Thread(target=self.run)  # Launch the client thread
            thread.start()

            self.all_accounts = pd.DataFrame([], columns = ['reqId','Account', 'Tag', 'Value' , 'Currency'])

        def error(self, reqId:TickerId, errorCode:int, errorString:str):
            if reqId > -1:
                print("Error. Id: " , reqId, " Code: " , errorCode , " Msg: " , errorString)

        def accountSummary(self, reqId, account, tag, value, currency):
            index = str(account)
            self.all_accounts.loc[index]=reqId, account, tag, value, currency

    ib_api = ib_class("127.0.0.1", 7497, client_id)
    ib_api.reqAccountSummary(0, "IPO", "NetLiquidation") # associated callback: accountSummary
    print("Waiting for IB's API response for NAVs requests...\n")
    time.sleep(time_sleep)
    current_nav = ib_api.all_accounts
    ib_api.disconnect()

    return current_nav


def get_portfolio():

    def agg_multipositions(row):
        d = {'Quantity': row['Quantity'].sum(), 'AverageCost': row['Average Cost'].mean()}
        return pd.Series(d, index=['Quantity', 'AverageCost'])

    positions = read_positions()
    return positions.groupby(['Symbol']).apply(agg_multipositions)

def get_groupnavs():
    navs = read_navs()
    return navs['Value'].astype('int64').sum()

def get_dbrules():
    pass

def get_list_for_buy():
    pass

def buy_order():
    pass

def callback_processing():
    # отсылка изменений
    pass

def check_connection():
    from ibapi.client import EClient
    from ibapi.wrapper import EWrapper

    class IBapi(EWrapper, EClient):
        def __init__(self):
            EClient.__init__(self, self)

    app = IBapi()
    app.connect('127.0.0.1', 7497, 123)
    app.run()

    #Uncomment this section if unable to connect
    #and to prevent errors on a reconnect
    import time
    time.sleep(2)
    app.disconnect()


def main():
    # 1. Получить портфель и БД
    # 1.1 Для всех из портфеля повесить обработчик
    # 2. Получить список на покупку
    # 3. Для этого списка реализовать покупку для каждой позиции
    # 3.1 Для каждой, которую получилось купить повесить обработчик
    pass





if __name__ == '__main__':
    main()
