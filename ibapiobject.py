from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.execution import ExecutionFilter

from scaffolding import *
from logger import log
from string import printable

import pandas as pd

import datetime
import queue


MAX_WAIT_SECONDS = 10


class IBAPIWrapper(EWrapper):
    """
    The wrapper deals with the action coming back from the IB gateway or TWS instance
    We override methods in EWrapper that will get called when this action happens, like currentTime
    Extra methods are added as we need to store the results in this object
    """
    def __init__(self):
        self._my_contract_details = {}
        self._my_requested_execution = {}

        # We set these up as we could get things coming along before we run an init
        self._my_executions_stream = queue.Queue()
        self._my_commission_stream = queue.Queue()
        self._my_open_orders = queue.Queue()

        # TWS data and NAVS
        self.__tws_data__ = pd.DataFrame([], columns=['Symbol', 'Quantity', 'Average Cost'])
        self.__acc_summary__ = pd.DataFrame([], columns=['reqId', 'Account', 'Tag', 'Value', 'Currency'])
        self.__tws_data_agg__ = None

    # error handling code
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

    def error(self, id, error_code, error_message):
        errormsg = "IB error id %d errorcode %d string %s" % (id, error_code, error_message)
        self._my_errors.put(errormsg)

        if error_code == 202:
            log(f'Order {id} canceled')
        elif id > -1:
            message = ''.join(filter(lambda x: x in set(printable), errormsg))
            log(f"Error. Id: {id}, code: {error_code} message: {message}", "ERROR")
            self._my_errors.put(message)

    # get contract details code
    def init_contractdetails(self, reqId):
        contract_details_queue = self._my_contract_details[reqId] = queue.Queue()
        return contract_details_queue

    def contractDetails(self, reqId, contractDetails):
        if reqId not in self._my_contract_details.keys():
            self.init_contractdetails(reqId)

        self._my_contract_details[reqId].put(contractDetails)

    def contractDetailsEnd(self, reqId):
        if reqId not in self._my_contract_details.keys():
            self.init_contractdetails(reqId)

        self._my_contract_details[reqId].put(FINISHED)

    # orders
    def init_open_orders(self):
        open_orders_queue = self._my_open_orders = queue.Queue()
        return open_orders_queue

    def orderStatus(self, orderId, status: str, filled: float,
                    remaining: float, avgFillPrice: float, permId: int,
                    parentId: int, lastFillPrice: float, clientId: int,
                    whyHeld: str, mktCapPrice: float):
        order_details = orderInformation(orderId, status=status, filled=filled,
                 avgFillPrice=avgFillPrice, permid=permId,
                 parentId=parentId, lastFillPrice=lastFillPrice, clientId=clientId,
                                         whyHeld=whyHeld)
        self._my_open_orders.put(order_details)

    def openOrder(self, orderId, contract, order, orderstate):
        """
        Tells us about any orders we are working now

        overriden method
        """
        order_details = orderInformation(orderId, contract=contract, order=order, orderstate=orderstate)
        self._my_open_orders.put(order_details)

    def openOrderEnd(self):
        """
        Finished getting open orders
        Overriden method
        """
        self._my_open_orders.put(FINISHED)

    """
        Getting account summary and multi positions
    """
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

    """ Executions and commissions

    requested executions get dropped into single queue: self._my_requested_execution[reqId]
    Those that arrive as orders are completed without a relevant reqId go into self._my_executions_stream
    All commissions go into self._my_commission_stream (could be requested or not)

    The *_stream queues are permanent, and init when the TestWrapper instance is created
    """

    def init_requested_execution_data(self, reqId):
        execution_queue = self._my_requested_execution[reqId] = queue.Queue()
        return execution_queue

    def access_commission_stream(self):
        ## Access to the 'permanent' queue for commissions
        return self._my_commission_stream

    def access_executions_stream(self):
        ## Access to the 'permanent' queue for executions
        return self._my_executions_stream

    def commissionReport(self, commreport):
        """
        This is called if

        a) we have submitted an order and a fill has come back
        b) We have asked for recent fills to be given to us

        However no reqid is ever passed

        overriden method

        :param commreport:
        :return:
        """
        commdata = execInformation(commreport.execId, Commission=commreport.commission,
                        commission_currency = commreport.currency,
                        realisedpnl = commreport.realizedPNL)


        ## there are some other things in commreport you could add
        ## make sure you add them to the .attributes() field of the execInformation class

        ## These always go into the 'stream' as could be from a request, or a fill thats just happened
        self._my_commission_stream.put(commdata)

    def execDetails(self, reqId, contract, execution):
        """
        This is called if

        a) we have submitted an order and a fill has come back (in which case reqId will be FILL_CODE)
        b) We have asked for recent fills to be given to us (reqId will be

        See API docs for more details
        """
        ## overriden method
        log(f'Order Executed: {reqId} {contract.symbol} {contract.secType} {contract.currency} {execution.execId}  {execution.orderId} {execution.shares} {execution.lastLiquidity}')

        execdata = execInformation(execution.execId, contract=contract,
                                   ClientId=execution.clientId, OrderId=execution.orderId,
                                   time=execution.time, AvgPrice=execution.avgPrice,
                                   AcctNumber=execution.acctNumber, Shares=execution.shares,
                                   Price = execution.price)

        ## there are some other things in execution you could add
        ## make sure you add them to the .attributes() field of the execInformation class

        reqId = int(reqId)

        ## We eithier put this into a stream if its just happened, or store it for a specific request
        if reqId == FILL_CODE:
            self._my_executions_stream.put(execdata)
        else:
            self._my_requested_execution[reqId].put(execdata)

    def execDetailsEnd(self, reqId):
        """
        No more orders to look at if execution details requested
        """
        self._my_requested_execution[reqId].put(FINISHED)

    # order ids
    def init_nextvalidid(self):
        orderid_queue = self._my_orderid_data = queue.Queue()
        return orderid_queue

    def nextValidId(self, orderId):
        """
        Give the next valid order id

        Note this doesn't 'burn' the ID; if you call again without executing the next ID will be the same

        If you're executing through multiple clients you are probably better off having an explicit counter

        """
        if getattr(self, '_my_orderid_data', None) is None:
            ## getting an ID which we haven't asked for
            ## this happens, IB server just sends this along occassionally
            self.init_nextvalidid()

        self._my_orderid_data.put(orderId)


class IBAPIClient(EClient):
    """
    The client method

    We don't override native methods, but instead call them from our own wrappers
    """
    def __init__(self, wrapper):
        ## Set up with a wrapper inside
        EClient.__init__(self, wrapper)

        self._market_data_q_dict = {}
        self._commissions=list_of_execInformation()

    def resolve_ib_contract(self, ibcontract, reqId=DEFAULT_GET_CONTRACT_ID):

        """
        From a partially formed contract, returns a fully fledged version

        :returns fully resolved IB contract
        """

        # Make a place to store the data we're going to return
        contract_details_queue = FinishableQueue(self.init_contractdetails(reqId))
        log("Getting full contract details from the server... ")
        self.reqContractDetails(reqId, ibcontract)
        # Run until we get a valid contract(s) or get bored waiting
        new_contract_details = contract_details_queue.get(timeout=MAX_WAIT_SECONDS)

        while self.wrapper.is_error():
            self.get_error()

        if contract_details_queue.timed_out():
            log("Exceeded maximum wait for wrapper to confirm finished - seems to be normal behaviour")

        if len(new_contract_details) == 0:
            log("Failed to get additional contract details: returning unresolved contract", "ERROR")
            return ibcontract

        if len(new_contract_details)>1:
            log("got multiple contracts using first one")

        new_contract_details = new_contract_details[0]
        resolved_ibcontract = new_contract_details.contract
        return resolved_ibcontract

    def get_next_brokerorderid(self):
        """
        Get next broker order id
        :return: broker order id, int; or TIME_OUT if unavailable
        """

        # Make a place to store the data we're going to return
        orderid_q = self.init_nextvalidid()
        # -1 is irrelevant apparently
        self.reqIds(-1)

        # Run until we get a valid contract(s) or get bored waiting
        try:
            brokerorderid = orderid_q.get(timeout=MAX_WAIT_SECONDS)
        except queue.Empty:
            log("Wrapper timeout waiting for broker orderid", "ERROR")
            brokerorderid = TIME_OUT

        while self.wrapper.is_error():
            self.get_error(timeout=MAX_WAIT_SECONDS)

        return brokerorderid

    # orders section
    def place_order(self, ibcontract, order, orderid = None):
        """
            Places an order
            Returns brokerorderid or None
        """
        # We can eithier supply our own ID or ask IB to give us the next valid one
        if orderid is None:
            log("Getting orderid from IB")
            orderid = self.get_next_brokerorderid()
            if orderid is TIME_OUT:
                msg = "Couldn't get an orderid from IB, and you didn't provide an orderid"
                log(msg, "ERROR")
                return None
        log(f"Using order id of {orderid}")

        # Note: It's possible if you have multiple traidng instances for orderids to be submitted out of sequence
        #   in which case IB will break

        # Place the order
        self.placeOrder(
            orderid,  # orderId,
            ibcontract,  # contract,
            order  # order
        )

        return orderid

    def any_open_orders(self):
        """
        Simple wrapper to tell us if we have any open orders
        """

        return len(self.get_open_orders()) > 0

    def get_open_orders(self):
        """
        Returns a list of any open orders
        """
        # store the orders somewhere
        open_orders_queue = FinishableQueue(self.init_open_orders())
        # You may prefer to use reqOpenOrders() which only retrieves orders for this client
        self.reqAllOpenOrders()

        # Run until we get a terimination or get bored waiting
        open_orders_list = list_of_orderInformation(open_orders_queue.get(timeout=(MAX_WAIT_SECONDS // 2)))

        while self.wrapper.is_error():
            self.get_error()

        if open_orders_queue.timed_out():
            log("Exceeded maximum wait for wrapper to confirm finished whilst getting orders")

        # open orders queue will be a jumble of order details, turn into a tidy dict with no duplicates
        open_orders_dict = open_orders_list.merged_dict()
        return open_orders_dict

    def get_executions_and_commissions(self, reqId=DEFAULT_EXEC_TICKER, execution_filter = ExecutionFilter()):
        """
        Returns a list of all executions done today with commission data
        """

        ## store somewhere
        execution_queue = FinishableQueue(self.init_requested_execution_data(reqId))

        ## We can change ExecutionFilter to subset different orders
        ## note this will also pull in commissions but we would use get_executions_with_commissions
        self.reqExecutions(reqId, execution_filter)

        ## Run until we get a terimination or get bored waiting
        MAX_WAIT_SECONDS = 10
        exec_list = list_of_execInformation(execution_queue.get(timeout = MAX_WAIT_SECONDS))

        while self.wrapper.is_error():
            self.get_error()

        if execution_queue.timed_out():
            log("Exceeded maximum wait for wrapper to confirm finished whilst getting exec / commissions")

        ## Commissions will arrive seperately. We get all of them, but will only use those relevant for us
        commissions = self._all_commissions()

        ## glue them together, create a dict, remove duplicates
        all_data = exec_list.blended_dict(commissions)

        return all_data


    def _recent_fills(self):
        """
        Returns any fills since we last called recent_fills

        :return: list of executions as execInformation objects
        """

        ## we don't set up a queue but access the permanent one
        fill_queue = self.access_executions_stream()

        list_of_fills=list_of_execInformation()

        while not fill_queue.empty():
            MAX_WAIT_SECONDS = 5
            try:
                next_fill = fill_queue.get(timeout=MAX_WAIT_SECONDS)
                list_of_fills.append(next_fill)
            except queue.Empty:
                ## corner case where Q emptied since we last checked if empty at top of while loop
                pass

        ## note this could include duplicates and is a list
        return list_of_fills


    def recent_fills_and_commissions(self):
        """
        Return recent fills, with commissions added in

        :return: dict of execInformation objects, keys are execids
        """

        recent_fills = self._recent_fills()
        commissions = self._all_commissions() ## we want all commissions

        ## glue them together, create a dict, remove duplicates
        all_data = recent_fills.blended_dict(commissions)

        return all_data


    def _recent_commissions(self):
        """
        Returns any commissions that are in the queue since we last checked

        :return: list of commissions as execInformation objects
        """

        ## we don't set up a queue, as there is a permanent one
        comm_queue = self.access_commission_stream()

        list_of_comm=list_of_execInformation()

        while not comm_queue.empty():
            MAX_WAIT_SECONDS = 5
            try:
                next_comm = comm_queue.get(timeout=MAX_WAIT_SECONDS)
                list_of_comm.append(next_comm)
            except queue.Empty:
                ## corner case where Q emptied since we last checked if empty at top of while loop
                pass

        ## note this could include duplicates and is a list
        return list_of_comm


    def _all_commissions(self):
        """
        Returns all commissions since we created this instance

        :return: list of commissions as execInformation objects
        """

        original_commissions = self._commissions
        latest_commissions = self._recent_commissions()

        all_commissions = list_of_execInformation(original_commissions + latest_commissions)

        self._commissions = all_commissions

        # note this could include duplicates and is a list
        return all_commissions


    def cancel_order(self, orderid):

        ## Has to be an order placed by this client. I don't check this here -
        ## If you have multiple IDs then you you need to check this yourself.

        self.cancelOrder(orderid)

        ## Wait until order is cancelled
        start_time=datetime.datetime.now()
        MAX_WAIT_TIME_SECONDS = 10

        finished = False

        while not finished:
            if orderid not in self.get_open_orders():
                ## finally cancelled
                finished = True

            if (datetime.datetime.now() - start_time).seconds > MAX_WAIT_TIME_SECONDS:
                log("Wrapper didn't come back with confirmation that order was cancelled!")
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
                log("Wrapper didn't come back with confirmation that all orders were cancelled!")
                finished = True

        ## return nothing
