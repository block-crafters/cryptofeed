'''
Copyright (C) 2017-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import logging
from decimal import Decimal


from cryptofeed.defines import OKEX_SWAP, BUY, SELL
from cryptofeed.exchange.okex import OKEx
from cryptofeed.standards import timestamp_normalize

LOG = logging.getLogger('feedhandler')


class OKExSwap(OKEx):
    id = OKEX_SWAP

    def __init__(self, pairs=None, channels=None, callbacks=None, **kwargs):
        super().__init__(pairs=pairs, channels=channels, callbacks=callbacks, **kwargs)
        self.address = 'wss://real.okex.com:8443/ws/v3'
        self.book_depth = 200

    def parse_order(self, data):
        """
        Reference:
            type(str): Type (1: open long 2: open short 3: close long 4: close short)
            contract_val(str): Contract value
            client_oid(str): the order ID customised by yourself
            order_type(str): 0: Normal limit order 1: Post only 2: Fill Or Kill 3: Immediatel Or Cancel 4：Market
            state(str): -2:Failed, -1:Canceled, 0:Open, 1:Partially Filled, 2:Fully Filled, 3:Submitting, 4:Canceling

            ex)
            {
                "table":"swap/order",
                "data":[
                    {
                        "algo_type":"",
                        "client_oid":"",
                        "contract_val":"10",
                        "error_code":"0",
                        "event_code":"",
                        "event_message":"",
                        "fee":"-0.000010",
                        "filled_qty":"1",
                        "instrument_id":"ETH-USD-SWAP",
                        "last_fill_id":"36648794",
                        "last_fill_px":"406.44",
                        "last_fill_qty":"1",
                        "last_fill_time":"2020-10-28T04:44:08.532Z",
                        "leverage":"4.00",
                        "order_id":"619937496542646272",
                        "order_side":"",
                        "order_type":"4",
                        "price":"398.37",
                        "price_avg":"406.44",
                        "size":"1",
                        "sl_price":"0.00",
                        "sl_trigger_price":"0.00",
                        "state":"2",
                        "status":"2",
                        "timestamp":"2020-10-28T04:44:08.532Z",
                        "tp_price":"0.00",
                        "tp_trigger_price":"0.00",
                        "type":"3"
                    }
                ]
            }
        """
        ts = timestamp_normalize(self.id, data['timestamp'])
        order = {
            'order_id': data['order_id'],
            'client_order_id': data.get('client_oid', ''),
            'timestamp': ts
        }
        if data.get('type'):
            # NOTE: type(str): Type (1: open long 2: open short 3: close long 4: close short)
            order['side'] = BUY if data.get('type') in ['1', '4'] else SELL
        if data.get('state'):
            order['status'] = self.parse_order_status(data['state'])

        # 0 should be True at `if` statement.
        if data.get('contract_val') is not None:
            if data.get('size') is not None:
                order['amount'] = float(Decimal(data.get('size')) * Decimal(data.get('contract_val')))
            if data.get('filled_qty') is not None:
                order['filled'] = float(Decimal(data.get('filled_qty')) * Decimal(data.get('contract_val')))
            if order.get('amount') is not None and order.get('filled') is not None:
                order['remaining'] = order['amount'] - order['filled']
        if data.get('price') is not None:
            order['price'] = float(data['price'])
        if data.get('price_avg') is not None:
            order['average'] = float(data.get('price_avg') or 0)

        return order
