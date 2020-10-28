'''
Copyright (C) 2017-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import asyncio
import json
import re
import logging
from decimal import Decimal
import os
import zlib

from sortedcontainers import SortedDict as sd

from cryptofeed.feed import Feed
from cryptofeed.defines import TRADES, BUY, SELL, BID, ASK, TICKER, L2_BOOK, OKCOIN, ORDER, ORDER_SWAP
from cryptofeed.standards import pair_exchange_to_std, timestamp_normalize
from cryptofeed.rest.okex import OKEx as RestOKEx


LOG = logging.getLogger('feedhandler')


class OKCoin(Feed):
    id = OKCOIN
    table_prefixs = ['spot']

    def __init__(self, pairs=None, channels=None, callbacks=None, **kwargs):
        super().__init__('wss://real.okcoin.com:8443/ws/v3', pairs=pairs, channels=channels, callbacks=callbacks, **kwargs)
        self.book_depth = 200

        if self.use_private_channels:
            self.key_id = os.environ.get('OKEX_API_KEY')
            self.key_secret = os.environ.get('OKEX_SECRET_KEY')
            self.passphrase = os.environ.get('OKEX_PASSWORD')

    def __reset(self):
        self.l2_book = {}

    async def subscribe(self, websocket):
        if self.use_private_channels:
            # wait for login
            await asyncio.sleep(0.5)
        self.__reset()
        if self.config:
            for chan in self.config:
                args = [f"{chan}:{pair}" for pair in self.config[chan]]
                await websocket.send(json.dumps({
                        "op": "subscribe",
                        "args": args
                    }))
        else:
            chans = [f"{chan}:{pair}" for chan in self.channels for pair in self.pairs]
            await websocket.send(json.dumps({
                                    "op": "subscribe",
                                    "args": chans
                                }))

    async def _ticker(self, msg):
        """
        {'table': 'spot/ticker', 'data': [{'instrument_id': 'BTC-USD', 'last': '3977.74', 'best_bid': '3977.08', 'best_ask': '3978.73', 'open_24h': '3978.21', 'high_24h': '3995.43', 'low_24h': '3961.02', 'base_volume_24h': '248.245', 'quote_volume_24h': '988112.225861', 'timestamp': '2019-03-22T22:26:34.019Z'}]}
        """
        for update in msg['data']:
            await self.callback(TICKER, feed=self.id,
                                         pair=update['instrument_id'],
                                         bid=Decimal(update['best_bid']),
                                         ask=Decimal(update['best_ask']),
                                         timestamp=timestamp_normalize(self.id, update['timestamp']))

    async def _trade(self, msg):
        """
        {'table': 'spot/trade', 'data': [{'instrument_id': 'BTC-USD', 'price': '3977.44', 'side': 'buy', 'size': '0.0096', 'timestamp': '2019-03-22T22:45:44.578Z', 'trade_id': '486519521'}]}
        """
        for trade in msg['data']:
            if msg['table'] == 'futures/trade':
                amount_sym = 'qty'
            else:
                amount_sym = 'size'
            await self.callback(TRADES,
                feed=self.id,
                pair=pair_exchange_to_std(trade['instrument_id']),
                order_id=trade['trade_id'],
                side=BUY if trade['side'] == 'buy' else SELL,
                amount=Decimal(trade[amount_sym]),
                price=Decimal(trade['price']),
                timestamp=timestamp_normalize(self.id, trade['timestamp'])
            )

    async def _book(self, msg):
        if msg['action'] == 'partial':
            # snapshot
            for update in msg['data']:
                pair = pair_exchange_to_std(update['instrument_id'])
                self.l2_book[pair] = {
                    BID: sd({
                        Decimal(price) : Decimal(amount) for price, amount, *_ in update['bids']
                    }),
                    ASK: sd({
                        Decimal(price) : Decimal(amount) for price, amount, *_ in update['asks']
                    })
                }
                await self.book_callback(self.l2_book[pair], L2_BOOK, pair, True, None, timestamp_normalize(self.id, update['timestamp']))
        else:
            # update
            for update in msg['data']:
                delta = {BID: [], ASK: []}
                pair = pair_exchange_to_std(update['instrument_id'])
                for side in ('bids', 'asks'):
                    s = BID if side == 'bids' else ASK
                    for price, amount, *_ in update[side]:
                        price = Decimal(price)
                        amount = Decimal(amount)
                        if amount == 0:
                            delta[s].append((price, 0))
                            del self.l2_book[pair][s][price]
                        else:
                            delta[s].append((price, amount))
                            self.l2_book[pair][s][price] = amount
                await self.book_callback(self.l2_book[pair], L2_BOOK, pair, False, delta, timestamp_normalize(self.id, update['timestamp']))

    async def _order(self, msg):
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
        for data in msg['data']:
            if data.get('order_id'):
                data_type = ORDER
                if 'SWAP' in data['instrument_id']:
                    data_type = ORDER_SWAP
                new_info = self.parse_order(data)
                await self.callback(data_type, feed=self.id, pair=pair_exchange_to_std(data['instrument_id']), **new_info)

    def parse_order(self, data):
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
            order['status'] = OKCoin.parse_order_status(data['state'])

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

    @staticmethod
    def parse_order_status(state):
        """
        Reference:
            state(str):
                -2:Failed
                -1:Canceled
                0:Open
                1:Partially Filled
                2:Fully Filled
                3:Submitting
                4:Canceling
        """
        states = {
            '-2': 'failed',
            '-1': 'canceled',
            '0': 'open',
            '1': 'open',
            '2': 'closed',
            '3': 'open',
            '4': 'canceled'
        }

        ret = states.get(state, None)
        return ret

    async def message_handler(self, msg: str, timestamp: float):
        # DEFLATE compression, no header
        msg = zlib.decompress(msg, -15)
        msg = json.loads(msg, parse_float=Decimal)

        if 'event' in msg:
            if msg['event'] == 'error':
                LOG.error("%s: Error: %s", self.id, msg)
            elif msg['event'] == 'subscribe':
                pass
            elif msg['event'] == 'login':
                self.logged_in = True
            else:
                LOG.warning("%s: Unhandled event %s", self.id, msg)
        elif 'table' in msg:
            if re.match(f'^({"|".join(self.table_prefixs)})/ticker$', msg['table']):
                await self._ticker(msg)
            elif re.match(f'^({"|".join(self.table_prefixs)})/trade$', msg['table']):
                await self._trade(msg)
            elif re.match(f'^({"|".join(self.table_prefixs)})/depth$', msg['table']):
                await self._book(msg)
            elif re.match(f'^({"|".join(self.table_prefixs)})/order$', msg['table']):
                await self._order(msg)
            else:
                LOG.warning("%s: Unhandled message %s", self.id, msg)
        else:
            LOG.warning("%s: Unhandled message %s", self.id, msg)

    async def authenticate(self, websocket):
        auth_info = RestOKEx.generate_signature('GET', '/users/self/verify', key_id=self.key_id, key_secret=self.key_secret, passphrase=self.passphrase)
        api_key = auth_info.get('api_key')
        passphrase = auth_info.get('passphrase')
        timestamp = auth_info.get('timestamp')
        sign = auth_info.get('sign')
        await websocket.send(json.dumps({"op": "login",
                                         "args": [api_key, passphrase, timestamp, sign]}))
