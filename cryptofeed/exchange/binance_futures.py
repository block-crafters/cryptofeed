'''
Copyright (C) 2017-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import os
import json
import logging
from decimal import Decimal


from cryptofeed.feed import Feed
from cryptofeed.defines import BINANCE_FUTURES, ORDER, BUY, SELL
from cryptofeed.exchange.binance import Binance
from cryptofeed.rest.binance_futures import BinanceFutures as RestBinanceFutures
from cryptofeed.standards import pair_exchange_to_std, timestamp_normalize, feed_to_exchange

LOG = logging.getLogger('feedhandler')


class BinanceFutures(Binance):
    id = BINANCE_FUTURES

    def __init__(self, pairs=None, channels=None, callbacks=None, depth=1000, **kwargs):
        Feed.__init__(self, None, pairs=pairs, channels=channels, callbacks=callbacks, **kwargs)

        if self.use_private_channels:
            self.key_id = os.environ.get('BINANCE_API_KEY')
            self.key_secret = os.environ.get('BINANCE_SECRET_KEY')

        self.rest_client = RestBinanceFutures()
        self.book_depth = depth
        self.ws_endpoint = 'wss://fstream.binance.com'
        self.rest_endpoint = 'https://fapi.binance.com/fapi/v1'
        self.address = self._address()
        self._reset()

    def _check_update_id(self, pair: str, msg: dict):
        skip_update = False
        forced = False

        if pair in self.last_update_id:
            if msg['u'] < self.last_update_id[pair]:
                skip_update = True
            elif msg['U'] <= self.last_update_id[pair] <= msg['u']:
                del self.last_update_id[pair]
                forced = True
            else:
                raise Exception("Error - snaphot has no overlap with first update")

        return skip_update, forced

    async def _order(self, msg: dict):
        new_info = self.parse_order(msg)
        await self.callback(ORDER, feed=self.id, **new_info)

    def parse_order(self, data):
        """
        Futures order response format
        {
        "e":"ORDER_TRADE_UPDATE",     // Event Type
        "E":1568879465651,            // Event Time
        "T": 1568879465650,           //  Transaction Time
        "o":{
            "s":"BTCUSDT",              // Symbol
            "c":"TEST",                 // Client Order Id
            "S":"SELL",                 // Side
            "o":"LIMIT",                // Order Type
            "f":"GTC",                  // Time in Force
            "q":"0.001",                // Original Quantity
            "p":"9910",                 // Price
            "ap":"0",                   // Average Price
            "sp":"0",                   // Stop Price
            "x":"NEW",                  // Execution Type
            "X":"NEW",                  // Order Status
            "i":8886774,                // Order Id
            "l":"0",                    // Order Last Filled Quantity
            "z":"0",                    // Order Filled Accumulated Quantity
            "L":"0",                    // Last Filled Price
            "N": "USDT",                // Commission Asset, will not push if no commission
            "n": "0",                   // Commission, will not push if no commission
            "T":1568879465651,          // Order Trade Time
            "t":0,                      // Trade Id
            "b":"0",                    // Bids Notional
            "a":"9.91",                 // Ask Notional
            "m": false,                 // Is this trade the maker side?
            "R":false,                  // Is this reduce only
            "wt": "CONTRACT_PRICE"      // stop price working type
            }
        }
        """
        order = data['o']
        ts = timestamp_normalize(self.id, data['E'])
        parsed_order = {
            'pair': pair_exchange_to_std(order['s']),
            'order_id': order['i'],
            'client_order_id': order.get('c', ''),
            'timestamp': ts
        }
        parsed_order['side'] = BUY if order['S'] == 'BUY' else SELL
        parsed_order['status'] = self.parse_order_status(order['X']) # X: Current order status
        parsed_order['amount'] = float(order['q']) # q: Order quantity
        filled = float(order['z']) # z: Cumulative filled quantity
        parsed_order['filled'] = filled
        parsed_order['remaining'] = parsed_order['amount'] - filled
        if order.get('p') is not None:
            parsed_order['price'] = float(order['p']) # p: Order price
        if float(order['ap']) > 0:
            parsed_order['average'] = float(order['ap'])

        return parsed_order

    async def message_handler(self, msg: str, timestamp: float):
        msg = json.loads(msg, parse_float=Decimal)
        if self.use_private_channels:
            # NOTE: Implement each case if needed
            if msg['e'] == 'listenKeyExpired':
                LOG.warning('listen_key was expired')
            elif msg['e'] == 'ACCOUNT_UPDATE':
                pass
            elif msg['e'] == 'ORDER_TRADE_UPDATE':
                symbol = msg.get('o', {}).get('s', '')
                pairs = []
                if self.config:
                    channel = feed_to_exchange(self.id, ORDER)
                    pairs = list(self.config[channel])
                else:
                    pairs = self.pairs

                if symbol in pairs:
                    await self._order(msg)

        else:
            # Combined stream events are wrapped as follows: {"stream":"<streamName>","data":<rawPayload>}
            # streamName is of format <symbol>@<channel>
            pair, _ = msg['stream'].split('@', 1)
            msg = msg['data']
            pair = pair.upper()

            if msg['e'] == 'depthUpdate':
                await self._book(msg, pair, timestamp)
            elif msg['e'] == 'aggTrade':
                await self._trade(msg)
            elif msg['e'] == '24hrTicker':
                await self._ticker(msg)
            else:
                LOG.warning("%s: Unexpected message received: %s", self.id, msg)
