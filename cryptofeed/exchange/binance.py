'''
Copyright (C) 2017-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import os
import asyncio
import json
import logging
import time
from decimal import Decimal

import aiohttp
from sortedcontainers import SortedDict as sd

from cryptofeed.feed import Feed
from cryptofeed.defines import TICKER, TRADES, ORDER, BUY, SELL, BID, ASK, L2_BOOK, BINANCE
from cryptofeed.rest.binance import Binance as RestBinance
from cryptofeed.standards import pair_exchange_to_std, timestamp_normalize, feed_to_exchange


LOG = logging.getLogger('feedhandler')


class Binance(Feed):
    id = BINANCE

    def __init__(self, pairs=None, channels=None, callbacks=None, depth=1000, **kwargs):
        super().__init__(None, pairs=pairs, channels=channels, callbacks=callbacks, **kwargs)

        if self.use_private_channels:
            self.key_id = os.environ.get('BINANCE_API_KEY')
            self.key_secret = os.environ.get('BINANCE_SECRET_KEY')

        self.rest_client = RestBinance()
        self.book_depth = depth
        self.ws_endpoint = 'wss://stream.binance.com:9443'
        self.rest_endpoint = 'https://www.binance.com/api/v1'
        self.address = self._address()
        self.listen_key_start_time = None
        self._reset()

    def _address(self):
        if self.use_private_channels:
            self.listen_key = self.rest_client.create_listen_key().get('listenKey')
            address = self.ws_endpoint+f'/ws/{self.listen_key}'
            self.listen_key_start_time = time.time()
            return address
        else:
            address = self.ws_endpoint+'/stream?streams='
            for chan in self.channels if not self.config else self.config:
                for pair in self.pairs if not self.config else self.config[chan]:
                    pair = pair.lower()
                    stream = f"{pair}@{chan}/"
                    address += stream
            return address[:-1]

    def _reset(self):
        self.l2_book = {}
        self.last_update_id = {}

    def extend_listen_key_validity(self):
        """
        Extend user data listen_key validity and reset start_time
        """
        try:
            self.rest_client.keepalive_listen_key(self.listen_key)
            self.listen_key_start_time = time.time()
        except:
            LOG.error("Failed to keepalive listen_key", exc_info=True)

    async def _trade(self, msg):
        """
        {
            "e": "aggTrade",  // Event type
            "E": 123456789,   // Event time
            "s": "BNBBTC",    // Symbol
            "a": 12345,       // Aggregate trade ID
            "p": "0.001",     // Price
            "q": "100",       // Quantity
            "f": 100,         // First trade ID
            "l": 105,         // Last trade ID
            "T": 123456785,   // Trade time
            "m": true,        // Is the buyer the market maker?
            "M": true         // Ignore
        }
        """
        price = Decimal(msg['p'])
        amount = Decimal(msg['q'])
        await self.callback(TRADES, feed=self.id,
                                     order_id=msg['a'],
                                     pair=pair_exchange_to_std(msg['s']),
                                     side=SELL if msg['m'] else BUY,
                                     amount=amount,
                                     price=price,
                                     timestamp=timestamp_normalize(self.id, msg['E']))

    async def _ticker(self, msg):
        """
        {
        "e": "24hrTicker",  // Event type
        "E": 123456789,     // Event time
        "s": "BNBBTC",      // Symbol
        "p": "0.0015",      // Price change
        "P": "250.00",      // Price change percent
        "w": "0.0018",      // Weighted average price
        "x": "0.0009",      // Previous day's close price
        "c": "0.0025",      // Current day's close price
        "Q": "10",          // Close trade's quantity
        "b": "0.0024",      // Best bid price
        "B": "10",          // Best bid quantity
        "a": "0.0026",      // Best ask price
        "A": "100",         // Best ask quantity
        "o": "0.0010",      // Open price
        "h": "0.0025",      // High price
        "l": "0.0010",      // Low price
        "v": "10000",       // Total traded base asset volume
        "q": "18",          // Total traded quote asset volume
        "O": 0,             // Statistics open time
        "C": 86400000,      // Statistics close time
        "F": 0,             // First trade ID
        "L": 18150,         // Last trade Id
        "n": 18151          // Total number of trades
        }
        """
        pair = pair_exchange_to_std(msg['s'])
        bid = Decimal(msg['b'])
        ask = Decimal(msg['a'])
        await self.callback(TICKER, feed=self.id,
                                     pair=pair,
                                     bid=bid,
                                     ask=ask,
                                     timestamp=timestamp_normalize(self.id, msg['E']))

    async def _snapshot(self, pairs: list):
        urls = [f'{self.rest_endpoint}/depth?symbol={sym}&limit={self.book_depth}' for sym in pairs]
        async def fetch(session, url):
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.json()

        async with aiohttp.ClientSession() as session:
            results = await asyncio.gather(*[fetch(session, url) for url in urls])

        for r, pair in zip(results, pairs):
            std_pair = pair_exchange_to_std(pair)
            self.last_update_id[pair] = r['lastUpdateId']
            self.l2_book[std_pair] = {BID: sd(), ASK: sd()}
            for s, side in (('bids', BID), ('asks', ASK)):
                for update in r[s]:
                    price = Decimal(update[0])
                    amount = Decimal(update[1])
                    self.l2_book[std_pair][side][price] = amount

    def _check_update_id(self, pair: str, msg: dict):
        skip_update = False
        forced = False

        if pair in self.last_update_id:
            if msg['u'] <= self.last_update_id[pair]:
                skip_update = True
            elif msg['U'] <= self.last_update_id[pair]+1 <= msg['u']:
                del self.last_update_id[pair]
                forced = True
            else:
                raise Exception("Error - snaphot has no overlap with first update")

        return skip_update, forced

    async def _book(self, msg: dict, pair: str, timestamp: float):
        """
        {
            "e": "depthUpdate", // Event type
            "E": 123456789,     // Event time
            "s": "BNBBTC",      // Symbol
            "U": 157,           // First update ID in event
            "u": 160,           // Final update ID in event
            "b": [              // Bids to be updated
                    [
                        "0.0024",       // Price level to be updated
                        "10"            // Quantity
                    ]
            ],
            "a": [              // Asks to be updated
                    [
                        "0.0026",       // Price level to be updated
                        "100"           // Quantity
                    ]
            ]
        }
        """
        skip_update, forced = self._check_update_id(pair, msg)
        if skip_update:
            return

        delta = {BID: [], ASK: []}
        pair = pair_exchange_to_std(pair)
        timestamp = msg['E']

        for s, side in (('b', BID), ('a', ASK)):
            for update in msg[s]:
                price = Decimal(update[0])
                amount = Decimal(update[1])

                if amount == 0:
                    if price in self.l2_book[pair][side]:
                        del self.l2_book[pair][side][price]
                        delta[side].append((price, amount))
                else:
                    self.l2_book[pair][side][price] = amount
                    delta[side].append((price, amount))

        await self.book_callback(self.l2_book[pair], L2_BOOK, pair, forced, delta, timestamp_normalize(self.id, timestamp))

    async def _order(self, msg: dict):
        """
        {
            "e": "executionReport",        // Event type
            "E": 1499405658658,            // Event time
            "s": "ETHBTC",                 // Symbol
            "c": "mUvoqJxFIILMdfAW5iGSOW", // Client order ID
            "S": "BUY",                    // Side
            "o": "LIMIT",                  // Order type
            "f": "GTC",                    // Time in force
            "q": "1.00000000",             // Order quantity
            "p": "0.10264410",             // Order price
            "P": "0.00000000",             // Stop price
            "F": "0.00000000",             // Iceberg quantity
            "g": -1,                       // OrderListId
            "C": null,                     // Original client order ID; This is the ID of the order being canceled
            "x": "NEW",                    // Current execution type
            "X": "NEW",                    // Current order status
            "r": "NONE",                   // Order reject reason; will be an error code.
            "i": 4293153,                  // Order ID
            "l": "0.00000000",             // Last executed quantity
            "z": "0.00000000",             // Cumulative filled quantity
            "L": "0.00000000",             // Last executed price
            "n": "0",                      // Commission amount
            "N": null,                     // Commission asset
            "T": 1499405658657,            // Transaction time
            "t": -1,                       // Trade ID
            "I": 8641984,                  // Ignore
            "w": true,                     // Is the order on the book?
            "m": false,                    // Is this trade the maker side?
            "M": false,                    // Ignore
            "O": 1499405658657,            // Order creation time
            "Z": "0.00000000",             // Cumulative quote asset transacted quantity
            "Y": "0.00000000",             // Last quote asset transacted quantity (i.e. lastPrice * lastQty)
            "Q": "0.00000000"              // Quote Order Qty
            }
        """
        new_info = self.parse_order(msg)
        await self.callback(ORDER, feed=self.id, **new_info)

    @staticmethod
    def parse_order_status(status):
        """
        Order status (status):

        NEW - The order has been accepted by the engine.
        PARTIALLY_FILLED - A part of the order has been filled.
        FILLED - The order has been completely filled.
        CANCELED - The order has been canceled by the user.
        PENDING_CANCEL (currently unused)
        REJECTED - The order was not accepted by the engine and not processed.
        EXPIRED - The order was canceled according to the order type's rules 
                (e.g. LIMIT FOK orders with no fill, LIMIT IOC or MARKET orders that partially fill) 
                or by the exchange, 
                (e.g. orders canceled during liquidation, orders canceled during maintenance)
        """

        statuses = {
            'NEW': 'open',
            'PARTIALLY_FILLED': 'open',
            'FILLED': 'closed',
            'CANCELED': 'canceled',
            'PENDING_CANCEL': 'canceling',
            'REJECTED': 'rejected',
            'EXPIRED': 'canceled',
        }

        ret = statuses.get(status, None)
        return ret

    def parse_order(self, data):
        ts = timestamp_normalize(self.id, data['E'])
        order = {
            'pair': pair_exchange_to_std(data['s']),
            'order_id': data['i'],
            'client_order_id': data.get('c', ''),
            'timestamp': ts
        }
        order['side'] = BUY if data['S'] == 'BUY' else SELL
        order['status'] = self.parse_order_status(data['X']) # X: Current order status
        order['amount'] = float(data['q']) # q: Order quantity
        filled = float(data['z']) # z: Cumulative filled quantity
        order['filled'] = filled
        order['remaining'] = order['amount'] - filled
        if data.get('p') is not None:
            order['price'] = float(data['p']) # p: Order price
        cumulative_quote_quantity = float(data['Z']) # Z: Cumulative quote asset transacted quantity
        if filled > 0:
            order['average'] = cumulative_quote_quantity / filled

        return order

    async def message_handler(self, msg: str, timestamp: float):
        msg = json.loads(msg, parse_float=Decimal)
        if self.use_private_channels:
            if msg['e'] == 'outboundAccountInfo':
                pass
            elif msg['e'] == 'outboundAccountPosition':
                pass
            elif msg['e'] == 'balanceUpdate':
                pass
            elif msg['e'] == 'executionReport':
                symbol = msg.get('s', '')
                if symbol in self.pairs:
                    await self._order(msg)

            elif msg['e'] == 'listStatus':
                pass        
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

    async def subscribe(self, websocket):
        # Binance does not have a separate subscribe message, the
        # subsription information is included in the
        # connection endpoint
        self._reset()
        # If full book enabled, collect snapshot first
        if feed_to_exchange(self.id, L2_BOOK) in self.channels:
            await self._snapshot(self.pairs)
        elif feed_to_exchange(self.id, L2_BOOK) in self.config:
            await self._snapshot(self.config[feed_to_exchange(self.id, L2_BOOK)])
