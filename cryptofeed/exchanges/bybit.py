'''
Copyright (C) 2018-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from collections import defaultdict
from cryptofeed.symbols import Symbol
import logging
from decimal import Decimal
from functools import partial
from typing import Any, Dict, List, Callable, Optional, Tuple, Union
from datetime import datetime as dt

from sortedcontainers import SortedDict as sd
from yapic import json

from cryptofeed.connection import AsyncConnection, WSAsyncConn
from cryptofeed.defines import (
    BALANCES, BID, ASK, BUY, BYBIT, FUNDING, L2_BOOK, SELL, TRADES, 
    OPEN_INTEREST, FUTURES_INDEX, ORDER_INFO, USER_FILLS, FUTURES, PERPETUAL
)
from cryptofeed.feed import Feed
# For auth
import hmac
import time

LOG = logging.getLogger('feedhandler')


class Bybit(Feed):
    id = BYBIT
    symbol_endpoint = 'https://api.bybit.com/v2/public/symbols'
    websocket_channels = {
        L2_BOOK: 'orderBookL2_25',
        TRADES: 'trade',
        USER_FILLS: 'execution',
        ORDER_INFO: 'order',
        FUTURES_INDEX: 'instrument_info.100ms',
        OPEN_INTEREST: 'instrument_info.100ms',
        FUNDING: 'instrument_info.100ms'
    }

    @classmethod
    def timestamp_normalize(cls, ts: Union[int, dt]) -> float:
        if isinstance(ts, int):
            return ts / 1000.0
        else:
            return ts.timestamp()

    @classmethod
    def _parse_symbol_data(cls, data: dict) -> Tuple[Dict, Dict]:
        ret = {}
        info = defaultdict(dict)

        # PERPETUAL & FUTURES
        for symbol in data['result']:
            base = symbol['base_currency']
            quote = symbol['quote_currency']

            stype = PERPETUAL
            expiry = None
            if not symbol['name'].endswith(quote):
                stype = FUTURES
                year = symbol['name'].replace(base + quote, '')[-2:]
                expiry = year + symbol['alias'].replace(base + quote, '')[-4:]

            s = Symbol(base, quote, type=stype, expiry_date=expiry)

            ret[s.normalized] = symbol['name']
            info['tick_size'][s.normalized] = symbol['price_filter']['tick_size']
            info['instrument_type'][s.normalized] = stype

        return ret, info

    def __init__(self, **kwargs):
        super().__init__({'USD': 'wss://stream.bybit.com/realtime', 'USDT': 'wss://stream.bybit.com/realtime_public', 'USDTP': 'wss://stream.bybit.com/realtime_private'}, **kwargs)

    def __reset(self, quote=None):
        self._instrument_info_cache = {}
        if quote is None:
            self._l2_book = {}
        else:
            rem = [symbol for symbol in self._l2_book if quote in symbol]
            for symbol in rem:
                del self._l2_book[symbol]

    async def message_handler(self, msg: str, conn, timestamp: float):

        msg = json.loads(msg, parse_float=Decimal)

        if "success" in msg:
            if msg['success']:
                if msg['request']['op'] == 'auth':
                    LOG.info("%s: Authenticated successful", conn.uuid)
                elif msg['request']['op'] == 'subscribe':
                    LOG.info("%s: Subscribed to channels: %s", conn.uuid, msg['request']['args'])
                else:
                    LOG.warning("%s: Unhandled 'successs' message received", conn.uuid)
            else:
                LOG.error("%s: Error from exchange %s", conn.uuid, msg)
        elif "trade" in msg["topic"]:
            await self._trade(msg, timestamp)
        elif "orderBookL2" in msg["topic"]:
            await self._book(msg, timestamp)
        elif "instrument_info" in msg["topic"]:
            await self._instrument_info(msg, timestamp)
        elif "order" == msg["topic"]:
            await self._order(msg, timestamp)
        elif "wallet" == msg["topic"]:
            await self._balance(msg, timestamp)
        elif "position" == msg["position"]:
            await self._position(msg, timestamp)
        elif "execution" in msg["topic"]:
            await self._execution(msg, timestamp)
        else:
            LOG.warning("%s: Unhandled message type %s", conn.uuid, msg)

    def connect(self) -> List[Tuple[AsyncConnection, Callable[[None], None], Callable[[str, float], None]]]:
        '''
            Linear PERPETUAL (USDT) public goes to USDT endpoint
            Linear PERPETUAL (USDT) private goes to USDTP endpoint
            Inverse PERPETUAL and FUTURES (USD) both private and public goes to USD endpoint
        '''
        ret = []
        if any(pair.split('-')[1] == 'USDT' for pair in self.normalized_symbols):
            if any(self.is_authenticated_channel(self.exchange_channel_to_std(chan)) for chan in self.subscription):
                ret.append((WSAsyncConn(self.address['USDTP'], self.id, **self.ws_defaults),
                            partial(self.subscribe, quote='USDTP'), self.message_handler, self.authenticate))
            if any(not self.is_authenticated_channel(self.exchange_channel_to_std(chan)) for chan in self.subscription):
                ret.append((WSAsyncConn(self.address['USDT'], self.id, **self.ws_defaults),
                            partial(self.subscribe, quote='USDT'), self.message_handler, self.__no_auth))
        if any(pair.split('-')[1] == 'USD' for pair in self.normalized_symbols):
            ret.append((WSAsyncConn(self.address['USD'], self.id, **self.ws_defaults),
                        partial(self.subscribe, quote='USD'), self.message_handler, self.authenticate))
        return ret

    async def subscribe(self, connection: AsyncConnection, quote: str = None):

        if quote == 'USD' or quote == 'USDTP':
            channels = []
            for chan in self.subscription:
                if self.is_authenticated_channel(self.exchange_channel_to_std(chan)):
                    channels.append(chan)
            if channels:
                await connection.write(json.dumps({"op": "subscribe", "args": channels}))
                LOG.debug(f'{connection.uuid}: Subscribing to auth, quote: {quote}, channels: {channels}')

        if quote == 'USDTP':
            return

        self.__reset(quote=quote)

        for chan in self.subscription:
            if not self.is_authenticated_channel(self.exchange_channel_to_std(chan)):
                for pair in self.subscription[chan]:
                    # Bybit uses separate addresses for difference quote currencies
                    if 'USDT' not in pair and quote == 'USDT':
                        continue
                    if 'USDT' in pair and quote == 'USD':
                        continue

                    await connection.write(json.dumps(
                        {
                            "op": "subscribe",
                            "args": [f"{chan}.{pair}"]
                        }
                    ))
                    LOG.debug(f'{connection.uuid}: Subscribing to public, quote: {quote}, {chan}.{pair}')

    async def _instrument_info(self, msg: dict, timestamp: float):
        """
        ### Snapshot type update
        {
        "topic": "instrument_info.100ms.BTCUSD",
        "type": "snapshot",
        "data": {
            "id": 1,
            "symbol": "BTCUSD",                           //instrument name
            "last_price_e4": 81165000,                    //the latest price
            "last_tick_direction": "ZeroPlusTick",        //the direction of last tick:PlusTick,ZeroPlusTick,MinusTick,
                                                          //ZeroMinusTick
            "prev_price_24h_e4": 81585000,                //the price of prev 24h
            "price_24h_pcnt_e6": -5148,                   //the current last price percentage change from prev 24h price
            "high_price_24h_e4": 82900000,                //the highest price of prev 24h
            "low_price_24h_e4": 79655000,                 //the lowest price of prev 24h
            "prev_price_1h_e4": 81395000,                 //the price of prev 1h
            "price_1h_pcnt_e6": -2825,                    //the current last price percentage change from prev 1h price
            "mark_price_e4": 81178500,                    //mark price
            "index_price_e4": 81172800,                   //index price
            "open_interest": 154418471,                   //open interest quantity - Attention, the update is not
                                                          //immediate - slowest update is 1 minute
            "open_value_e8": 1997561103030,               //open value quantity - Attention, the update is not
                                                          //immediate - the slowest update is 1 minute
            "total_turnover_e8": 2029370141961401,        //total turnover
            "turnover_24h_e8": 9072939873591,             //24h turnover
            "total_volume": 175654418740,                 //total volume
            "volume_24h": 735865248,                      //24h volume
            "funding_rate_e6": 100,                       //funding rate
            "predicted_funding_rate_e6": 100,             //predicted funding rate
            "cross_seq": 1053192577,                      //sequence
            "created_at": "2018-11-14T16:33:26Z",
            "updated_at": "2020-01-12T18:25:16Z",
            "next_funding_time": "2020-01-13T00:00:00Z",  //next funding time
                                                          //the rest time to settle funding fee
            "countdown_hour": 6                           //the remaining time to settle the funding fee
        },
        "cross_seq": 1053192634,
        "timestamp_e6": 1578853524091081                  //the timestamp when this information was produced
        }

        ### Delta type update
        {
        "topic": "instrument_info.100ms.BTCUSD",
        "type": "delta",
        "data": {
            "delete": [],
            "update": [
                {
                    "id": 1,
                    "symbol": "BTCUSD",
                    "prev_price_24h_e4": 81565000,
                    "price_24h_pcnt_e6": -4904,
                    "open_value_e8": 2000479681106,
                    "total_turnover_e8": 2029370495672976,
                    "turnover_24h_e8": 9066215468687,
                    "volume_24h": 735316391,
                    "cross_seq": 1053192657,
                    "created_at": "2018-11-14T16:33:26Z",
                    "updated_at": "2020-01-12T18:25:25Z"
                }
            ],
            "insert": []
        },
        "cross_seq": 1053192657,
        "timestamp_e6": 1578853525691123
        }
        """
        update_type = msg['type']

        if update_type == 'snapshot':
            updates = [msg['data']]
        else:
            updates = msg['data']['update']

        for info in updates:
            if msg['topic'] in self._instrument_info_cache and self._instrument_info_cache[msg['topic']] == updates:
                continue
            else:
                self._instrument_info_cache[msg['topic']] = updates

            if 'updated_at_e9' in info:
                ts = info['updated_at_e9'] / 1e9
            elif 'updated_at' in info:
                ts = self.timestamp_normalize(info['updated_at'])
            else:
                continue

            if 'open_interest' in info:
                await self.callback(OPEN_INTEREST, feed=self.id,
                                    symbol=self.exchange_symbol_to_std_symbol(info['symbol']),
                                    open_interest=Decimal(info['open_interest']),
                                    timestamp=ts,
                                    receipt_timestamp=timestamp)

            if 'index_price_e4' in info:
                await self.callback(FUTURES_INDEX, feed=self.id,
                                    symbol=self.exchange_symbol_to_std_symbol(info['symbol']),
                                    futures_index=Decimal(info['index_price_e4']) * Decimal('1e-4'),
                                    timestamp=ts,
                                    receipt_timestamp=timestamp)

            if 'funding_rate_e6' in info:
                await self.callback(FUNDING, feed=self.id,
                                    symbol=self.exchange_symbol_to_std_symbol(info['symbol']),
                                    rate=Decimal(info['funding_rate_e6']) * Decimal('1e-6'),
                                    predicted_rate=Decimal(info['predicted_funding_rate_e6']) * Decimal('1e-6'),
                                    next_funding_time=info['next_funding_time'].timestamp(),
                                    timestamp=ts,
                                    receipt_timestamp=timestamp)

    async def _trade(self, msg: dict, timestamp: float):
        """
        {"topic":"trade.BTCUSD",
        "data":[
            {
                "timestamp":"2019-01-22T15:04:33.461Z",
                "symbol":"BTCUSD",
                "side":"Buy",
                "size":980,
                "price":3563.5,
                "tick_direction":"PlusTick",
                "trade_id":"9d229f26-09a8-42f8-aff3-0ba047b0449d",
                "cross_seq":163261271}]}
        """
        data = msg['data']
        for trade in data:
            if isinstance(trade['trade_time_ms'], str):
                ts = int(trade['trade_time_ms'])
            else:
                ts = trade['trade_time_ms']

            await self.callback(TRADES,
                                feed=self.id,
                                symbol=self.exchange_symbol_to_std_symbol(trade['symbol']),
                                order_id=trade['trade_id'],
                                side=BUY if trade['side'] == 'Buy' else SELL,
                                amount=Decimal(trade['size']),
                                price=Decimal(trade['price']),
                                timestamp=self.timestamp_normalize(ts),
                                receipt_timestamp=timestamp
                                )

    async def _book(self, msg: dict, timestamp: float):
        pair = self.exchange_symbol_to_std_symbol(msg['topic'].split('.')[1])
        update_type = msg['type']
        data = msg['data']
        forced = False
        delta = {BID: [], ASK: []}

        if update_type == 'snapshot':
            self._l2_book[pair] = {BID: sd({}), ASK: sd({})}
            # the USDT perpetual data is under the order_book key
            if 'order_book' in data:
                data = data['order_book']

            for update in data:
                side = BID if update['side'] == 'Buy' else ASK
                self._l2_book[pair][side][Decimal(update['price'])] = Decimal(update['size'])
            forced = True
        else:
            for delete in data['delete']:
                side = BID if delete['side'] == 'Buy' else ASK
                price = Decimal(delete['price'])
                delta[side].append((price, 0))
                del self._l2_book[pair][side][price]

            for utype in ('update', 'insert'):
                for update in data[utype]:
                    side = BID if update['side'] == 'Buy' else ASK
                    price = Decimal(update['price'])
                    amount = Decimal(update['size'])
                    delta[side].append((price, amount))
                    self._l2_book[pair][side][price] = amount

        # timestamp is in microseconds
        ts = msg['timestamp_e6']
        if isinstance(ts, str):
            ts = int(ts)
        await self.book_callback(self._l2_book[pair], L2_BOOK, pair, forced, delta, ts / 1000000, timestamp)

    @staticmethod
    def parse_time_in_force(time_in_force: str) -> Optional[str]:
        return {
            'GoodTillCancel': 'GTC',
            'ImmediateOrCancel': 'IOC',
            'FillOrKill': 'FOK',
            'PostOnly': 'PO',
        }.get(time_in_force)

    @staticmethod
    def parse_order_status(status: str) -> Optional[str]:
        return {
            'Created': 'open',
            'Rejected': 'rejected',
            'New': 'open',
            'PartiallyFilled': 'open',
            'Filled': 'closed',
            'Cancelled': 'canceled',
            'PendingCancel': 'open',
        }.get(status)

    def parse_order(self, data: dict[str, Any]) -> dict[str, Any]:
        timestamp = self.timestamp_normalize(data['timestamp'])
        order = {
            'order_id': data['order_id'],
            'timestamp': timestamp,
            'account': self.custom_account_id,
        }
        if data.get('symbol') is not None:
            order['symbol'] = self.exchange_symbol_to_std_symbol(data['symbol'])
        if data.get('side') is not None:
            order['side'] = SELL if data['side'] == "Sell" else BUY
        if data.get('order_status') is not None:
            order['status'] = self.parse_order_status(data['order_status'])
            order['postOnly'] = (order['status'] == 'PO')
        if data.get('time_in_force') is not None:
            order['timeInForce'] = self.parse_time_in_force(data['time_in_force'])
        if data.get('order_link_id') is not None:
            order['clientOrderId'] = data['order_link_id']
        if data.get('price') is not None:
            order['price'] = Decimal(data['price'])
            # Bitmex is using order['avgPrice'] but seems latest CCXT uses 'average' key instead?
        if data.get('qty') is not None:
            order['amount'] = Decimal(data['qty'])
            if data.get('leaves_qty') is not None:
                order['filled'] = order['amount'] - data['leaves_qty']
                order['remaining'] = data['leaves_qty']
        if data.get('order_type') is not None:
            order['orderType'] = data['order_type']
        if data.get('timestamp') is not None:
            order['lastTradeTimestamp'] = self.timestamp_normalize(data['timestamp'])

        return order

    async def _order(self, msg: dict, timestamp: float):
        """
        {
            "topic": "order",
            "data": [
                {
                    "order_id": "xxxxxxxx-xxxx-xxxx-9a8f-4a973eb5c418", # (O)
                    "order_link_id": "",                                # (O)
                    "symbol": "BTCUSD",                                 # (O)
                    "side": "Sell",                                     # (O)
                    "order_type": "Market",
                    "price": "8579.5",                                  # (O)
                    "qty": 1,                                           # (O)
                    "time_in_force": "ImmediateOrCancel",               # (O)
                    "create_type": "CreateByClosing",
                    "cancel_type": "",
                    "order_status": "Filled",                           # (O)
                    "leaves_qty": 0,                                    # (O)
                    "cum_exec_qty": 1,
                    "cum_exec_value": "0.00011655",
                    "cum_exec_fee": "0.00000009",
                    "timestamp": "2020-01-14T14:09:31.778Z",
                    "take_profit": "0",
                    "stop_loss": "0",
                    "trailing_stop": "0",
                    "trailing_active": "0",
                    "last_exec_price": "8580",
                    "reduce_only": false,
                    "close_on_trigger": false
                }
            ]
        }
        """
        for i in range(len(msg['data'])):
            data = self.parse_order(msg['data'][i])
            symbol = data['symbol']
            data_to_log = {
                'feed': self.id,
                'timestamp': data.get('timestamp'),
                'symbol': data.get('symbol'),
                'order_id': data.get('order_id'),
                'create_type': data.get('create_type'),
            }
            LOG.debug(f"Handle order msg: {data_to_log}")
            await self.callback(ORDER_INFO, feed=self.id, symbol=symbol, data=data, receipt_timestamp=timestamp)

    def parse_balance(self, data: dict[str, Any]) -> dict[str, Any]:
        balance: dict[str, Any] = {
            'timestamp': self.timestamp_normalize(data['timestamp'])
        }
        if data.get('user_id') is not None:
            balance['account'] = self.custom_account_id or data['user_id']
        if data.get('coin') is not None:
            balance['currency'] = data['coin']
        if data.get('wallet_balance') is not None:
            balance['total'] = Decimal(data['wallet_balance'])
        if data.get('available_balance') is not None:
            balance['free'] = Decimal(data['available_balance'])

        return balance

    async def _balance(self, msg: dict, timestamp: float):
        """
        {
            "topic": "wallet",
            "data": [
                {
                    "user_id": 738713,
                    "coin": "BTC",
                    "available_balance": "1.50121026",
                    "wallet_balance": "1.50121261"
                }
            ]
        }
        """
        for i in range(len(msg["data"])):
            data = self.parse_balance(msg["data"][i])
            data_to_log = {
                'feed': self.id,
                'timestamp': data.get('timestamp'),
                'currency': data.get('symbol'),
                'total': data.get('total'),
                'free': data.get('free'),
            }
            LOG.debug(f"Handle margin msg: {data_to_log}")
            await self.callback(BALANCES, feed=self.id, **data)

    async def _execution(self, msg: dict, timestamp: float):
        for i in range(len(msg['data'])):
            data = msg['data'][i]
            symbol = self.exchange_symbol_to_std_symbol(data['symbol'])
            await self.callback(USER_FILLS, feed=self.id, symbol=symbol, data=data, receipt_timestamp=timestamp)

    async def authenticate(self, conn: AsyncConnection):
        if any(self.is_authenticated_channel(self.exchange_channel_to_std(chan)) for chan in self.subscription):
            auth = self._auth(self.key_id, self.key_secret)
            LOG.debug(f"{conn.uuid}: Sending authentication request with message {auth}")
            await conn.write(auth)

    def _auth(self, key_id: str, key_secret: str) -> str:
        # https://bybit-exchange.github.io/docs/inverse/#t-websocketauthentication

        expires = int((time.time() + 60)) * 1000
        signature = str(hmac.new(bytes(key_secret, 'utf-8'), bytes(f'GET/realtime{expires}', 'utf-8'), digestmod='sha256').hexdigest())
        return json.dumps({'op': 'auth', 'args': [key_id, expires, signature]})

    async def __no_auth(self, conn: AsyncConnection):
        pass
