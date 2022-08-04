'''
Copyright (C) 2017-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import logging
import time
from typing import ClassVar, Optional

import aioredis
import aioredis.client
from yapic import json

from cryptofeed.backends.backend import (BackendQueue, BackendBookCallback, BackendCandlesCallback, BackendBookDeltaCallback, BackendFundingCallback,
                                         BackendOpenInterestCallback, BackendTickerCallback, BackendTradeCallback,
                                         BackendLiquidationsCallback, BackendPositionsCallback, BackendBalancesCallback,
                                         BackendOrderInfoCallback)

LOG = logging.getLogger('feedhandler')

def convert_to_str(data):
    for k, v in data.items():
        if v is None:
            data[k] = ''
        else:
            data[k] = str(v)


class RedisCallback(BackendQueue):

    forced_timestamp_key: ClassVar[Optional[str]] = None
    timestamp_key_suffix: ClassVar[str] = "__timestamp"

    def __init__(self, host='127.0.0.1', port=6379, socket=None, key=None, numeric_type=float, **kwargs):
        """
        setting key lets you override the prefix on the
        key used in redis. The defaults are related to the data
        being stored, i.e. trade, funding, etc
        """
        prefix = 'redis://'
        if socket:
            prefix = 'unix://'

        self.redis = aioredis.from_url(f"{prefix}{host}:{port}")
        self.key = key if key else self.default_key
        self.numeric_type = numeric_type

    def set_timestamp(self, pipe: aioredis.client.Pipeline, key: Optional[str] = None):
        timestamp_key = (type(self).forced_timestamp_key or key or self.key) + ":" + type(self).timestamp_key_suffix
        return pipe.set(timestamp_key, time.time())


class RedisZSetCallback(RedisCallback):
    def __init__(self, host='127.0.0.1', port=6379, socket=None, key=None, numeric_type=float, score_key='timestamp', **kwargs):
        self.score_key = score_key
        super().__init__(host=host, port=port, socket=socket, key=key, numeric_type=numeric_type, **kwargs)

    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict, key=None):
        score = data[self.score_key]
        await self.queue.put({'feed': feed, 'symbol': symbol, 'score': score, 'data': data, 'key': key})

    async def writer(self):
        while True:
            async with self.read_many_queue() as updates, self.redis.pipeline(transaction=False) as pipe:
                for update in updates:
                    key = update['key']
                    data = json.dumps(update['data'])
                    score = update['score']
                    pipe = pipe.zadd(key, {data: score}, nx= True)
                    pipe = self.set_timestamp(pipe, key)
                await pipe.execute()


class RedisHashCallback(RedisCallback):
    def __init__(self, host='127.0.0.1', port=6379, socket=None, key=None, numeric_type=float, score_key='timestamp', **kwargs):
        self.score_key = score_key
        super().__init__(host=host, port=port, socket=socket, key=key, numeric_type=numeric_type, **kwargs)
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict, key=None):
        score = data[self.score_key]
        await self.queue.put({'feed': feed, 'symbol': symbol, 'score': score, 'data': data, 'key': key})

    async def writer(self):
        while True:
            async with self.read_many_queue() as updates, self.redis.pipeline(transaction=False) as pipe:
                for update in updates:
                    key = update['key']
                    data = update['data']
                    convert_to_str(data)
                    pipe = pipe.hmset(key, data)
                    pipe = self.set_timestamp(pipe, key)
                await pipe.execute()

class RedisStreamCallback(RedisCallback):
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict, key=None):
        await self.queue.put({'feed': feed, 'symbol': symbol, 'data': data, 'key': key})

    async def writer(self):
        while True:
            async with self.read_many_queue() as updates, self.redis.pipeline(transaction=False) as pipe:
                for update in updates:
                    key = update['key']
                    data = update['data']
                    convert_to_str(data)
                    pipe = pipe.xadd(key, data)
                    pipe = self.set_timestamp(pipe, key)
                await pipe.execute()

class TradeRedis(RedisZSetCallback, BackendTradeCallback):
    default_key = 'trades'
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class TradeStream(RedisStreamCallback, BackendTradeCallback):
    default_key = 'trades_stream'
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class TradeLatestRedis(RedisHashCallback, BackendTradeCallback):
    default_key = 'trade_latest'
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)

        
class FundingRedis(RedisZSetCallback, BackendFundingCallback):
    default_key = 'funding'
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class FundingStream(RedisStreamCallback, BackendFundingCallback):
    default_key = 'funding_stream'
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class BookRedis(RedisHashCallback, BackendBookCallback):
    default_key = 'book'
    authorized = False
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)

class BookLatestRedis(RedisHashCallback, BackendBookCallback):
    default_key = 'book_latest'
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        data['bid'] = json.dumps(data['bid'])
        data['ask'] = json.dumps(data['ask'])
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class BookStream(RedisStreamCallback, BackendBookCallback):
    default_key = 'book_stream'
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        data['delta'] = 'False'
        data['bid'] = json.dumps(data['bid'])
        data['ask'] = json.dumps(data['ask'])
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class BookDeltaRedis(RedisZSetCallback, BackendBookDeltaCallback):
    default_key = 'book'
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class BookDeltaStream(RedisStreamCallback, BackendBookDeltaCallback):
    default_key = 'book_stream'
    authorized = False
    async def write(self, feed: str, symbol: str, timestamp: str, receipt_timestamp: float, data: dict):
        data['delta'] = 'True'
        data['bid'] = json.dumps(data['bid'])
        data['ask'] = json.dumps(data['ask'])
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class TickerRedis(RedisZSetCallback, BackendTickerCallback):
    default_key = 'ticker'
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class TickerStream(RedisStreamCallback, BackendTickerCallback):
    default_key = 'ticker_stream'
    authorized = False
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class TickerLatestRedis(RedisHashCallback, BackendTickerCallback):
    default_key = 'ticker_latest'
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class OpenInterestRedis(RedisZSetCallback, BackendOpenInterestCallback):
    default_key = 'open_interest'
    authorized = False
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class OpenInterestStream(RedisStreamCallback, BackendOpenInterestCallback):
    default_key = 'open_interest_stream'
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class LiquidationsRedis(RedisZSetCallback, BackendLiquidationsCallback):
    default_key = 'liquidations'
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class LiquidationsStream(RedisStreamCallback, BackendLiquidationsCallback):
    default_key = 'liquidations_stream'
    authorized = False
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class CandlesRedis(RedisZSetCallback, BackendCandlesCallback):
    default_key = 'candles'
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class CandlesStream(RedisStreamCallback, BackendCandlesCallback):
    default_key = 'candles_stream'
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class PositionsLatestRedis(RedisHashCallback, BackendPositionsCallback):
    default_key = 'positions_latest'
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        account = data['account']
        key = f"{self.key}:{feed}:{account}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class PositionsStream(RedisStreamCallback, BackendPositionsCallback):
    default_key = 'positions_stream'
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        account = data['account']
        key = f"{self.key}:{feed}:{account}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class BalancesLatestRedis(RedisHashCallback, BackendBalancesCallback):
    """
    Reference:
        Format:
            key: balances_stream:BITMEX:<ID>:BTC
                1) "timestamp"
                2) "1650615195.849"
                3) "account"
                4) "12345"
                5) "free"
                6) "9489102"
                7) "total"
                8) "11256525"
                9) "currency"
                10) "BTC"
                11) "feed"
                12) "BITMEX"
    """
    default_key = 'balances_latest'
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        account = data['account']
        currency = data['currency']
        key = f"{self.key}:{feed}:{account}:{currency}"
        if symbol is not None:
            key += f":{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class BalancesStream(RedisStreamCallback, BackendBalancesCallback):
    """
    Reference:
        Format:
            key: balances_stream:BITMEX:<ID>:BTC
            item:
                "1650615196099-0"
                    1) "timestamp"
                    2) "1650615195.849"
                    3) "account"
                    4) "12345"
                    5) "free"
                    6) "9489102"
                    7) "total"
                    8) "11256525"
                    9) "currency"
                    10) "BTC"
                    11) "feed"
                    12) "BITMEX"
    """
    default_key = 'balances_stream'
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        account = data['account']
        currency = data['currency']
        key = f"{self.key}:{feed}:{account}:{currency}"
        if symbol is not None:
            key += f":{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class OrderInfoLatestRedis(RedisHashCallback, BackendOrderInfoCallback):
    default_key = 'order_info'
    forced_timestamp_key: ClassVar[Optional[str]] = 'order_info'

    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        order_id = data['order_id']
        account = data['account']
        key = f"{self.key}:{feed}:{account}:{symbol}:{order_id}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)