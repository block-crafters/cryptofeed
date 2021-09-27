'''
Copyright (C) 2017-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import aioredis
from yapic import json
import logging

from cryptofeed.backends.backend import (BackendQueue, BackendBookCallback, BackendCandlesCallback, BackendBookDeltaCallback, BackendFundingCallback,
                                         BackendOpenInterestCallback, BackendTickerCallback, BackendTradeCallback,
                                         BackendLiquidationsCallback, BackendPositionsCallback, BackendBalancesCallback,
                                         BackendOrderInfoCallback)

LOG = logging.getLogger('feedhandler')

def trades_none_to_str(data):
    if data['order_type'] is None:
        data['order_type'] = 'None'
    if data['id'] is None:
        data['id'] = 'None'


class RedisCallback(BackendQueue):
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

class RedisHashCallback(RedisCallback):
    def __init__(self, host='127.0.0.1', port=6379, socket=None, key=None, numeric_type=float, score_key='timestamp', **kwargs):
        self.score_key = score_key
        super().__init__(host=host, port=port, socket=socket, key=key, numeric_type=numeric_type, **kwargs)
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict, key=None):
        score = data[self.score_key]
        await self.queue.put({'feed': feed, 'symbol': symbol, 'score': score, 'data': data, 'key': key})

    async def writer(self):
        while True:
            count = self.queue.qsize()
            if count > 1:
                async with self.read_many_queue(count) as updates:
                    async with self.redis.pipeline(transaction=False) as pipe:
                        for update in updates:
                            feed = update['feed']
                            symbol = update['symbol']
                            key = update['key']
                            for k, v in update['data'].items():
                                redis_v = pipe.hget(key, k)
                                if redis_v is not None and redis_v != str(v):
                                    pipe = pipe.hset(key, k, str(v))
                            
                            if not self.recent_only and self.authorized:
                                data = json.dumps(update['data'])
                                account = update['data']['account']
                                pipe = pipe.zadd(f"{self.key}_collected:{account}:{feed}:{symbol}", {data: update['score']}, nx=True)
                            if not self.recent_only and not self.authorized:
                                data = json.dumps(update['data'])
                                pipe = pipe.zadd(f"{self.key}_collected:{feed}:{symbol}", {data: update['score']}, nx=True)
                        await pipe.execute()
            else:
                async with self.read_queue() as update:
                    async with self.redis.pipeline(transaction=False) as pipe:
                        feed = update['feed']
                        symbol = update['symbol']
                        key = update['key']
                        for k, v in update['data'].items():
                            redis_v = pipe.hget(key, k)
                            if redis_v is not None and redis_v != str(v):
                                await self.redis.hset(key, k, str(v))
                    
                        if not self.recent_only and self.authorized:
                                account = update['data']['account']
                                data = json.dumps(update['data'])
                                pipe = pipe.zadd(f"{self.key}_collected:{account}:{feed}:{symbol}", {data: update['score']}, nx=True)
                        if not self.recent_only and not self.authorized:
                            data = json.dumps(update['data'])
                            pipe = pipe.zadd(f"{self.key}_collected:{feed}:{symbol}", {data: update['score']}, nx=True)
                        await pipe.execute()


class RedisStreamCallback(RedisCallback):
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict, key=None):
        await self.queue.put({'feed': feed, 'symbol': symbol, 'data': data, 'key': key})

    async def writer(self):
        while True:
            count = self.queue.qsize()
            if count > 1:
                async with self.read_many_queue(count) as updates:
                    async with self.redis.pipeline(transaction=False) as pipe:
                        for update in updates:
                            feed = update['feed']
                            symbol = update['symbol']
                            data = update['data']
                            pipe = pipe.xadd(f"{self.key}:{feed}:{symbol}", data)
                        await pipe.execute()
            else:
                async with self.read_queue() as update:
                    feed = update['feed']
                    symbol = update['symbol']
                    data = update['data']
                    await self.redis.xadd(f"{self.key}:{feed}:{symbol}", data)


class TradeRedis(RedisHashCallback, BackendTradeCallback):
    default_key = 'trades'
    recent_only = False
    authorized = False
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        trades_none_to_str(data)
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class TradeStream(RedisStreamCallback, BackendTradeCallback):
    default_key = 'trades'
    authorized = False
    recent_only = False
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        trades_none_to_str(data)
        await super().write(feed, symbol, timestamp, receipt_timestamp, data)


class FundingRedis(RedisHashCallback, BackendFundingCallback):
    default_key = 'funding'
    authorized = False
    recent_only = True
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class FundingStream(RedisStreamCallback, BackendFundingCallback):
    default_key = 'funding'
    authorized = False
    recent_only = True
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class BookRedis(RedisHashCallback, BackendBookCallback):
    default_key = 'book'
    authorized = False
    recent_only = True
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class BookDeltaRedis(RedisHashCallback, BackendBookDeltaCallback):
    default_key = 'book'
    authorized = False
    recent_only = True
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class BookStream(RedisStreamCallback, BackendBookCallback):
    default_key = 'book'
    authorized = False
    recent_only = True
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        data['delta'] = 'False'
        data['bid'] = json.dumps(data['bid'])
        data['ask'] = json.dumps(data['ask'])
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class BookDeltaStream(RedisStreamCallback, BackendBookDeltaCallback):
    default_key = 'book'
    authorized = False
    recent_only = True
    async def write(self, feed: str, symbol: str, timestamp: str, receipt_timestamp: float, data: dict):
        data['delta'] = 'True'
        data['bid'] = json.dumps(data['bid'])
        data['ask'] = json.dumps(data['ask'])
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class TickerRedis(RedisHashCallback, BackendTickerCallback):
    default_key = 'ticker'
    authorized = False
    recent_only = True
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class TickerStream(RedisStreamCallback, BackendTickerCallback):
    default_key = 'ticker'
    authorized = False
    recent_only= True


class OpenInterestRedis(RedisHashCallback, BackendOpenInterestCallback):
    default_key = 'open_interest'
    authorized = False
    recent_only = True
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class OpenInterestStream(RedisStreamCallback, BackendOpenInterestCallback):
    default_key = 'open_interest'
    authorized = False
    recent_only = True


class LiquidationsRedis(RedisHashCallback, BackendLiquidationsCallback):
    default_key = 'liquidations'
    authorized = False
    recent_only = True
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class LiquidationsStream(RedisStreamCallback, BackendLiquidationsCallback):
    default_key = 'liquidations'
    authorized = False
    recent_only = True


class CandlesRedis(RedisHashCallback, BackendCandlesCallback):
    default_key = 'candles'
    authorized = False
    recent_only = True
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        key = f"{self.key}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class CandlesStream(RedisStreamCallback, BackendCandlesCallback):
    default_key = 'candles'
    authorized = False
    recent_only = True

class PositionsRedis(RedisHashCallback, BackendPositionsCallback):
    default_key = 'positions'
    authorized = True
    recent_only = True
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        account = data['account']
        key = f"{self.key}:{account}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class PositionsStream(RedisStreamCallback, BackendPositionsCallback):
    default_key = 'positions'
    authorized = True
    recent_only = True


class BalancesRedis(RedisHashCallback, BackendBalancesCallback):
    default_key = 'balances'
    authorized = True
    recent_only = True
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        account = data['account']
        key = f"{self.key}:{account}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class BalancesStream(RedisStreamCallback, BackendBalancesCallback):
    default_key = 'balances'
    authorized = True
    recent_only = True


class OrderInfoRedis(RedisHashCallback, BackendOrderInfoCallback):
    default_key = 'order_info'
    authorized = True
    recent_only = False
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        order_id = data['order_id']
        account = data['account']
        key = f"{self.key}:{account}:{order_id}:{feed}:{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)

class OrderInfoStream(RedisStreamCallback, BackendOrderInfoCallback):
    default_key = 'order_info'
    authorized = True
    recent_only = False