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

def convert_to_str(data):
    for k, v in data.items():
        if v is None:
            data[k] = ''
        else:
            data[k] = str(v)


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


class RedisZSetCallback(RedisCallback):
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
                            key = update['key']
                            data = json.dumps(update['data'])
                            score = update['score']
                            pipe = pipe.zadd(key, {data: score}, nx= True)
                        await pipe.execute()
            else:
                async with self.read_queue() as update:
                    key = update['key']
                    data = json.dumps(update['data'])
                    score = update['score']
                    await self.redis.zadd(key, {data: score}, nx= True)


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
                            key = update['key']
                            data = update['data']
                            convert_to_str(data)
                            pipe = pipe.hmset(key, data)
                        await pipe.execute()
            else:
                async with self.read_queue() as update:
                    async with self.redis.pipeline(transaction=False) as pipe:
                        key = update['key']
                        data = update['data']
                        convert_to_str(data)
                        await self.redis.hmset(key, data)


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
                            key = update['key']
                            data = update['data']
                            convert_to_str(data)
                            pipe = pipe.xadd(key, data)
                        await pipe.execute()
            else:
                async with self.read_queue() as update:
                    key = update['key']
                    data = update['data']
                    convert_to_str(data)
                    await self.redis.xadd(key, data)


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
    default_key = 'balances_latest'
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        account = data['account']
        key = f"{self.key}:{feed}:{account}"
        if symbol is not None:
            key += f":{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class BalancesStream(RedisStreamCallback, BackendBalancesCallback):
    default_key = 'balances_stream'
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        account = data['account']
        key = f"{self.key}:{feed}:{account}"
        if symbol is not None:
            key += f":{symbol}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)


class OrderInfoLatestRedis(RedisHashCallback, BackendOrderInfoCallback):
    default_key = 'order_info_latest'
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        order_id = data['order_id']
        account = data['account']
        key = f"{self.key}:{feed}:{account}:{symbol}:{order_id}"
        await super().write(feed, symbol, timestamp, receipt_timestamp, data, key)