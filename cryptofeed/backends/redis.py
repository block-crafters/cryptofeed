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
        self.recentOnly = self.recentOnly
        self.authorized = self.authorized
        self.numeric_type = numeric_type

class RedisHashCallback(RedisCallback):
    def __init__(self, host='127.0.0.1', port=6379, socket=None, key=None, numeric_type=float, score_key='timestamp', **kwargs):
        self.score_key = score_key
        super().__init__(host=host, port=port, socket=socket, key=key, numeric_type=numeric_type, **kwargs)
    
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        score = data[self.score_key]
        await self.queue.put({'feed': feed, 'symbol': symbol, 'score': score, 'data': data})
    
    async def writer(self):
        while True:
            count = self.queue.qsize()
            if count > 1:
                async with self.read_many_queue(count) as updates:
                    async with self.redis.pipeline(transaction=False) as pipe:
                        print(f"count >1: RecentOnly-{self.recentOnly}; Authorized-{self.authorized}")
                        for update in updates:
                            if self.authorized:
                                for k, v in update['data'].items():
                                    redis_v = pipe.hget(f"{self.key}-{update['data']['account']}-{update['feed']}-{update['symbol']}", k)
                                    if redis_v is not None and redis_v != str(v):
                                        pipe = pipe.hset(f"{self.key}-{update['data']['account']}-{update['feed']}-{update['symbol']}", k, str(v))
                            else:
                                for k, v in update['data'].items():
                                    redis_v = pipe.hget(f"{self.key}-{update['feed']}-{update['symbol']}", k)
                                    if redis_v is not None and redis_v != str(v):
                                        pipe = pipe.hset(f"{self.key}-{update['feed']}-{update['symbol']}", k, str(v))

                            if not self.recentOnly and self.authorized:
                                data = json.dumps(update['data'])
                                pipe = pipe.zadd(f"{self.key}_collected-{update['data']['account']}-{update['feed']}-{update['symbol']}", {data: update['score']}, nx=True)
                            if not self.recentOnly and not self.authorized:
                                data = json.dumps(update['data'])
                                pipe = pipe.zadd(f"{self.key}_collected-{update['feed']}-{update['symbol']}", {data: update['score']}, nx=True)
                        await pipe.execute()
            else:
                async with self.read_queue() as update:
                    async with self.redis.pipeline(transaction=False) as pipe:
                        # if self.authorized:
                        #         self.key += f"-{update['data']['account']}"
                        print(f"RecentOnly-{self.recentOnly}; Authorized-{self.authorized}")
                        if self.authorized:
                            for k, v in update['data'].items():
                                redis_v = pipe.hget(f"{self.key}-{update['data']['account']}-{update['feed']}-{update['symbol']}", k)
                                if redis_v is not None and redis_v != str(v):
                                    await self.redis.hset(f"{self.key}-{update['data']['account']}-{update['feed']}-{update['symbol']}", k, str(v))
                        else:
                            for k, v in update['data'].items():
                                redis_v = pipe.hget(f"{self.key}-{update['feed']}-{update['symbol']}", k)
                                if redis_v is not None and redis_v != str(v):
                                    await self.redis.hset(f"{self.key}-{update['feed']}-{update['symbol']}", k, str(v))
                        if not self.recentOnly and self.authorized:
                                data = json.dumps(update['data'])
                                pipe = pipe.zadd(f"{self.key}_collected-{update['data']['account']}-{update['feed']}-{update['symbol']}", {data: update['score']}, nx=True)
                        if not self.recentOnly and not self.authorized:
                            data = json.dumps(update['data'])
                            pipe = pipe.zadd(f"{self.key}_collected-{update['feed']}-{update['symbol']}", {data: update['score']}, nx=True)

# class RedisZSetCallback(RedisCallback):
#     def __init__(self, host='127.0.0.1', port=6379, socket=None, key=None, numeric_type=float, score_key='timestamp', **kwargs):
#         """
#         score_key: str
#             the value at this key will be used to store the data in the ZSet in redis. The
#             default is timestamp. If you wish to look up the data by a different value,
#             use this to change it. It must be a numeric value.
#         """
#         self.score_key = score_key
#         super().__init__(host=host, port=port, socket=socket, key=key, numeric_type=numeric_type, **kwargs)

#     async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
#         score = data[self.score_key]
#         # data = json.dumps(data)
#         await self.queue.put({'feed': feed, 'symbol': symbol, 'score': score, 'data': data})

#     async def writer(self):
#         while True:

#             count = self.queue.qsize()
#             if count > 1:
#                 async with self.read_many_queue(count) as updates:
#                     async with self.redis.pipeline(transaction=False) as pipe:
#                         for update in updates:
#                             for k, v in update['data'].items():
#                                 redis_v = pipe.hget(f"{self.key}-{update['feed']}-{update['symbol']}-{update['data']['order_id']}", k)
#                                 if redis_v is not None and redis_v != str(v):
#                                     pipe = pipe.hset(f"{self.key}-{update['feed']}-{update['symbol']}", k, str(v))
#                             data = json.dumps(update['data'])
#                             pipe = pipe.zadd(f"{self.key}-{update['feed']}-{update['symbol']}", {data: update['score']}, nx=True)
#                         await pipe.execute()
#             else:
#                 async with self.read_queue() as update:
#                     async with self.redis.pipeline(transaction=False) as pipe:
#                         for k, v in update['data'].items():
#                             redis_v = pipe.hget(f"{self.key}-{update['feed']}-{update['symbol']}", k)
#                             if redis_v is not None and redis_v != str(v):
#                                 await self.redis.hset(f"{self.key}-{update['feed']}-{update['symbol']}", k, str(v))
#                     # await self.redis.zadd(f"{self.key}-{update['feed']}-{update['symbol']}", {update['data']: update['score']}, nx=True)


class RedisStreamCallback(RedisCallback):
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        await self.queue.put({'feed': feed, 'symbol': symbol, 'data': data})

    async def writer(self):
        while True:
            count = self.queue.qsize()
            if count > 1:
                async with self.read_many_queue(count) as updates:
                    async with self.redis.pipeline(transaction=False) as pipe:
                        for update in updates:
                            pipe = pipe.xadd(f"{self.key}-{update['feed']}-{update['symbol']}", update['data'])
                        await pipe.execute()
            else:
                async with self.read_queue() as update:
                    await self.redis.xadd(f"{self.key}-{update['feed']}-{update['symbol']}", update['data'])


class TradeRedis(RedisHashCallback, BackendTradeCallback):
    default_key = 'trades'
    recentOnly = False
    authorized = False
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        trades_none_to_str(data)
        await super().write(feed, symbol, timestamp, receipt_timestamp, data)


class TradeStream(RedisStreamCallback, BackendTradeCallback):
    default_key = 'trades'
    authorized = False
    recentOnly = False
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        trades_none_to_str(data)
        await super().write(feed, symbol, timestamp, receipt_timestamp, data)


class FundingRedis(RedisHashCallback, BackendFundingCallback):
    default_key = 'funding'
    authorized = False
    recentOnly = True


class FundingStream(RedisStreamCallback, BackendFundingCallback):
    default_key = 'funding'
    authorized = False
    recentOnly = True


class BookRedis(RedisHashCallback, BackendBookCallback):
    default_key = 'book'
    authorized = False
    recentOnly = True


class BookDeltaRedis(RedisHashCallback, BackendBookDeltaCallback):
    default_key = 'book'
    authorized = False
    recentOnly = True


class BookStream(RedisStreamCallback, BackendBookCallback):
    default_key = 'book'
    authorized = False
    recentOnly = True
    async def write(self, feed: str, symbol: str, timestamp: float, receipt_timestamp: float, data: dict):
        data['delta'] = 'False'
        data['bid'] = json.dumps(data['bid'])
        data['ask'] = json.dumps(data['ask'])

        await super().write(feed, symbol, timestamp, receipt_timestamp, data)


class BookDeltaStream(RedisStreamCallback, BackendBookDeltaCallback):
    default_key = 'book'
    authorized = False
    recentOnly = True
    async def write(self, feed: str, symbol: str, timestamp: str, receipt_timestamp: float, data: dict):
        data['delta'] = 'True'
        data['bid'] = json.dumps(data['bid'])
        data['ask'] = json.dumps(data['ask'])

        await super().write(feed, symbol, timestamp, receipt_timestamp, data)


class TickerRedis(RedisHashCallback, BackendTickerCallback):
    default_key = 'ticker'
    authorized = False
    recentOnly = True


class TickerStream(RedisStreamCallback, BackendTickerCallback):
    default_key = 'ticker'
    authorized = False
    recentOnly = True


class OpenInterestRedis(RedisHashCallback, BackendOpenInterestCallback):
    default_key = 'open_interest'
    authorized = False
    recentOnly = True


class OpenInterestStream(RedisStreamCallback, BackendOpenInterestCallback):
    default_key = 'open_interest'
    authorized = False
    recentOnly = True


class LiquidationsRedis(RedisHashCallback, BackendLiquidationsCallback):
    default_key = 'liquidations'
    authorized = False
    recentOnly = True


class LiquidationsStream(RedisStreamCallback, BackendLiquidationsCallback):
    default_key = 'liquidations'
    authorized = False
    recentOnly = True


class CandlesRedis(RedisHashCallback, BackendCandlesCallback):
    default_key = 'candles'
    authorized = False
    recentOnly = True


class CandlesStream(RedisStreamCallback, BackendCandlesCallback):
    default_key = 'candles'
    authorized = False
    recentOnly = True

class PositionsRedis(RedisHashCallback, BackendPositionsCallback):
    default_key = 'positions'
    authorized = True
    recentOnly = True


class PositionsStream(RedisStreamCallback, BackendPositionsCallback):
    default_key = 'positions'
    authorized = True
    recentOnly = True


class BalancesRedis(RedisHashCallback, BackendBalancesCallback):
    default_key = 'balances'
    authorized = True
    recentOnly = True


class BalancesStream(RedisStreamCallback, BackendBalancesCallback):
    default_key = 'balances'
    authorized = True
    recentOnly = True


class OrderInfoRedis(RedisHashCallback, BackendOrderInfoCallback):
    default_key = 'order_info'
    authorized = True
    recentOnly = False


class OrderInfoStream(RedisStreamCallback, BackendOrderInfoCallback):
    default_key = 'order_info'
    authorized = True
    recentOnly = False