'''
Copyright (C) 2017-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import json

import aioredis

from cryptofeed.backends.backend import BackendBookCallback, BackendBookDeltaCallback, BackendTickerCallback, BackendTradeCallback, BackendFundingCallback, BackendOrderCallback
from cryptofeed.util.lock import redis_order_lock


class RedisCallback:
    def __init__(self, host='127.0.0.1', port=6379, socket=None, key=None, numeric_type=float, **kwargs):
        """
        setting key lets you override the prefix on the
        key used in redis. The defaults are related to the data
        being stored, i.e. trade, funding, etc
        """
        self.redis = None
        self.key = key if key else self.default_key
        self.numeric_type = numeric_type
        self.conn_str = socket if socket else f'redis://{host}:{port}'


class RedisZSetCallback(RedisCallback):
    async def write(self, feed: str, pair: str, timestamp: float, data: dict):
        data = json.dumps(data)
        if self.redis is None:
            self.redis = await aioredis.create_redis_pool(self.conn_str, encoding='utf-8')
        await self.redis.zadd(f"{self.key}-{feed}-{pair}", timestamp, data, exist=self.redis.ZSET_IF_NOT_EXIST)


class RedisStreamCallback(RedisCallback):
    async def write(self, feed: str, pair: str, timestamp: float, data: dict):
        if self.redis is None:
            self.redis = await aioredis.create_redis_pool(self.conn_str, encoding='utf-8')
        await self.redis.xadd(f"{self.key}-{feed}-{pair}", data)


class RedisOrderCallback(RedisCallback):
    async def write(self, feed: str, pair: str, data: dict):
        redis_key = f"{self.key}-{feed}-{pair}"
        if self.redis is None:
            self.redis = await aioredis.create_redis_pool(self.conn_str, encoding='utf-8')

        async with redis_order_lock:
            order = {}
            order_str = await self.redis.get(redis_key)

            if order_str:
                order = json.loads(order_str)

                if order is None:
                    order = {}

            unhandled_amount = order.get('unhandled_amount', 0)
            previous_filled_amount = order.get('filled', 0)
            current_filled_amount = data.get('filled', 0)
            new_filled_amount = current_filled_amount - previous_filled_amount
            unhandled_amount += new_filled_amount

            order.update(data)
            order['unhandled_amount'] = unhandled_amount

            await self.redis.set(f"{self.key}-{feed}-{pair}", json.dumps(order))


class TradeRedis(RedisZSetCallback, BackendTradeCallback):
    default_key = 'trades'


class TradeStream(RedisStreamCallback, BackendTradeCallback):
    default_key = 'trades'


class FundingRedis(RedisZSetCallback, BackendFundingCallback):
    default_key = 'funding'


class FundingStream(RedisStreamCallback, BackendFundingCallback):
    default_key = 'funding'


class BookRedis(RedisZSetCallback, BackendBookCallback):
    default_key = 'book'


class BookDeltaRedis(RedisZSetCallback, BackendBookDeltaCallback):
    default_key = 'book'


class BookStream(RedisStreamCallback, BackendBookCallback):
    default_key = 'book'

    async def write(self, feed, pair, timestamp, data):
        data = {'data': json.dumps(data)}
        await super().write(feed, pair, timestamp, data)


class BookDeltaStream(RedisStreamCallback, BackendBookDeltaCallback):
    default_key = 'book'

    async def write(self, feed, pair, timestamp, data):
        data = {'data': json.dumps(data)}
        await super().write(feed, pair, timestamp, data)


class TickerRedis(RedisZSetCallback, BackendTickerCallback):
    default_key = 'ticker'


class TickerStream(RedisStreamCallback, BackendTickerCallback):
    default_key = 'ticker'


class OrderRedis(RedisOrderCallback, BackendOrderCallback):
    default_key = 'order'
