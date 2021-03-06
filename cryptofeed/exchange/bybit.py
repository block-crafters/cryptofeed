import os
import logging
import json
from decimal import Decimal

from sortedcontainers import SortedDict as sd

from cryptofeed.feed import Feed
from cryptofeed.defines import BYBIT, BUY, SELL, TRADES, BID, ASK, L2_BOOK, ORDER
from cryptofeed.rest.bybit import Bybit as RestBybit
from cryptofeed.standards import feed_to_exchange, timestamp_normalize, pair_exchange_to_std as normalize_pair


LOG = logging.getLogger('feedhandler')


class Bybit(Feed):
    id = BYBIT
    private_channels = ['position', 'execution', 'order', 'stop_order']

    def __init__(self, pairs=None, channels=None, callbacks=None, **kwargs):
        super().__init__('wss://stream.bybit.com/realtime', pairs=pairs, channels=channels, callbacks=callbacks, **kwargs)

        if kwargs.get('use_private_channels'):
            self.key_id = os.environ.get('BYBIT_API_KEY')
            self.key_secret = os.environ.get('BYBIT_SECRET_KEY')

    def __reset(self):
        self.l2_book = {}

    async def message_handler(self, msg: str, timestamp: float):
        msg = json.loads(msg)

        if "success" in msg:
            if msg['success']:
                LOG.debug("%s: Subscription success %s", self.id, msg)
            else:
                LOG.error("%s: Error from exchange %s", self.id, msg)
        elif "trade" in msg["topic"]:
            await self._trade(msg)
        elif "orderBookL2_25" in msg["topic"]:
            await self._book(msg)
        elif 'order' == msg['topic']:
            
            await self._order(msg)
        else:
            LOG.warning("%s: Invalid message type %s", self.id, msg)

    async def authenticate(self, websocket):
        """
        Reference:
            https://github.com/bybit-exchange/bybit-official-api-docs/blob/master/en/websocket.md#authentication
        """
        auth_info = RestBybit.generate_signature('GET', '/realtime', key_id=self.key_id, key_secret=self.key_secret)
        expires = int(auth_info.get('api-expires'))
        key_id = auth_info.get('api-key')
        signature = auth_info.get('api-signature')
        await websocket.send(json.dumps({"op": "auth",
                                         "args": [key_id, expires, signature]}))

    async def subscribe(self, websocket):
        self.__reset()
        for chan in self.channels if self.channels else self.config:
            for pair in self.pairs if self.pairs else self.config[chan]:
                if chan in self.private_channels:
                    topic = chan
                else:
                    topic = f'{chan}.{pair}'

                await websocket.send(json.dumps(
                    {
                        "op": "subscribe",
                        "args": [topic]
                    }
                ))

    @staticmethod
    def parse_order_status(status):
        statuses = {
            'Created': 'open',
            'New': 'open',
            'PartiallyFilled': 'open',
            'Filled': 'closed',
            'Cancelled': 'canceled',
            'Rejected': 'rejected',
            'Untriggered': 'open',
            'Triggered': 'open',
            'Active': 'open',
        }

        ret = statuses.get(status, None)
        return ret
    
    def parse_order(self, data):
        ts = timestamp_normalize(self.id, data['timestamp'])
        order = {
            'order_id': data['order_id'],
            'client_order_id': data.get('order_link_id', ''),
            'timestamp': ts
        }
        if data.get('side'):
            order['side'] = BUY if data['side'] == 'Buy' else SELL
        if data.get('order_status'):
            order['status'] = Bybit.parse_order_status(data['order_status'])
        
        # 0 should be True at `if` statement.
        if data.get('qty') is not None:
            order['amount'] = data['qty']
        if data.get('cum_exec_qty') is not None:
            order['filled'] = data['cum_exec_qty']
        if data.get('leaves_qty') is not None:
            order['remaining'] = data['leaves_qty']
        if data.get('price') is not None:
            order['price'] = float(data['price'])
        average = None
        cum_exec_value = float(data['cum_exec_value']) if data['cum_exec_value'] is not None else None
        if cum_exec_value and cum_exec_value > 0 and order['filled'] and order['filled'] > 0:
            average = order['filled'] / cum_exec_value
        order['average'] = average
        
        return order

    async def _trade(self, msg):
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
            await self.callback(TRADES,
                feed=self.id,
                pair=normalize_pair(trade['symbol']),
                order_id=trade['trade_id'],
                side=BUY if trade['side'] == 'Buy' else SELL,
                amount=Decimal(trade['size']),
                price=Decimal(trade['price']),
                timestamp=timestamp_normalize(self.id, trade['timestamp'])
            )

    async def _book(self, msg):
        pair = normalize_pair(msg['topic'].split('.')[1])
        update_type = msg['type']
        data = msg['data']
        forced = False
        delta = {BID: [], ASK: []}


        if update_type == 'snapshot':
            self.l2_book[pair] = {BID: sd({}), ASK: sd({})}
            for update in data:
                side = BID if update['side'] == 'Buy' else ASK
                self.l2_book[pair][side][Decimal(update['price'])] = Decimal(update['size'])
            forced = True
        else:
            for delete in data['delete']:
                side = BID if delete['side'] == 'Buy' else ASK
                price = Decimal(delete['price'])
                delta[side].append((price, 0))
                del self.l2_book[pair][side][price]

            for utype in ('update', 'insert'):
                for update in data[utype]:
                    side = BID if update['side'] == 'Buy' else ASK
                    price = Decimal(update['price'])
                    amount = Decimal(update['size'])
                    delta[side].append((price, amount))
                    self.l2_book[pair][side][price] = amount

        # timestamp is in microseconds
        await self.book_callback(self.l2_book[pair], L2_BOOK, pair, forced, delta, msg['timestamp_e6'] / 1000000)

    async def _order(self, msg):
        if self.config:
            channel = feed_to_exchange(self.id, ORDER)
            pairs = list(self.config[channel])
        else:
            pairs = self.pairs

        for data in msg['data']:
            if data.get('order_status') and data.get('symbol', '') in pairs:
                new_info = self.parse_order(data)
                await self.callback(ORDER, feed=self.id, pair=normalize_pair(data['symbol']), **new_info)
