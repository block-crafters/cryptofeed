'''
Copyright (C) 2017-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import os
import asyncio
import json
import logging
from decimal import Decimal

import aiohttp
from sortedcontainers import SortedDict as sd

from cryptofeed.feed import Feed
from cryptofeed.defines import TICKER, TRADES, ORDER, BUY, SELL, BID, ASK, L2_BOOK, BINANCE_MARGIN
from cryptofeed.exchange.binance import Binance
from cryptofeed.rest.binance_margin import BinanceMargin as RestBinanceMargin
from cryptofeed.standards import pair_exchange_to_std, timestamp_normalize, feed_to_exchange


LOG = logging.getLogger('feedhandler')


class BinanceMargin(Binance):
    id = BINANCE_MARGIN

    def __init__(self, pairs=None, channels=None, callbacks=None, depth=1000, **kwargs):
        Feed.__init__(self, None, pairs=pairs, channels=channels, callbacks=callbacks, **kwargs)

        if self.use_private_channels:
            self.key_id = os.environ.get('BINANCE_API_KEY')
            self.key_secret = os.environ.get('BINANCE_SECRET_KEY')

        self.rest_client = RestBinanceMargin()
        self.book_depth = depth
        self.ws_endpoint = 'wss://stream.binance.com:9443'
        self.rest_endpoint = 'https://www.binance.com/api/v1'
        self.address = self._address()
        self._reset()
