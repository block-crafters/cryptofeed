import logging

from cryptofeed.defines import BINANCE_FUTURES
from cryptofeed.rest.binance import Binance

LOG = logging.getLogger('rest')


class BinanceFutures(Binance):
    # spot, margin, futures class를 api가 같은 것 끼리 묶는다.
    # spot, margin -> Binance
    # futures -> BinanceFutures
    ID = BINANCE_FUTURES
    api = 'https://fapi.binance.com'
