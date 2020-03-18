import logging
import time

from cryptofeed.defines import BINANCE_FUTURES
from cryptofeed.rest.binance import Binance

LOG = logging.getLogger('rest')


class BinanceFutures(Binance):
    # NOTE(boseok): market에 따라 다음과 같이 class를 구분한다.
    # spot -> Binance
    # margin -> BinanceMargin(Binance)
    # futures -> BinanceFutures(Binance)

    ID = BINANCE_FUTURES
    api = 'https://fapi.binance.com'

    def create_listen_key(self):
        """
        Create listenKey for user data stream

        futures: POST /fapi/v1/listenKey
        https://binance-docs.github.io/apidocs/futures/en/#start-user-data-stream-user_stream

        Returns:
        {
           "listenKey": "pqia91ma19a5s61cv6a81va65sdf19v8a65a1a5s61cv6a81va65sdf19v8a65a1"
        }
        """
        endpoint = '/fapi/v1/listenKey'
        params = {
            'recvWindow': 5000,
            'timestamp': int(time.time() * 1000),
        }
        return self._post(endpoint, params, True)

    def keepalive_listen_key(self, listen_key: str):
        """
        Keepalive a user data stream to prevent a time out

        futures: PUT /fapi/v1/listenKey
        https://binance-docs.github.io/apidocs/futures/en/#keepalive-user-data-stream-user_stream

        Args:
            listen_key(str):
        """
        endpoint = '/fapi/v1/listenKey'
        params = {
            'listenKey': listen_key
        }
        return self._put(endpoint, params, True)
