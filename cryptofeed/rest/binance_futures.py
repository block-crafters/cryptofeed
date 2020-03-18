import logging
import time

from cryptofeed.defines import BINANCE_FUTURES
from cryptofeed.rest.binance import Binance

LOG = logging.getLogger('rest')


class BinanceFutures(Binance):
    # spot, margin, futures class를 api가 같은 것 끼리 묶는다.
    # spot, margin -> Binance
    # futures -> BinanceFutures
    ID = BINANCE_FUTURES
    api = 'https://fapi.binance.com'

    def create_listen_key(self):
        """
        spot: POST /api/v3/userDataStream
        margin: POST /sapi/v1/userDataStream
        https://binance-docs.github.io/apidocs/spot/en/#listen-key-spot
        https://binance-docs.github.io/apidocs/spot/en/#listen-key-margin

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

    def keepalive_listen_key(self, listen_key):
        """
        spot: PUT /api/v3/userDataStream
        margin: PUT /sapi/v1/userDataStream
        https://binance-docs.github.io/apidocs/spot/en/#listen-key-spot
        https://binance-docs.github.io/apidocs/spot/en/#listen-key-margin

        {
           "listenKey": "pqia91ma19a5s61cv6a81va65sdf19v8a65a1a5s61cv6a81va65sdf19v8a65a1"
        }
        """
        endpoint = '/fapi/v1/listenKey'
        params = {
            'listenKey': listen_key
        }
        return self._put(endpoint, params, True)
