import os
from dotenv import load_dotenv
import logging

from cryptofeed.rest.binance import Binance
from cryptofeed.defines import BINANCE_MARGIN

load_dotenv()
LOG = logging.getLogger('rest')

class BinanceMargin(Binance):
    # spot, margin, futures class를 api가 같은 것 끼리 묶는다.
    # spot, margin -> Binance
    # futures -> BinanceFutures
    ID = BINANCE_MARGIN

    def __init__(self):
        self.key_id = os.getenv('BINANCE_API_KEY')
        self.key_secret = os.getenv('BINANCE_SECRET_KEY')

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
        endpoint = '/sapi/v1/userDataStream'
        return self._post(endpoint)

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
        endpoint = '/sapi/v1/userDataStream'
        data = {
            'listenKey': listen_key
        }
        return self._put(endpoint, data)
