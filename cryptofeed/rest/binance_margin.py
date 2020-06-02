import os
import logging

from cryptofeed.rest.binance import Binance
from cryptofeed.defines import BINANCE_MARGIN

LOG = logging.getLogger('rest')

class BinanceMargin(Binance):
    # NOTE(boseok): market에 따라 다음과 같이 class를 구분한다.
    # spot -> Binance
    # margin -> BinanceMargin(Binance)
    # futures -> BinanceFutures(Binance)

    ID = BINANCE_MARGIN

    def __init__(self):
        self.key_id = os.getenv('BINANCE_API_KEY')
        self.key_secret = os.getenv('BINANCE_SECRET_KEY')

    def create_listen_key(self):
        """
        Create listenKey for user data stream

        margin: POST /sapi/v1/userDataStream
        https://binance-docs.github.io/apidocs/spot/en/#listen-key-margin

        Returns:
        {
           "listenKey": "pqia91ma19a5s61cv6a81va65sdf19v8a65a1a5s61cv6a81va65sdf19v8a65a1"
        }
        """
        endpoint = '/sapi/v1/userDataStream'
        return self._post(endpoint)

    def keepalive_listen_key(self, listen_key: str):
        """
        Keepalive a user data stream to prevent a time out

        margin: PUT /sapi/v1/userDataStream
        https://binance-docs.github.io/apidocs/spot/en/#listen-key-margin

        Args:
            listen_key(str)
        """
        endpoint = '/sapi/v1/userDataStream'
        data = {
            'listenKey': listen_key
        }
        return self._put(endpoint, data)
