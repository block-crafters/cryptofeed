from time import sleep
import time
import hashlib
import hmac
from urllib.parse import urlparse
import logging
import zlib
from datetime import datetime as dt
from datetime import timedelta

import requests

from cryptofeed.rest.api import API, request_retry
from cryptofeed.defines import BINANCE, SELL, BUY, BID, ASK
from cryptofeed.standards import timestamp_normalize

LOG = logging.getLogger('rest')


class Binance(API):
    # spot, margin, futures class를 api가 같은 것 끼리 묶는다.
    # spot, margin -> Binance
    # futures -> BinanceFutures
    ID = BINANCE
    api = 'https://api.binance.com'

    @staticmethod
    def generate_signature(verb: str, url: str, data='', key_id=None, key_secret=None) -> dict:
        """
        verb: GET/POST/PUT
        url: api endpoint
        data: body (if present)
        """
        pass

    def _post(self, command: str, payload=None):
        pass

    def _put(self, command: str, payload=None):
        pass

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
        endpoint = ''
        return requests.post(f'{self.api}{endpoint}')

    def keepalive_listen_key(self):
        """
        spot: PUT /api/v3/userDataStream
        margin: PUT /sapi/v1/userDataStream
        https://binance-docs.github.io/apidocs/spot/en/#listen-key-spot
        https://binance-docs.github.io/apidocs/spot/en/#listen-key-margin

        {
           "listenKey": "pqia91ma19a5s61cv6a81va65sdf19v8a65a1a5s61cv6a81va65sdf19v8a65a1"
        }
        """
        pass
