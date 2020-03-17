import os
from time import sleep
import time
import hashlib
import hmac
from urllib.parse import urlparse
import logging
import zlib
from datetime import datetime as dt
from datetime import timedelta

from dotenv import load_dotenv
import requests

from cryptofeed.rest.api import API, request_retry
from cryptofeed.defines import BINANCE, SELL, BUY, BID, ASK
from cryptofeed.standards import timestamp_normalize

load_dotenv()
LOG = logging.getLogger('rest')


class Binance(API):
    # spot, margin, futures class를 api가 같은 것 끼리 묶는다.
    # spot, margin -> Binance
    # futures -> BinanceFutures
    ID = BINANCE
    api = 'https://api.binance.com'

    def __init__(self):
        self.key_id = os.getenv('BINANCE_API_KEY')
        self.key_secret = os.getenv('BINANCE_SECRET_KEY')

    def generate_signature(self, data='') -> dict:
        """
        verb: GET/POST/PUT
        url: api endpoint
        data: body (if present)
        """
        signature = hmac.new(bytes(self.key_secret, 'utf8'), bytes(data, 'utf8'), digestmod=hashlib.sha256).hexdigest()
        return signature

    def _post(self, endpoint: str, data=None):
        headers = {
            'Accept': 'application/json',
            'User-Agent': 'binance/python',
            'X-MBX-APIKEY': self.key_id
        }
        response = requests.post(f'{self.api}{endpoint}', headers=headers, data=data)
        print('response', response)
        return response.json()

    def _put(self, endpoint: str, data=None):
        headers = {
            'Accept': 'application/json',
            'User-Agent': 'binance/python',
            'X-MBX-APIKEY': self.key_id
        }
        response = requests.put(f'{self.api}{endpoint}', headers=headers, data=data)
        return response.json()

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
        endpoint = '/api/v3/userDataStream'
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
        endpoint = '/api/v3/userDataStream'
        data = {
            'listenKey': listen_key
        }
        return self._put(endpoint, data)
