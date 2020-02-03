'''
Copyright (C) 2017-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from time import sleep
import time
import hashlib
import hmac
from urllib.parse import urlparse
import logging
import zlib
from datetime import datetime as dt
from datetime import timedelta

from sortedcontainers import SortedDict as sd
import requests
import pandas as pd

from cryptofeed.rest.api import API, request_retry
from cryptofeed.defines import BYBIT, SELL, BUY, BID, ASK
from cryptofeed.standards import timestamp_normalize


RATE_LIMIT_SLEEP = 2
API_MAX = 500
API_REFRESH = 300

LOG = logging.getLogger('rest')


class Bybit(API):
    ID = BYBIT
    api = 'https://www.bybit.com'

    @staticmethod
    def generate_signature(verb: str, url: str, data='', key_id=None, key_secret=None) -> dict:
        """
        verb: GET/POST/PUT
        url: api endpoint
        data: body (if present)
        """
        expires = int(round(time.time()) * 1000 + 1000)

        parsedURL = urlparse(url)
        path = parsedURL.path
        if parsedURL.query:
            path = path + '?' + parsedURL.query

        if isinstance(data, (bytes, bytearray)):
            data = data.decode('utf8')

        message = verb + path + str(expires) + data
        signature = hmac.new(bytes(key_secret, 'utf8'), bytes(message, 'utf8'), digestmod=hashlib.sha256).hexdigest()
        return {
            "api-expires": str(expires),
            "api-key": key_id,
            "api-signature": signature
        }
