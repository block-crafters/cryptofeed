'''
Copyright (C) 2017-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import base64
import hmac
import logging
import datetime

from cryptofeed.rest.api import API
from cryptofeed.defines import OKEX


RATE_LIMIT_SLEEP = 2
API_MAX = 500
API_REFRESH = 300

LOG = logging.getLogger('rest')


class OKEx(API):
    ID = OKEX
    api = 'https://www.okex.com/api/'

    @staticmethod
    def generate_signature(verb: str, url: str, data='', key_id=None, key_secret=None, passphrase=None) -> dict:
        """
        verb: GET/POST/PUT
        url: api endpoint
        data: body (if present)
        """
        now = datetime.datetime.now()
        timestamp = str(now.timestamp())
        sign = OKEx.sign((timestamp + str.upper(verb) + url + str(data)), key_secret)

        return {
            'api_key': key_id,
            'passphrase': passphrase,
            'timestamp': timestamp,
            'sign': sign
        }

    @staticmethod
    def sign(message, key_secret):
        mac = hmac.new(bytes(key_secret, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
        d = mac.digest()
        return base64.b64encode(d).decode('utf-8')
