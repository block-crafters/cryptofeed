import os
import hashlib
import hmac
import logging
import time

from dotenv import load_dotenv
import requests

from cryptofeed.rest.api import API
from cryptofeed.defines import BINANCE

load_dotenv()
LOG = logging.getLogger('rest')

class Binance(API):
    # NOTE(boseok): market에 따라 다음과 같이 class를 구분한다.
    # spot -> Binance
    # margin -> BinanceMargin(Binance)
    # futures -> BinanceFutures(Binance)

    ID = BINANCE
    api = 'https://api.binance.com'

    def __init__(self):
        self.key_id = os.getenv('BINANCE_API_KEY')
        self.key_secret = os.getenv('BINANCE_SECRET_KEY')

    def generate_signature(self, params=None) -> dict:
        """
        Generate signature for authentication

        params: body (if present)
        """
        if params:
            params_str = self.stringify_params(params)
        
        signature = hmac.new(bytes(self.key_secret, 'utf8'), bytes(params_str, 'utf8'), digestmod=hashlib.sha256).hexdigest()
        return signature

    @staticmethod
    def stringify_params(params: dict):
        """
        Stringify params to concatenated string

        params: body (if present)
        """
        has_signature = False
        if 'signature' in params:
            has_signature = True
            signature = params['signature']

        params_list = list(params.items())
        params_except_signature_list = [param_tuple for param_tuple in params_list if param_tuple[0] != 'signature']
        params_str = '&'.join([f'{param_tuple[0]}={Binance.bool_to_str(param_tuple[1])}' for param_tuple in params_except_signature_list])

        if has_signature:
            if params_except_signature_list:
                params_str += f'&signature={signature}'
            else:
                params_str = f'signature={signature}'

        return params_str

    @staticmethod
    def bool_to_str(value: bool):
        """
        Convert bool to string
        """
        if type(value) == 'bool':
            bool_str = 'true' if value else 'false'
            return bool_str
        else:
            return value

    def _post(self, endpoint: str, params=None, sign=False):
        if params is None:
            params = {}

        if sign:
            signature = self.generate_signature(params)
            params['signature'] = signature

        query = self.stringify_params(params)
        url = f'{self.api}{endpoint}?{query}'

        headers = {
            'X-MBX-APIKEY': self.key_id
        }
        response = requests.post(url, headers=headers, params=params)
        return response.json()

    def _put(self, endpoint: str, params=None, sign=False):
        if params is None:
            params = {}

        if sign:
            signature = self.generate_signature(params)
            params['signature'] = signature

        query = self.stringify_params(params)
        url = f'{self.api}{endpoint}?{query}'

        headers = {
            'X-MBX-APIKEY': self.key_id
        }
        response = requests.put(url, headers=headers, params=params)
        return response.json()

    def create_listen_key(self):
        """
        Create listenKey for user data stream

        spot: POST /api/v3/userDataStream
        https://binance-docs.github.io/apidocs/spot/en/#listen-key-spot

        Returns:
        {
           "listenKey": "pqia91ma19a5s61cv6a81va65sdf19v8a65a1a5s61cv6a81va65sdf19v8a65a1"
        }
        """
        endpoint = '/api/v3/userDataStream'
        return self._post(endpoint)

    def keepalive_listen_key(self, listen_key: str):
        """
        Keepalive a user data stream to prevent a time out

        spot: PUT /api/v3/userDataStream
        https://binance-docs.github.io/apidocs/spot/en/#listen-key-spot

        Args:
            listen_key(str)
        """
        endpoint = '/api/v3/userDataStream'
        params = {
            'listenKey': listen_key
        }
        return self._put(endpoint, params)
