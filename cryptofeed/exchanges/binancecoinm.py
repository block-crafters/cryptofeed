'''
Copyright (C) 2017-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from decimal import Decimal
import logging
from typing import Any, Tuple

from yapic import json

from cryptofeed.defines import BINANCECOINM, FUNDING, LIQUIDATIONS, OPEN_INTEREST, POSITIONS, BUY, SELL, BALANCES
from cryptofeed.exchanges.binance import Binance
from cryptofeed.exchanges.mixins.binance_rest import BinancecoinmRestMixin


LOG = logging.getLogger('feedhandler')


class Binancecoinm(Binance, BinancecoinmRestMixin):
    """
    TODO(Boseok): If data receiving through "ORDER_TRADE_UPDATE" or "ACCOUNT_UPDATE",
          implement Websocket User Data Request.
    """
    id = BINANCECOINM
    symbol_endpoint = 'https://dapi.binance.com/dapi/v1/exchangeInfo'
    valid_depths = [5, 10, 20, 50, 100, 500, 1000]
    valid_depth_intervals = {'100ms', '250ms', '500ms'}
    websocket_channels = {
        **Binance.websocket_channels,
        FUNDING: 'markPrice',
        OPEN_INTEREST: 'open_interest',
        LIQUIDATIONS: 'forceOrder',
        POSITIONS: POSITIONS,
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # overwrite values previously set by the super class Binance
        self.ws_endpoint = 'wss://dstream.binance.com'
        self.rest_endpoint = 'https://dapi.binance.com/dapi/v1'

    def _get_listen_key_url(self) -> str:
        """
        Return listenKey REST api call url
        """
        return self.rest_endpoint + '/listenKey'

    def _check_update_id(self, pair: str, msg: dict) -> Tuple[bool, bool, bool]:
        skip_update = False
        forced = not self.forced[pair]
        current_match = self.last_update_id[pair] == msg['u']

        if forced and msg['u'] < self.last_update_id[pair]:
            skip_update = True
        elif forced and msg['U'] <= self.last_update_id[pair] <= msg['u']:
            self.last_update_id[pair] = msg['u']
            self.forced[pair] = True
        elif not forced and self.last_update_id[pair] == msg['pu']:
            self.last_update_id[pair] = msg['u']
        else:
            self._reset()
            LOG.warning("%s: Missing book update detected, resetting book", self.id)
            skip_update = True
        return skip_update, forced, current_match

    def _parse_balance(self, data: dict) -> dict:
        """
        Parse balance raw data to a format

        Reference:
            balance raw data
            {
                "a":"BTC",                // Asset
                "wb":"122624.12345678",   // Wallet Balance
                "cw":"100.12345678"       // Cross Wallet Balance
            }
        """
        parsed_balance: dict = {}
        if data.get('a'):
            parsed_balance['currency'] = data['a']
        if data.get('wb') is not None:
            parsed_balance['wallet_balance'] = Decimal(data['wb'])
        return parsed_balance
    
    def _parse_position(self, data: dict) -> dict:
        """
        Parse position raw data to a format

        Reference:
            position raw data
            {
                "s":"BTCUSD_200925",      // Symbol
                "pa":"0",                 // Position Amount
                "ep":"0.0",               // Entry Price
                "cr":"200",               // (Pre-fee) Accumulated Realized
                "up":"0",                 // Unrealized PnL
                "mt":"isolated",          // Margin Type
                "iw":"0.00000000",        // Isolated Wallet (if isolated position)
                "ps":"BOTH"               // Position Side
            }
        """
        parsed_position: dict = {}
        if data.get('s'):
            parsed_position['symbol'] = self.exchange_symbol_to_std_symbol(data['s'])
        if data.get('ep') is not None:
            parsed_position['entry_price'] = Decimal(data['ep'])
        if data.get('up') is not None:
            parsed_position['unrealized_pnl'] = Decimal(data['up'])
        if data.get('pa') is not None:
            # NOTE: pa(position amount) is signed value.
            parsed_position['contracts'] = Decimal(data['pa'])
        # NOTE(Boseok): liquidation price is not included in websocket data.
        if data.get('mt'):
            parsed_position['margin_mode'] = 'cross' if data['mt'] == 'CROSSED' else 'isolated'
        return parsed_position

    async def _account_update(self, msg: dict, timestamp: float):
        """
        {
            "e": "ACCOUNT_UPDATE",            // Event Type
            "E": 1564745798939,               // Event Time
            "T": 1564745798938 ,              // Transaction
            "i": "SfsR",                      // Account Alias
            "a":{                             // Update Data
                "m":"ORDER",                  // Event reason type
                "B":[                         // Balances
                    {
                        "a":"BTC",                // Asset
                        "wb":"122624.12345678",   // Wallet Balance
                        "cw":"100.12345678"       // Cross Wallet Balance
                    },
                    {
                        "a":"ETH",
                        "wb":"1.00000000",
                        "cw":"0.00000000"
                    }
                ],
                "P":[
                    {
                        "s":"BTCUSD_200925",      // Symbol
                        "pa":"0",                 // Position Amount
                        "ep":"0.0",               // Entry Price
                        "cr":"200",               // (Pre-fee) Accumulated Realized
                        "up":"0",                 // Unrealized PnL
                        "mt":"isolated",          // Margin Type
                        "iw":"0.00000000",        // Isolated Wallet (if isolated position)
                        "ps":"BOTH"               // Position Side
                    },
                    {
                        "s":"BTCUSD_200925",
                        "pa":"20",
                        "ep":"6563.6",
                        "cr":"0",
                        "up":"2850.21200000",
                        "mt":"isolated",
                        "iw":"13200.70726908",
                        "ps":"LONG"
                    },
                    {
                        "s":"BTCUSD_200925",
                        "pa":"-10",
                        "ep":"6563.8",
                        "cr":"-45.04000000",
                        "up":"-1423.15600000",
                        "mt":"isolated",
                        "iw":"6570.42511771",
                        "ps":"SHORT"
                    }
                ]
            }
        }
        """
        timestamp: float = self.timestamp_normalize(msg['E'])
        account_id: str = self.custom_account_id or msg['i']
        account_data: dict = msg['a']
        balances_data: list[dict] = account_data['B']
        positions_data: list[dict] = account_data['P']

        for balance in balances_data:
            data = {
                'account': account_id,
                'timestamp': timestamp,
            }
            parsed_balance = self._parse_balance(balance)
            data.update(parsed_balance)
            await self.callback(BALANCES, feed=self.id, **data)
        for position in positions_data:
            data = {
                'account': account_id,
                'timestamp': timestamp,
            }
            parsed_position = self._parse_position(position)
            data.update(parsed_position)
            await self.callback(POSITIONS, feed=self.id, **data)

    def parse_order(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        {
            "e":"ORDER_TRADE_UPDATE",     // Event Type
            "E":1591274595442,            // Event Time
            "T":1591274595453,            // Transaction Time
            "i":"SfsR",                   // Account Alias
            "o":
            {
                "s":"BTCUSD_200925",        // Symbol
                "c":"TEST",                 // Client Order Id
                // special client order id:
                // starts with "autoclose-": liquidation order
                // "adl_autoclose": ADL auto close order
                "S":"SELL",                 // Side
                "o":"TRAILING_STOP_MARKET", // Order Type
                "f":"GTC",                  // Time in Force
                "q":"2",                    // Original Quantity
                "p":"0",                    // Original Price
                "ap":"0",                   // Average Price
                "sp":"9103.1",              // Stop Price. Please ignore with TRAILING_STOP_MARKET order
                "x":"NEW",                  // Execution Type
                "X":"NEW",                  // Order Status
                "i":8888888,                // Order Id
                "l":"0",                    // Order Last Filled Quantity
                "z":"0",                    // Order Filled Accumulated Quantity
                "L":"0",                    // Last Filled Price
                "ma": "BTC",                // Margin Asset
                "N":"BTC",                  // Commission Asset of the trade, will not push if no commission
                "n":"0",                    // Commission of the trade, will not push if no commission
                "T":1591274595442,          // Order Trade Time
                "t":0,                      // Trade Id
                "rp": "0",                  // Realized Profit of the trade
                "b":"0",                    // Bid quantity of base asset
                "a":"0",                    // Ask quantity of base asset
                "m":false,                  // Is this trade the maker side?
                "R":false,                  // Is this reduce only
                "wt":"CONTRACT_PRICE",      // Stop Price Working Type
                "ot":"TRAILING_STOP_MARKET",// Original Order Type
                "ps":"LONG",                // Position Side
                "cp":false,                 // If Close-All, pushed with conditional order
                "AP":"9476.8",              // Activation Price, only puhed with TRAILING_STOP_MARKET order
                "cr":"5.0",                 // Callback Rate, only puhed with TRAILING_STOP_MARKET order
                "pP": false                 // If conditional order trigger is protected
            }
        }
        """
        timestamp = self.timestamp_normalize(data['E'])
        order_data: dict[str, Any] = data['o']
        order = {
            'order_id': str(order_data['i']),
            'timestamp': timestamp
        }
        if order_data.get('s') is not None:
            order['symbol'] = self.exchange_symbol_to_std_symbol(order_data['s'])
        if order_data.get('S'):
            order['side'] = BUY if order_data['S'] == 'BUY' else SELL
        if order_data.get('X'):
            order['status'] = self.parse_order_status(order_data['X'])

        order['account'] = self.custom_account_id or data.get('i')
        if order_data.get('q') is not None:
            order['amount'] = Decimal(order_data['q'])
        if order_data.get('z') is not None:
            order['filled'] = Decimal(order_data['z'])
        if order.get('amount') is not None and order.get("filled") is not None:
            order['remaining'] = max(order.get('amount') - order.get("filled"), 0)
        if order_data.get('p') is not None:
            order['price'] = Decimal(order_data['p'])
        if order_data.get('ap') is not None:
            order['average'] = Decimal(order_data.get('ap'))
        if order_data.get('c') is not None:
            order['clientOrderId'] = order_data.get('c')
        if order_data.get('o') is not None:
            order['type'] = order_data.get('o').lower() if order_data.get('o') else None
        if order_data.get('f') is not None:
            timeInForce = order_data.get('f')
            if timeInForce == 'GTX':
            # GTX means "Good Till Crossing" and is an equivalent way of saying Post Only
                timeInForce = 'PO'
            order['timeInForce'] = timeInForce

        return order

    async def message_handler(self, msg: str, conn, timestamp: float):
        msg = json.loads(msg, parse_float=Decimal)

        # Handle account updates from User Data Stream
        if self.requires_authentication:
            msg_type = msg.get('e')
            if msg_type == 'ACCOUNT_UPDATE':
                await self._account_update(msg, timestamp)
            elif msg_type == 'ORDER_TRADE_UPDATE':
                await self._order_update(msg, timestamp)
            return
        # Combined stream events are wrapped as follows: {"stream":"<streamName>","data":<rawPayload>}
        # streamName is of format <symbol>@<channel>
        pair, _ = msg['stream'].split('@', 1)
        msg = msg['data']

        pair = pair.upper()

        msg_type = msg.get('e')
        if msg_type == 'bookTicker':
            await self._ticker(msg, timestamp)
        elif msg_type == 'depthUpdate':
            await self._book(msg, pair, timestamp)
        elif msg_type == 'aggTrade':
            await self._trade(msg, timestamp)
        elif msg_type == 'forceOrder':
            await self._liquidations(msg, timestamp)
        elif msg_type == 'markPriceUpdate':
            await self._funding(msg, timestamp)
        elif msg_type == 'kline':
            await self._candle(msg, timestamp)
        else:
            LOG.warning("%s: Unexpected message received: %s", self.id, msg)
