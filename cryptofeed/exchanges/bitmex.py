'''
Copyright (C) 2017-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from typing import Dict, Tuple
import hashlib
import hmac
import logging
import time
from collections import defaultdict
from datetime import datetime as dt
from decimal import Decimal

from sortedcontainers import SortedDict as sd
from yapic import json

from cryptofeed.defines import BID, ASK, BITMEX, BUY, FUNDING, FUTURES, L2_BOOK, LIQUIDATIONS, OPEN_INTEREST, PERPETUAL, SELL, TICKER, TRADES, UNFILLED, ORDER_INFO, POSITIONS, BALANCES
from cryptofeed.feed import Feed
from cryptofeed.symbols import Symbol
from cryptofeed.connection import AsyncConnection
from cryptofeed.exchanges.mixins.bitmex_rest import BitmexRestMixin


LOG = logging.getLogger('feedhandler')


class Bitmex(Feed, BitmexRestMixin):
    id = BITMEX
    symbol_endpoint = "https://www.bitmex.com/api/v1/instrument/active"
    websocket_channels = {
        L2_BOOK: 'orderBookL2',
        TRADES: 'trade',
        TICKER: 'quote',
        FUNDING: 'funding',
        OPEN_INTEREST: 'instrument',
        LIQUIDATIONS: 'liquidation',
        ORDER_INFO: 'order',
        POSITIONS: 'position',
        BALANCES: 'margin'
    }
    channels_without_symbol = [
        'margin'
    ]
    request_limit = 0.5

    @classmethod
    def _parse_symbol_data(cls, data: dict) -> Tuple[Dict, Dict]:
        ret = {}
        info = defaultdict(dict)

        for entry in data:
            base = entry['rootSymbol'].replace("XBT", "BTC")
            quote = entry['quoteCurrency'].replace("XBT", "BTC")

            stype = PERPETUAL
            if entry['expiry']:
                stype = FUTURES

            s = Symbol(base, quote, type=stype, expiry_date=entry['expiry'])
            ret[s.normalized] = entry['symbol']
            info['tick_size'][s.normalized] = entry['tickSize']
            info['instrument_type'][s.normalized] = stype

        return ret, info

    def parse_order(self, data):
        timestamp = self.timestamp_normalize(data['timestamp'])
        order = {
            'order_id': data['orderID'],
            'timestamp': timestamp
        }
        if data.get('side'):
            order['side'] = BUY if data['side'] == 'Buy' else SELL
        if data.get('ordStatus'):
            order['status'] = Bitmex.parse_order_status(data['ordStatus'])

        # 0 should be True at `if` statement.
        if data.get('account') is not None:
            order['account'] = Decimal(data['account'])
        if data.get('orderQty') is not None:
            order['amount'] = Decimal(data['orderQty'])
        if data.get('cumQty') is not None:
            order['filled'] = Decimal(data['cumQty'])
        if data.get('leavesQty') is not None:
            order['remaining'] = Decimal(data['leavesQty'])
        if data.get('price') is not None:
            order['price'] = Decimal(data['price'])
        if data.get('avgPx') is not None:
            order['avgPrice'] = Decimal(data.get('avgPx'))
        if data.get('clOrdID') is not None:
            order['clientOrderId'] = data.get('clOrdID')
        if data.get('stopPx') is not None:
            order['stopPrice'] = data.get('stopPx')
        if data.get('execInst') is not None:
            order['postOnly'] = (data.get('execInst')=='ParticipateDoNotInitiate')
        if data.get('ordType') is not None:
            order['orderType'] = data.get('ordType')
        if data.get('cost') is not None:
            order['cost'] = data.get('cost')
        if data.get('fee') is not None:
            order['fee'] = data.get('fee')
        if data.get('trades') is not None:
            order['trades'] = data.get('trades')
        if data.get('timeInForce') is not None:
            order['timeInForce'] = Bitmex.parse_time_in_force(data.get('timeInForce'))
        if data.get('transactTime') is not None:
            order['lastTradeTimestamp'] = self.timestamp_normalize(data.get('transactTime'))
        return order

    def parse_position(self, data):
        timestamp = self.timestamp_normalize(data['timestamp'])
        position = {
            'timestamp': timestamp
        }
        if data.get('account') is not None:
            position['account'] = data['account']
        if data.get('symbol') is not None:
            position['symbol'] = self.exchange_symbol_to_std_symbol(data['symbol'])
        if data.get('initMargin') is not None:
            position['initialMargin'] = Decimal(data.get('initMargin'))
        if data.get('maintMargin') is not None:
            position['maintenanceMargin'] = Decimal(data['maintMargin'])
        if data.get('avgEntryPrice') is not None:
            position['entryPrice'] = Decimal(data.get('avgEntryPrice'))
        if data.get('homeNotional') is not None:
            position['homeNotional'] = Decimal(data.get('homeNotional'))
        if data.get('foreignNotional') is not None:
            position['foreignNotional'] = Decimal(data.get('foreignNotional'))
        if data.get('leverage') is not None:
            position['leverage'] = Decimal(data.get('leverage'))
        if data.get('unrealisedPnl') is not None:
            position['unrealizedPnl'] = Decimal(data['unrealisedPnl'])
        if data.get('currentQty') is not None:
            position['contracts'] = Decimal(data.get('currentQty'))
        if data.get('liquidationPrice') is not None:
            position['liquidationPrice'] = Decimal(data['liquidationPrice'])
        if data.get('markPrice') is not None:
            position['markPrice'] = Decimal(data['markPrice'])
        return position

    def parse_margin(self, data):
        timestamp = self.timestamp_normalize(data['timestamp'])
        balance = {
            'timestamp': timestamp
        }
        if data.get('account') is not None:
            balance['account'] = Decimal(data['account'])
        if data.get('availableMargin') is not None:
            balance['free'] = Decimal(data['availableMargin'])
        if data.get('marginBalance') is not None:
            balance['total'] = Decimal(data['marginBalance'])
        if data.get('currency') is not None:
            balance['currency'] = self.parse_currency(data['currency'])
        return balance

    @staticmethod
    def parse_order_status(status):
        statuses = {
            'PendingNew': 'open',
            'New': 'open',
            'PartiallyFilled': 'open',
            'Filled': 'closed',
            'Canceled': 'canceled',
            'Rejected': 'rejected',
        }

        ret = statuses.get(status, None)
        return ret

    @staticmethod
    def parse_currency(currency):
        currencies = {
            'XBt': 'BTC'
        }
        ret = currencies.get(currency, None)
        return ret

    @staticmethod
    def parse_time_in_force(timeInForce):
        times = {
            'Day': 'Day',
            'GoodTillCancel': 'GTC',
            'ImmediateOrCancel': 'IOC',
            'FillOrKill': 'FOK'
        }
        ret = times.get(timeInForce, None)
        return ret

    def __init__(self, sandbox=False, **kwargs):
        auth_api = 'wss://www.bitmex.com/realtime' if not sandbox else 'wss://testnet.bitmex.com/realtime'
        super().__init__(auth_api, **kwargs)
        self._reset()

    def _reset(self):
        self.partial_received = defaultdict(bool)
        self.order_id = {}
        for pair in self.normalized_symbols:
            self._l2_book[pair] = {BID: sd(), ASK: sd()}
            self.order_id[pair] = defaultdict(dict)

    async def _trade(self, msg: dict, timestamp: float):
        """
        trade msg example

        {
            'timestamp': '2018-05-19T12:25:26.632Z',
            'symbol': 'XBTUSD',
            'side': 'Buy',
            'size': 40,
            'price': 8335,
            'tickDirection': 'PlusTick',
            'trdMatchID': '5f4ecd49-f87f-41c0-06e3-4a9405b9cdde',
            'grossValue': 479920,
            'homeNotional': Decimal('0.0047992'),
            'foreignNotional': 40
        }
        """
        for data in msg['data']:
            ts = self.timestamp_normalize(data['timestamp'])
            await self.callback(TRADES, feed=self.id,
                                symbol=self.exchange_symbol_to_std_symbol(data['symbol']),
                                side=BUY if data['side'] == 'Buy' else SELL,
                                amount=Decimal(data['size']),
                                price=Decimal(data['price']),
                                order_id=data['trdMatchID'],
                                timestamp=ts,
                                receipt_timestamp=timestamp)

    async def _book(self, msg: dict, timestamp: float):
        """
        the Full bitmex book
        """
        # PERF perf_start(self.id, 'book_msg')

        delta = {BID: [], ASK: []}
        # if we reset the book, force a full update
        forced = False
        pair = self.exchange_symbol_to_std_symbol(msg['data'][0]['symbol'])
        if not self.partial_received[pair]:
            # per bitmex documentation messages received before partial
            # should be discarded
            if msg['action'] != 'partial':
                return
            self.partial_received[pair] = True
            forced = True

        if msg['action'] == 'partial':
            for data in msg['data']:
                side = BID if data['side'] == 'Buy' else ASK
                price = Decimal(data['price'])
                size = Decimal(data['size'])
                order_id = data['id']

                self._l2_book[pair][side][price] = size
                self.order_id[pair][side][order_id] = price
        elif msg['action'] == 'insert':
            for data in msg['data']:
                side = BID if data['side'] == 'Buy' else ASK
                price = Decimal(data['price'])
                size = Decimal(data['size'])
                order_id = data['id']

                self._l2_book[pair][side][price] = size
                self.order_id[pair][side][order_id] = price
                delta[side].append((price, size))
        elif msg['action'] == 'update':
            for data in msg['data']:
                side = BID if data['side'] == 'Buy' else ASK
                update_size = Decimal(data['size'])
                order_id = data['id']

                price = self.order_id[pair][side][order_id]

                self._l2_book[pair][side][price] = update_size
                self.order_id[pair][side][order_id] = price
                delta[side].append((price, update_size))
        elif msg['action'] == 'delete':
            for data in msg['data']:
                side = BID if data['side'] == 'Buy' else ASK
                order_id = data['id']

                delete_price = self.order_id[pair][side][order_id]
                del self.order_id[pair][side][order_id]
                del self._l2_book[pair][side][delete_price]
                delta[side].append((delete_price, 0))

        else:
            LOG.warning("%s: Unexpected l2 Book message %s", self.id, msg)
            return
        # PERF perf_end(self.id, 'book_msg')
        # PERF perf_log(self.id, 'book_msg')

        await self.book_callback(self._l2_book[pair], L2_BOOK, pair, forced, delta, timestamp, timestamp)

    async def _ticker(self, msg: dict, timestamp: float):
        for data in msg['data']:
            await self.callback(TICKER, feed=self.id,
                                symbol=self.exchange_symbol_to_std_symbol(data['symbol']),
                                bid=Decimal(data['bidPrice']),
                                ask=Decimal(data['askPrice']),
                                timestamp=self.timestamp_normalize(data['timestamp']),
                                receipt_timestamp=timestamp)

    async def _funding(self, msg: dict, timestamp: float):
        """
        {'table': 'funding',
         'action': 'partial',
         'keys': ['timestamp', 'symbol'],
         'types': {
             'timestamp': 'timestamp',
             'symbol': 'symbol',
             'fundingInterval': 'timespan',
             'fundingRate': 'float',
             'fundingRateDaily': 'float'
            },
         'foreignKeys': {
             'symbol': 'instrument'
            },
         'attributes': {
             'timestamp': 'sorted',
             'symbol': 'grouped'
            },
         'filter': {'symbol': 'XBTUSD'},
         'data': [{
             'timestamp': '2018-08-21T20:00:00.000Z',
             'symbol': 'XBTUSD',
             'fundingInterval': '2000-01-01T08:00:00.000Z',
             'fundingRate': Decimal('-0.000561'),
             'fundingRateDaily': Decimal('-0.001683')
            }]
        }
        """
        for data in msg['data']:
            ts = self.timestamp_normalize(data['timestamp'])
            interval = data['fundingInterval']
            interval = int((interval - dt(interval.year, interval.month, interval.day, tzinfo=interval.tzinfo)).total_seconds())
            await self.callback(FUNDING, feed=self.id,
                                symbol=self.exchange_symbol_to_std_symbol(data['symbol']),
                                timestamp=ts,
                                receipt_timestamp=timestamp,
                                interval=interval,
                                rate=data['fundingRate'],
                                rate_daily=data['fundingRateDaily']
                                )

    async def _instrument(self, msg: dict, timestamp: float):
        """
        Example instrument data

        {
        'table':'instrument',
        'action':'partial',
        'keys':[
            'symbol'
        ],
        'types':{
            'symbol':'symbol',
            'rootSymbol':'symbol',
            'state':'symbol',
            'typ':'symbol',
            'listing':'timestamp',
            'front':'timestamp',
            'expiry':'timestamp',
            'settle':'timestamp',
            'relistInterval':'timespan',
            'inverseLeg':'symbol',
            'sellLeg':'symbol',
            'buyLeg':'symbol',
            'optionStrikePcnt':'float',
            'optionStrikeRound':'float',
            'optionStrikePrice':'float',
            'optionMultiplier':'float',
            'positionCurrency':'symbol',
            'underlying':'symbol',
            'quoteCurrency':'symbol',
            'underlyingSymbol':'symbol',
            'reference':'symbol',
            'referenceSymbol':'symbol',
            'calcInterval':'timespan',
            'publishInterval':'timespan',
            'publishTime':'timespan',
            'maxOrderQty':'long',
            'maxPrice':'float',
            'lotSize':'long',
            'tickSize':'float',
            'multiplier':'long',
            'settlCurrency':'symbol',
            'underlyingToPositionMultiplier':'long',
            'underlyingToSettleMultiplier':'long',
            'quoteToSettleMultiplier':'long',
            'isQuanto':'boolean',
            'isInverse':'boolean',
            'initMargin':'float',
            'maintMargin':'float',
            'riskLimit':'long',
            'riskStep':'long',
            'limit':'float',
            'capped':'boolean',
            'taxed':'boolean',
            'deleverage':'boolean',
            'makerFee':'float',
            'takerFee':'float',
            'settlementFee':'float',
            'insuranceFee':'float',
            'fundingBaseSymbol':'symbol',
            'fundingQuoteSymbol':'symbol',
            'fundingPremiumSymbol':'symbol',
            'fundingTimestamp':'timestamp',
            'fundingInterval':'timespan',
            'fundingRate':'float',
            'indicativeFundingRate':'float',
            'rebalanceTimestamp':'timestamp',
            'rebalanceInterval':'timespan',
            'openingTimestamp':'timestamp',
            'closingTimestamp':'timestamp',
            'sessionInterval':'timespan',
            'prevClosePrice':'float',
            'limitDownPrice':'float',
            'limitUpPrice':'float',
            'bankruptLimitDownPrice':'float',
            'bankruptLimitUpPrice':'float',
            'prevTotalVolume':'long',
            'totalVolume':'long',
            'volume':'long',
            'volume24h':'long',
            'prevTotalTurnover':'long',
            'totalTurnover':'long',
            'turnover':'long',
            'turnover24h':'long',
            'homeNotional24h':'float',
            'foreignNotional24h':'float',
            'prevPrice24h':'float',
            'vwap':'float',
            'highPrice':'float',
            'lowPrice':'float',
            'lastPrice':'float',
            'lastPriceProtected':'float',
            'lastTickDirection':'symbol',
            'lastChangePcnt':'float',
            'bidPrice':'float',
            'midPrice':'float',
            'askPrice':'float',
            'impactBidPrice':'float',
            'impactMidPrice':'float',
            'impactAskPrice':'float',
            'hasLiquidity':'boolean',
            'openInterest':'long',
            'openValue':'long',
            'fairMethod':'symbol',
            'fairBasisRate':'float',
            'fairBasis':'float',
            'fairPrice':'float',
            'markMethod':'symbol',
            'markPrice':'float',
            'indicativeTaxRate':'float',
            'indicativeSettlePrice':'float',
            'optionUnderlyingPrice':'float',
            'settledPrice':'float',
            'timestamp':'timestamp'
        },
        'foreignKeys':{
            'inverseLeg':'instrument',
            'sellLeg':'instrument',
            'buyLeg':'instrument'
        },
        'attributes':{
            'symbol':'unique'
        },
        'filter':{
            'symbol':'XBTUSD'
        },
        'data':[
            {
                'symbol':'XBTUSD',
                'rootSymbol':'XBT',
                'state':'Open',
                'typ':'FFWCSX',
                'listing':'2016-05-13T12:00:00.000Z',
                'front':'2016-05-13T12:00:00.000Z',
                'expiry':None,
                'settle':None,
                'relistInterval':None,
                'inverseLeg':'',
                'sellLeg':'',
                'buyLeg':'',
                'optionStrikePcnt':None,
                'optionStrikeRound':None,
                'optionStrikePrice':None,
                'optionMultiplier':None,
                'positionCurrency':'USD',
                'underlying':'XBT',
                'quoteCurrency':'USD',
                'underlyingSymbol':'XBT=',
                'reference':'BMEX',
                'referenceSymbol':'.BXBT',
                'calcInterval':None,
                'publishInterval':None,
                'publishTime':None,
                'maxOrderQty':10000000,
                'maxPrice':1000000,
                'lotSize':1,
                'tickSize':Decimal(         '0.5'         ),
                'multiplier':-100000000,
                'settlCurrency':'XBt',
                'underlyingToPositionMultiplier':None,
                'underlyingToSettleMultiplier':-100000000,
                'quoteToSettleMultiplier':None,
                'isQuanto':False,
                'isInverse':True,
                'initMargin':Decimal(         '0.01'         ),
                'maintMargin':Decimal(         '0.005'         ),
                'riskLimit':20000000000,
                'riskStep':10000000000,
                'limit':None,
                'capped':False,
                'taxed':True,
                'deleverage':True,
                'makerFee':Decimal(         '-0.00025'         ),
                'takerFee':Decimal(         '0.00075'         ),
                'settlementFee':0,
                'insuranceFee':0,
                'fundingBaseSymbol':'.XBTBON8H',
                'fundingQuoteSymbol':'.USDBON8H',
                'fundingPremiumSymbol':'.XBTUSDPI8H',
                'fundingTimestamp':'2020-02-02T04:00:00.000Z',
                'fundingInterval':'2000-01-01T08:00:00.000Z',
                'fundingRate':Decimal(         '0.000106'         ),
                'indicativeFundingRate':Decimal(         '0.0001'         ),
                'rebalanceTimestamp':None,
                'rebalanceInterval':None,
                'openingTimestamp':'2020-02-02T00:00:00.000Z',
                'closingTimestamp':'2020-02-02T01:00:00.000Z',
                'sessionInterval':'2000-01-01T01:00:00.000Z',
                'prevClosePrice':Decimal(         '9340.63'         ),
                'limitDownPrice':None,
                'limitUpPrice':None,
                'bankruptLimitDownPrice':None,
                'bankruptLimitUpPrice':None,
                'prevTotalVolume':1999389257669,
                'totalVolume':1999420432348,
                'volume':31174679,
                'volume24h':1605909209,
                'prevTotalTurnover':27967114248663460,
                'totalTurnover':27967447182062520,
                'turnover':332933399058,
                'turnover24h':17126993087717,
                'homeNotional24h':Decimal(         '171269.9308771703'         ),
                'foreignNotional24h':1605909209,
                'prevPrice24h':9348,
                'vwap':Decimal(         '9377.3443'         ),
                'highPrice':9464,
                'lowPrice':Decimal(         '9287.5'         ),
                'lastPrice':9352,
                'lastPriceProtected':9352,
                'lastTickDirection':'ZeroMinusTick',
                'lastChangePcnt':Decimal(         '0.0004'         ),
                'bidPrice':9352,
                'midPrice':Decimal(         '9352.25'         ),
                'askPrice':Decimal(         '9352.5'         ),
                'impactBidPrice':Decimal(         '9351.9125'         ),
                'impactMidPrice':Decimal(         '9352.25'         ),
                'impactAskPrice':Decimal(         '9352.7871'         ),
                'hasLiquidity':True,
                'openInterest':983043322,
                'openValue':10518563545400,
                'fairMethod':'FundingRate',
                'fairBasisRate':Decimal(         '0.11607'         ),
                'fairBasis':Decimal(         '0.43'         ),
                'fairPrice':Decimal(         '9345.36'         ),
                'markMethod':'FairPrice',
                'markPrice':Decimal(         '9345.36'         ),
                'indicativeTaxRate':0,
                'indicativeSettlePrice':Decimal(         '9344.93'         ),
                'optionUnderlyingPrice':None,
                'settledPrice':None,
                'timestamp':'2020-02-02T00:30:43.772Z'
            }
        ]
        }
        """
        for data in msg['data']:
            if 'openInterest' in data:
                ts = self.timestamp_normalize(data['timestamp'])
                await self.callback(OPEN_INTEREST, feed=self.id,
                                    symbol=self.exchange_symbol_to_std_symbol(data['symbol']),
                                    open_interest=data['openInterest'],
                                    timestamp=ts,
                                    receipt_timestamp=timestamp)

    async def _liquidation(self, msg: dict, timestamp: float):
        """
        liquidation msg example

        {
            'orderID': '9513c849-ca0d-4e11-8190-9d221972288c',
            'symbol': 'XBTUSD',
            'side': 'Buy',
            'price': 6833.5,
            'leavesQty': 2020
        }
        """
        if msg['action'] == 'insert':
            for data in msg['data']:
                await self.callback(LIQUIDATIONS, feed=self.id,
                                    symbol=self.exchange_symbol_to_std_symbol(data['symbol']),
                                    side=BUY if data['side'] == 'Buy' else SELL,
                                    leaves_qty=Decimal(data['leavesQty']),
                                    price=Decimal(data['price']),
                                    order_id=data['orderID'],
                                    status=UNFILLED,
                                    timestamp=timestamp,
                                    receipt_timestamp=timestamp)
    async def _order(self, msg: dict, timestamp: float):
        """
        Example:
        {
        'feed': 'BITMEX', 
        'symbol': 'BTC-USD-PERP', 
        'order_id': '7e125225-2ae2-4dbf-9653-8d317d6420d8', 
        'timestamp': 1630308315.87, 
        'side': 'buy', 
        'status': 'open', 
        'amount': Decimal('100'), 
        'filled': Decimal('0'), 
        'remaining': Decimal('100'), 
        'price': Decimal('37947'), 
        'clientOrderId': '', 
        'postOnly': False, 
        'orderType': 'Limit', 
        'timeInForce': 'GTC', 
        'lastTradeTimestamp': 1630308315.87
        }
        """

        LOG.info("Handling order msg")
        for data in msg['data']:
            if data.get('ordStatus'):
                new_info = self.parse_order(data)
                await self.callback(ORDER_INFO, feed=self.id, **new_info)

    async def _position(self, msg: dict, timestamp: float):
        """
        {
        'account': 917702, 
        'symbol': 'ETHUSD', 
        'currency': 'XBt', 
        'underlying': 'ETH', 
        'quoteCurrency': 'USD', 
        'commission': Decimal('0.0005'), 
        'initMarginReq': Decimal('0.01'), 
        'maintMarginReq': Decimal('0.005'), 
        'riskLimit': 7500000000, 
        'leverage': 100, 
        'crossMargin': True, 
        'deleveragePercentile': 1, 
        'rebalancedPnl': -3185, 
        'prevRealisedPnl': 0, 
        'prevUnrealisedPnl': 0, 
        'prevClosePrice': Decimal('3937.92'), 
        'openingTimestamp': datetime.datetime(2021, 9, 6, 6, 0, tzinfo=datetime.timezone.utc), 
        'openingQty': 1, 
        'openingCost': 382490, 
        'openingComm': 1385, 
        'openOrderBuyQty': 0, 
        'openOrderBuyCost': 0, 
        'openOrderBuyPremium': 0, 
        'openOrderSellQty': 0, 
        'openOrderSellCost': 0, 
        'openOrderSellPremium': 0, 
        'execBuyQty': 0, 
        'execBuyCost': 0, 
        'execSellQty': 0, 
        'execSellCost': 0, 
        'execQty': 0, 
        'execCost': 0, 
        'execComm': 0, 
        'currentTimestamp': datetime.datetime(2021, 9, 6, 6, 34, 35, 385000, tzinfo=datetime.timezone.utc), 
        'currentQty': 1, 
        'currentCost': 382490, 
        'currentComm': 1385, 
        'realisedCost': 0, 
        'unrealisedCost': 382490, 
        'grossOpenCost': 0, 
        'grossOpenPremium': 0, 
        'grossExecCost': 0, 
        'isOpen': True, 'markPrice': Decimal('3944.45'), 
        'markValue': 394445, 'riskValue': 394445, 
        'homeNotional': Decimal('0.05181347150259067'), 
        'foreignNotional': Decimal('-204.37564766839378'), 
        'posState': '', 
        'posCost': 382490, 
        'posCost2': 386869, 
        'posCross': 4379, 
        'posInit': 3825, 
        'posComm': 196, 
        'posLoss': 4379, 
        'posMargin': 4021, 
        'posMaint': 2774, 
        'posAllowance': 0, 
        'taxableMargin': 0, 
        'initMargin': 0, 
        'maintMargin': 15976, 
        'sessionMargin': 0, 
        'targetExcessMargin': 0, 
        'varMargin': 0, 
        'realisedGrossPnl': 0, 
        'realisedTax': 0, 
        'realisedPnl': -1385, 
        'unrealisedGrossPnl': 11955, 
        'longBankrupt': 0, 
        'shortBankrupt': 0, 
        'taxBase': 0, 
        'indicativeTaxRate': None, 
        'indicativeTax': 0, 
        'unrealisedTax': 0, 
        'unrealisedPnl': 11955, 
        'unrealisedPnlPcnt': Decimal('0.0313'), 
        'unrealisedRoePcnt': Decimal('3.1256'), 
        'simpleQty': None, 
        'simpleCost': None, 
        'simpleValue': None, 
        'simplePnl': None, 
        'simplePnlPcnt': None, 
        'avgCostPrice': Decimal('3824.9'), 
        'avgEntryPrice': Decimal('3824.9'), 
        'breakEvenPrice': Decimal('3870.6'), 
        'marginCallPrice': 0, 
        'liquidationPrice': 0, 
        'bankruptPrice': 0, 
        'timestamp': datetime.datetime(2021, 9, 6, 6, 34, 35, 385000, tzinfo=datetime.timezone.utc), 
        'lastPrice': Decimal('3944.45'), 
        'lastValue': 394445
        }
        """

        LOG.info("Handle position msg")
        for data in msg['data']:
            parsed_data = self.parse_position(data)
            await self.callback(POSITIONS, feed=self.id, **parsed_data)

    async def _balance(self, msg: dict, timestamp: float):
        """
        {
        feed': 'BITMEX', 
        'account': 917702, 
        'currency': 'XBt', 
        'riskLimit': 1000000000000, 
        'prevState': '', 
        'state': '', 
        'action': '', 
        'amount': 954832, 
        'pendingCredit': 0, 
        'pendingDebit': 0, 
        'confirmedDebit': 0, 
        'prevRealisedPnl': 0, 
        'prevUnrealisedPnl': 0, 
        'grossComm': 720, 
        'grossOpenCost': 2806759, 
        'grossOpenPremium': 0, 
        'grossExecCost': 0, 
        'grossMarkValue': 390502, 
        'riskValue': 3197261, 
        'taxableMargin': 0, 
        'initMargin': 30889, 
        'maintMargin': 12033, 
        'sessionMargin': 0, 
        'targetExcessMargin': 0, 
        'varMargin': 0, 
        'realisedPnl': -720, 
        'unrealisedPnl': 8012, 
        'indicativeTax': 0, 
        'unrealisedProfit': 0, 
        'syntheticMargin': None, 
        'walletBalance': 954112, 
        'marginBalance': 962124, 
        'marginBalancePcnt': 1, 
        'marginLeverage': Decimal('0.40587491840968526'), 
        'marginUsedPcnt': Decimal('0.0446'), 
        'excessMargin': 919202, 
        'excessMarginPcnt': 1, 
        'availableMargin': 919202, 
        'withdrawableMargin': 919202, 
        'timestamp': datetime.datetime(2021, 9, 6, 3, 43, 45, 387000, tzinfo=datetime.timezone.utc), 
        'grossLastValue': 390502, 
        'commission': None
        }
        """

        LOG.info("Handle margin msg")
        for data in msg['data']:
            parsed_data = self.parse_margin(data)
            await self.callback(BALANCES, feed=self.id, **parsed_data)

    async def message_handler(self, msg: str, conn, timestamp: float):
        msg = json.loads(msg, parse_float=Decimal)
        if 'table' in msg:
            if msg['table'] == 'trade':
                await self._trade(msg, timestamp)
            elif msg['table'] == 'orderBookL2':
                await self._book(msg, timestamp)
            elif msg['table'] == 'funding':
                await self._funding(msg, timestamp)
            elif msg['table'] == 'instrument':
                await self._instrument(msg, timestamp)
            elif msg['table'] == 'quote':
                await self._ticker(msg, timestamp)
            elif msg['table'] == 'liquidation':
                await self._liquidation(msg, timestamp)
            elif msg['table'] == 'order':
                await self._order(msg, timestamp)
            elif msg['table'] == 'position':
                await self._position(msg, timestamp)
            elif msg['table'] == 'margin':
                await self._balance(msg, timestamp)
            else:
                LOG.warning("%s: Unhandled table=%r in %r", conn.uuid, msg['table'], msg)
        elif 'info' in msg:
            LOG.info("%s: Info message from exchange: %s", conn.uuid, msg)
        elif 'subscribe' in msg:
            if not msg['success']:
                LOG.error("%s: Subscribe failure: %s", conn.uuid, msg)
        elif 'error' in msg:
            LOG.error("%s: Error message from exchange: %s", conn.uuid, msg)
        elif 'request' in msg:
            if msg['success']:
                LOG.info("%s: Success %s", conn.uuid, msg['request'].get('op'))
            else:
                LOG.warning("%s: Failure %s", conn.uuid, msg['request'])
        else:
            LOG.warning("%s: Unexpected message from exchange: %s", conn.uuid, msg)

    async def subscribe(self, conn: AsyncConnection):
        self._reset()
        await self._authenticate(conn)
        chans = []
        for chan in self.subscription:
            if chan in Bitmex.channels_without_symbol:
                chans.append(f"{chan}")
            else:
                for pair in self.subscription[chan]:
                    chans.append(f"{chan}:{pair}")   

        for i in range(0, len(chans), 10):
            await conn.write(json.dumps({"op": "subscribe",
                                         "args": chans[i:i + 10]}))

    async def _authenticate(self, conn: AsyncConnection):
        """Send API Key with signed message."""
        # Docs: https://www.bitmex.com/app/apiKeys
        # https://github.com/BitMEX/sample-market-maker/blob/master/test/websocket-apikey-auth-test.py
        if self.key_id and self.key_secret:
            LOG.info('%s: Authenticate with signature', conn.uuid)
            expires = int(time.time()) + 365 * 24 * 3600  # One year
            msg = f'GET/realtime{expires}'.encode('utf-8')
            signature = hmac.new(self.key_secret.encode('utf-8'), msg, digestmod=hashlib.sha256).hexdigest()
            await conn.write(json.dumps({'op': 'authKeyExpires', 'args': [self.key_id, expires, signature]}))
