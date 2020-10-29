'''
Copyright (C) 2017-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import dotenv

from cryptofeed.callback import TickerCallback, TradeCallback, BookCallback
from cryptofeed.backends.redis import OrderRedis
from cryptofeed import FeedHandler
from cryptofeed.exchanges import OKEx
from cryptofeed.defines import L2_BOOK_SWAP, L2_BOOK, BID, ASK, TRADES, TRADES_SWAP, ORDER_SWAP

dotenv.load_dotenv()


async def trade(feed, pair, order_id, timestamp, side, amount, price):
    print(f"Timestamp: {timestamp} Feed: {feed} Pair: {pair} ID: {order_id} Side: {side} Amount: {amount} Price: {price}")


async def book(feed, pair, book, timestamp):
    print(f'Timestamp: {timestamp} Feed: {feed} Pair: {pair} Book Bid Size is {len(book[BID])} Ask Size is {len(book[ASK])}')


def main():
    fh = FeedHandler(timeout_interval=180)
    fh.add_feed(OKEx(pairs=['ETH-USD-SWAP', 'ETH-USDT-SWAP'], channels=[ORDER_SWAP], callbacks={ORDER_SWAP: OrderRedis()}, use_private_channels=True))

    fh.run()


if __name__ == '__main__':
    main()
