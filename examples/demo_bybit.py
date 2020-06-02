from dotenv import load_dotenv
from pathlib import Path

from cryptofeed.callback import TradeCallback, BookCallback
from cryptofeed.backends.redis import OrderRedis
from cryptofeed import FeedHandler
from cryptofeed.exchanges import Bybit
from cryptofeed.defines import TRADES, L2_BOOK, ORDER, BID, ASK

# Look for .env file in the directory where this process runs.
env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path, verbose=True, override=True)

async def trade(feed, pair, order_id, timestamp, side, amount, price):
    print(f"Timestamp: {timestamp} Feed: {feed} Pair: {pair} ID: {order_id} Side: {side} Amount: {amount} Price: {price}")


async def book(feed, pair, book, timestamp):
    print(f'Timestamp: {timestamp} Feed: {feed} Pair: {pair} Book Bid Size is {len(book[BID])} Ask Size is {len(book[ASK])}')


def main():
    f = FeedHandler()

    f.add_feed(Bybit(pairs=['BTC-USD', 'ETH-USD', 'XRP-USD', 'EOS-USD'], channels=[TRADES], callbacks={TRADES: TradeCallback(trade)}))
    f.add_feed(Bybit(pairs=['BTC-USD', 'ETH-USD', 'XRP-USD', 'EOS-USD'], channels=[L2_BOOK], callbacks={L2_BOOK: BookCallback(book)}))
    f.add_feed(Bybit(config={ORDER:['BTC-USD']}, callbacks={ORDER: OrderRedis()}, use_private_channels=True))
    f.run()


if __name__ == '__main__':
    main()
