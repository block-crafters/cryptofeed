from dotenv import load_dotenv

from cryptofeed.backends.redis import BookLatestRedis, PositionRedis, OrderRedis
from cryptofeed import FeedHandler
from cryptofeed.exchanges import Bitmex
from cryptofeed.defines import L2_BOOK, POSITION, ORDER

load_dotenv(verbose=True)

def main():
    f = FeedHandler()

    f.add_feed(Bitmex(max_depth=2, pairs=['XBTUSD'], channels=[L2_BOOK], callbacks={L2_BOOK: BookLatestRedis()}))
    f.add_feed(Bitmex(use_private_channels=True, pairs=['XBTUSD'], channels=[POSITION], callbacks={POSITION: PositionRedis()}))
    f.add_feed(Bitmex(use_private_channels=True, pairs=['XBTUSD'], channels=[ORDER], callbacks={ORDER: OrderRedis()}))
    f.run()


if __name__ == '__main__':
    main()
