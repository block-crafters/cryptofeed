from dotenv import load_dotenv

from cryptofeed.backends.redis import BookLatestRedis
from cryptofeed import FeedHandler
from cryptofeed.exchanges import Bitmex
from cryptofeed.defines import L2_BOOK

load_dotenv(verbose=True)

def main():
    f = FeedHandler()

    f.add_feed(Bitmex(max_depth=2, pairs=['XBTUSD'], channels=[L2_BOOK], callbacks={L2_BOOK: BookLatestRedis()}))
    f.run()


if __name__ == '__main__':
    main()
