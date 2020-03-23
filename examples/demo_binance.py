from dotenv import load_dotenv
import asyncio

from cryptofeed.backends.redis import BookLatestRedis, OrderRedis
from cryptofeed import FeedHandler
from cryptofeed.exchanges import Binance
from cryptofeed.defines import L2_BOOK, ORDER

load_dotenv(verbose=True)

async def main():
    f = FeedHandler()
    f.add_feed(Binance(use_private_channels=True, max_depth=2, pairs=['BTC-USDT'], channels=[ORDER], callbacks={ORDER: OrderRedis()}))
    f.add_feed(Binance(max_depth=2, pairs=['BTC-USDT'], channels=[L2_BOOK], callbacks={L2_BOOK: BookLatestRedis()}))
    f.run(start_loop=False)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(main())
    loop.run_forever()
