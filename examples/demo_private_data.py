from cryptofeed import FeedHandler
from cryptofeed.backends.redis import BalancesLatestRedis, PositionsLatestRedis
from cryptofeed.defines import BALANCES, POSITIONS


def main():
    feed_handler_config = {
        "log": {
            "filename": f"demo_private_data.log",
            "level": "DEBUG",
        },
        "bitmex": {
            "key_id": "<<YOUR-KEY-ID>>",
            "key_secret": "<<YOUR-KEY-SECRET>>",
        },

        "bybit": {
            "key_id": "<<YOUR-KEY-ID>>",
            "key_secret": "<<YOUR-KEY-SECRET>>",
        },
    }

    # TODO: control connection time for multi-processes
    f = FeedHandler(config=feed_handler_config)
    # balance
    f.add_feed("BITMEX", channels=[BALANCES], symbols=['BTC-USD-PERP'], callbacks={BALANCES: BalancesLatestRedis()}, custom_account_id="dev-bitmex")
    f.add_feed("BYBIT", channels=[BALANCES], symbols=['ETH-USD-PERP'], callbacks={BALANCES: BalancesLatestRedis()}, custom_account_id="dev-bybit")

    # Position
    f.add_feed("BITMEX", channels=[POSITIONS], symbols=['ETH-USD-PERP', 'ETH-BTC-22U30'], callbacks={POSITIONS: PositionsLatestRedis()}, custom_account_id="dev-bitmex")
    f.add_feed("BYBIT", channels=[POSITIONS], symbols=['ETH-USD-PERP'], callbacks={POSITIONS: PositionsLatestRedis()}, custom_account_id="dev-bybit")

    f.run()


if __name__ == '__main__':
    main()
