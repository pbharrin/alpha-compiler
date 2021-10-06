
"""
A trading calendar that trades all the times.  Pretty useless I know, but
due to origins of trading systems we need this.
Inspired by this post.
https://pythonprogramming.net/custom-markets-trading-calendar-bitcoin-python-programming-for-finance/

Created by Peter Harrington (pbharrin) on 10/6/21.
"""
from trading_calendars import TradingCalendar
from pandas.tseries.offsets import CustomBusinessDay
from datetime import time
from pytz import timezone
from zipline.utils.memoize import lazyval


class TwentyFourSevenCal(TradingCalendar):
    """
    Exchange calendar for 24/7 trading.

    Open Time: 12am, UTC
    Close Time: 11:59pm, UTC
    """

    @property
    def open_times(self):
        return [(None, time(0, 0))]

    @property
    def close_times(self):
        return [(None, time(23, 59))]

    @property
    def name(self):
        return "twentyfoursevencal"

    @property
    def tz(self):
        return timezone("UTC")

    @lazyval
    def day(self):
        return CustomBusinessDay(
            weekmask='Mon Tue Wed Thu Fri Sat Sun',
        )