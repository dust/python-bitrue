#  coding=utf-8

import math
from datetime import datetime
import collections

import dateparser
import pytz


def date_to_milliseconds(date_str):
    """Convert UTC date to milliseconds

    If using offset strings add "UTC" to date string e.g. "now UTC", "11 hours ago UTC"

    See dateparse docs for formats http://dateparser.readthedocs.io/en/latest/

    :param date_str: date in readable format, i.e. "January 01, 2018", "11 hours ago UTC", "now UTC"
    :type date_str: str
    """
    # get epoch value in UTC
    epoch = datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)
    # parse our date string
    d = dateparser.parse(date_str)
    # if the date is not timezone aware apply UTC timezone
    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        d = d.replace(tzinfo=pytz.utc)

    # return the difference in time
    return int((d - epoch).total_seconds() * 1000.0)


def interval_to_milliseconds(interval):
    """Convert a Binance interval string to milliseconds

    :param interval: Binance interval string, e.g.: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w
    :type interval: str

    :return:
         int value of interval in milliseconds
         None if interval prefix is not a decimal integer
         None if interval suffix is not one of m, h, d, w

    """
    seconds_per_unit = {
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
        "w": 7 * 24 * 60 * 60,
    }
    try:
        return int(interval[:-1]) * seconds_per_unit[interval[-1]] * 1000
    except (ValueError, KeyError):
        return None

def gen_depth_channel(lower_symbol, depth=0):
    return "market_%s_depth_step%d" %(lower_symbol, depth)

def gen_ticker_channel(lower_symbol):
    return "market_%s_ticker" %(lower_symbol,)

def gen_kline_channel(lower_symbol, interval='1min'):
    return 'market_$%s_kline_%s' %(lower_symbol, interval)

def gen_trade_channel(lower_symbol):
    return 'market_%s_trade_ticker' %(lower_symbol,)

def gen_depth_sub_msg(lower_symbol, depth=0):
    return '{"event":"sub","params":{"cb_id":"%s","channel":"%s"}}' %(lower_symbol, gen_depth_channel(lower_symbol, depth))

def gen_ticker_sub_msg(lower_symbol):
    return '{"event":"sub","params":{"cb_id":"%s","channel":"%s"}}' %(lower_symbol, gen_ticker_channel(lower_symbol))

def gen_kline_sub_msg(lower_symbol, interval='1min'):
    return '{"event":"sub","params":{"channel":"%s","cb_id":"%s"}}' %(gen_kline_channel(lower_symbol, interval), lower_symbol)

def gen_trade_sub_msg(lower_symbol):
    return '{"event":"sub","params":{"cb_id":"%s","channel":"%s"}}' %(lower_symbol, gen_trade_channel(lower_symbol))

def equals(f1, f2, rel_tol=1e-9, abs_tol=0.0):
    return math.isclose(f1, f2, rel_tol=rel_tol, abs_tol=abs_tol)

def equals_zero(f1, rel_tol=1e-9, abs_tol=0.0):
    return equals(f1, 0.0, rel_tol, abs_tol)

def extend(*args):
    if args is not None:
        result = None
        if type(args[0]) is collections.OrderedDict:
            result = collections.OrderedDict()
        else:
            result = {}
        for arg in args:
            result.update(arg)
        return result
    return {}

