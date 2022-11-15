from enum import Enum, unique
from threading import RLock
from decimal import Decimal

import logging

from sortedcontainers import SortedDict, SortedKeyList


@unique
class Side(Enum):
    BID = 0,
    ASK = 1

_ZERO = 0  # Decimal(0)

def to_dec(num=None, precision=None):
    if num is None:
        return Decimal("0.%s" %("".zfill(4 if precision is None else precision)))
    return Decimal(num if isinstance(num, (str,)) else str(num))

def to_int(num, precision):
    dec = to_dec(num, precision)
    return int(dec * 10 ** precision)

def fmt_dec(num: int, precision):
    return round(to_dec(num / (10 ** precision)), precision)


class CompactOrdBk(object):
    """
    简洁版的order book. 没有累计数。
    """

    def __init__(self, seq=1, bids=None, asks=None, precision=4, vol_prec=4):
        """构造一个简洁版的order book.

        Args:
            seq (int, optional): [序列、版本号]. Defaults to 1.
            bids ([type], optional): [买入订单项的集合，包括两个元素，0：价格，1：数量。]. Defaults to None.
            asks ([type], optional): [卖出订单项的集合，包括两个元素，0：价格，1：数量。]. Defaults to None.
            precision (int, optional): [价格精度，即价格的小数点位数]. Defaults to 4，这并不是一个合理的参数值。
            volume_prec (int, optional): [数量精度，即数量的小数点位数。]. Defaults to 4.
        """
        self.seq = seq
        self.precision = precision
        self.volume_prec = vol_prec
        self.bid_ob = SortedDict()
        self.ask_ob = SortedDict()
        self.lock = RLock()

        self.dbg_bid_set = set(())
        self.dbg_ask_set = set(())
        self.dbg_lck = RLock()
    
        if bids:
            for bid in bids:
                # self.bid_ob[bid[0]] = bid[1]  # [px,amt]
                self._add_bid(to_int(bid[0], self.precision), to_int(bid[1], self.volume_prec))
        if asks:
            for ask in asks:
                # self.ask_ob[ask[0]] = ask[1]  # [px, amt]
                self._add_ask(to_int(ask[0], self.precision), to_int(ask[1], self.volume_prec))
    
    def add_or_upd(self, side, px, amnt):
        if side == Side.BID:
            # self._dbg_add_bid(px)
            # self.bid_ob[px] = amnt
            self._add_bid(px, amnt)
        elif side == Side.ASK:
            # self._dbg_add_ask(px)
            # self.ask_ob[px] = amnt
            self._add_ask(px, amnt)

    def _add_bid(self, px, amnt):
        with self.lock:
            self.bid_ob[px] = amnt
    
    def _add_ask(self, px, amnt):
        with self.lock:
            self.ask_ob[px] = amnt
    
    def remove(self, side, px):
        if side == Side.BID:
            with self.lock:
                del self.bid_ob[px]
        elif side == Side.ASK:
            with self.lock:
                del self.ask_ob[px]
    
    def clear(self):
        with self.lock:
            self.bid_ob.clear()
            self.ask_ob.clear()
    
    def prefer(self, side, amnt, px=None, multiplier=1):
        exp_amnt = to_int(amnt, self.volume_prec) * multiplier
        p = to_int(px, self.precision) if px else None
        sum = 0
        # print(exp_amnt, self.ask_ob, self.bid_ob)
        if side == Side.BID:
            # r_idx = self.ask_ob.bisect_right(px)
            with self.lock:
                for i in range(0, len(self.ask_ob)):
                    (k, v) = self.ask_ob.peekitem(i)
                    sum += v  # k,v
                    if (p is None or  p >= k) and sum >= exp_amnt:
                        return (i, k, sum)
        elif side == Side.ASK:
            #
            with self.lock:
                for i in range(len(self.bid_ob)-1, -1, -1):
                    (k, v) = self.bid_ob.peekitem(i)
                    # print(k,v)
                    sum += v
                    if (p is None or p <= k) and sum >= exp_amnt:
                        return (i, k, sum)
        return (None, None, None)
    
    def get_best_bid(self):
        with self.lock:
            if not self.bid_ob or len(self.bid_ob) == 0:
                return None
            price = self.bid_ob.keys()[-1]
            return (fmt_dec(price, self.precision), fmt_dec(self.bid_ob[price], self.volume_prec))
    
    def get_best_ask(self):
        with self.lock:
            if not self.ask_ob or len(self.ask_ob) == 0:
                return None
            price = self.ask_ob.keys()[0]
            return (fmt_dec(price, self.precision), fmt_dec(self.ask_ob[price], self.volume_prec))
    
    def size(self, side):
        if side == Side.ASK:
            with self.lock:
                return len(self.ask_ob)
        elif side == Side.BID:
            with self.lock:
                return len(self.bid_ob)
    
    def level(self, side, px):
        p = to_int(px, self.precision)
        if side == Side.ASK:
            with self.lock:
                return self.ask_ob.bisect_right(p)
        elif side == Side.BID:
            with self.lock:
                return self.bid_ob.bisect_left(p)
    
    def reset(self, side, pairs, ts=None):
        if ts:
            self.seq = ts
        if side == Side.BID:
            with self.lock:
                self.bid_ob.clear()
            for bid in pairs:
                # self.bid_ob[bid[0]] = bid[1]  # [px,amt]
                self._add_bid(to_int(bid[0], self.precision), to_int(bid[1], self.volume_prec))
        elif side == Side.ASK:
            with self.lock:
                self.ask_ob.clear()
            for ask in pairs:
                self._add_ask(to_int(ask[0], self.precision), to_int(ask[1], self.volume_prec))
    
    def update_batch(self, side, pairs, ts):
        self.seq = ts
        if side == Side.BID:
            for bid in pairs:
                vol = to_int(bid[1], self.volume_prec)
                px = to_int(bid[0], self.precision)
                if vol > _ZERO:
                    self._add_bid(px, vol)
                else:
                    self.remove(side, px)
        elif side == Side.ASK:
            for ask in pairs:
                vol = to_int(ask[1], self.volume_prec)
                px = to_int(ask[0], self.precision)
                if vol > _ZERO:
                    self._add_ask(px, vol)
                else:
                    self.remove(side, px)
    
    def best_px(self, side):
        with self.lock:
            if side == Side.BID and len(self.bid_ob) > 0:
                return self.bid_ob.keys()[-1]
            elif side == Side.ASK and len(self.ask_ob) > 0:
                return self.ask_ob.keys()[0]
            return None
    
    def snapshot(self, side, top=5):
        if side == Side.BID:
            with self.lock:
                size = len(self.bid_ob)
                if size == 0:
                    return []
                keys = self.bid_ob.keys()[-top:]
                keys.reverse()
                return [[key, self.bid_ob[key]] for key in keys]
        elif side == Side.ASK:
            with self.lock:
                size = len(self.ask_ob)
                if size == 0:
                    return []
                keys = self.ask_ob.keys()[:top]
                return [[key, self.ask_ob[key]] for key in keys]
    
    def snapshot_txt(self, top=5):
        """
        output(self, px_precision, volume_precision):
        fmt = "%%.%df|%%.%df|%%.%df|%%.%df|%%.%df" % (px_precision, volume_precision, px_precision, volume_precision, px_precision)
        return fmt % (self.price, self.qty, self.amnt, self.acc_qty, self.acc_amnt)
        """
        bids = self.snapshot(Side.BID, top)
        asks = self.snapshot(Side.ASK, top)

        bid_str = ";".join(["%s|%s" %(fmt_dec(bid[0], self.precision), fmt_dec(bid[1], self.volume_prec)) for bid in bids])
        ask_str = ";".join(["%s|%s" %(fmt_dec(ask[0], self.precision), fmt_dec(ask[1], self.volume_prec)) for ask in asks])
        return "%s,BID,%s,%s,ASK,%s,%s" % (self.seq, len(bids), bid_str, len(asks), ask_str)
    
    def get_last_ts(self):
        return self.seq
    
    @staticmethod
    def parse(snapshot_txt, quote_prec, vol_prec):
        parts = snapshot_txt.split(',')
        seq_idx, bid_idx, ask_idx = (0, 3, 6) 
        bids = [bid.split('|') for bid in parts[bid_idx].split(";")]
        asks = [ask.split('|') for ask in parts[ask_idx].split(";")]
        return CompactOrdBk(int(parts[seq_idx]), bids, asks, precision=quote_prec, vol_prec=vol_prec)


class OrderEntry(object):
    """一个订单的数据结构
    """

    def __init__(self, price_level, _id, volume, status):
        self.price_level = price_level
        self.id = _id
        self.volume = volume
        self.status = status
    
    def __hash__(self):
        return self.id
    
    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.id == other.id

class PriceLevel(object):
    """ 在订单簿中的一个档位，包括价格及其价格下的订单。
    """

    def __init__(self, side, price):
        self.side = side
        self.price = price
        self.entry_lst = []
        self.lock = RLock()
    
    def get_price(self):
        return self.price
    
    def add(self, order_id, volume, status):
        order = OrderEntry(self, order_id, volume, status)
        with self.lock:
            self.entry_lst.append(order)
        return order
    
    def delete(self, order):
        with self.lock:
            self.entry_lst.remove(order)
    
    def get_order_ids(self):
        with self.lock:
            return [entry.id for entry in self.entry_lst]
    
    def get_volume(self):
        with self.lock:
            return sum([entry.volume for entry in self.entry_lst])

    def is_empty(self):
        with self.lock:
            return len(self.entry_lst) == 0
    
    def __hash__(self):
        return self.price
    
    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.price == other.price

class TrackBook(object):
    """某币对的未成交单包装数据结构。
    包含买，卖订单的价位及订单信息(id, price, volume等)。
    """

    def __init__(self, symbol, quote_prec, vol_prec):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.lock = RLock()
        self.symbol = symbol
        self.orders = SortedDict()
        self.quote_prec = quote_prec
        self.vol_prec = vol_prec
        self.bids = SortedKeyList(key=lambda pl: pl.get_price())
        self.asks = SortedKeyList(key=lambda pl: pl.get_price())
    
    def get_symbol(self):
        return self.symbol
    
    def get_order_cnt(self):
        return len(self.orders)
    
    def get_order(self, order_id):
        if order_id not in self.orders.keys():
            self.logger.info("get not exists order {}", order_id)
        else:
            order_entry = self.orders[order_id]
            return order_entry
    
    def get_order_size(self, order_id):
        order = self.get_order(order_id)
        if order:
            return fmt_dec(order.volume, self.vol_prec)
    
    @staticmethod
    def __add(levels, order_id, side, price, volume, status):
        idx = levels.bisect_key_left(price)
        level = None if idx == 0 or idx == len(levels) else levels[idx]
        if level is None:
            level = PriceLevel(side, price)
            levels.add(level)
        return level.add(order_id, volume, status)
    
    def __delete(self, entry):
        pl = entry.price_level
        pl.delete(entry)
        if pl.is_empty():
            if pl.side == Side.BID:
                self.bids.remove(pl)
            elif pl.side == Side.ASK:
                self.asks.remove(pl)

    
    def entry(self, order_id, side, price, volume, status):
        with self.lock:
            if  order_id in self.orders:
                return
            if side == Side.BID:
                self.orders[order_id] = self.__add(self.bids, order_id, side, to_int(price, self.quote_prec), to_int(volume, self.vol_prec), status)
            elif side == Side.ASK:
                self.orders[order_id] = self.__add(self.asks, order_id, side, to_int(price, self.quote_prec), to_int(volume, self.vol_prec), status)
    
    def new_size(self, order_id, side, price, remaining, status=None):
        with self.lock:
            if order_id not in self.orders:
                self.logger.warn("%s not found in TrackBook.new_size!", order_id)
                return
            entry = self.orders[order_id]
            entry.volume = to_int(remaining, self.vol_prec)
            if status is not None:
                entry.status = status
    
    def cancel(self, order_id):
        with self.lock:
            if order_id not in self.orders:
                self.logger.warn("%s not found in TrackBook.cancel!", order_id)
                return
            entry = self.orders[order_id]
            # if volume > entry.volume:
            #     self.logger.warn("%s cancel volume(%s) greater than entry.volume %s ", order_id, volume, entry.volume)
            self.__delete(entry)
            del self.orders[order_id]
    
    def remove(self, order_id):
        with self.lock:
            entry = self.orders.get(order_id)
            if entry is not None:
                self.__delete(entry)
                del self.orders[order_id]
    
    def get_best_bid(self):
        with self.lock:
            if not self.bids or len(self.bids) == 0:
                return None
            pl = self.bids[-1]
            return (fmt_dec(pl.get_price(), self.quote_prec), fmt_dec(pl.get_volume(), self.vol_prec), pl.get_order_ids())
    
    def get_best_ask(self):
        with self.lock:
            if not self.asks or len(self.asks) == 0:
                return None
            pl = self.asks[0]
            return (fmt_dec(pl.get_price(), self.quote_prec), fmt_dec(pl.get_volume(), self.vol_prec), pl.get_order_ids())
    
    def snapshot(self, side, top=5):
        if side == Side.BID:
            with self.lock:
                size = len(self.bids)
                if size == 0:
                    return []
                return reversed(self.bids)
        elif side == Side.ASK:
            with self.lock:
                size = len(self.asks)
                if size == 0:
                    return []
                return self.asks
    
    def dump(self, fo=None):
        bid_orders = self.snapshot(Side.BID, None)
        ask_orders = self.snapshot(Side.ASK, None)
        for o in bid_orders:
            if o.get_order_ids():
                line = "%d,%s,%s,%s" %(o.get_order_ids()[0], fmt_dec(o.get_price(), self.quote_prec), fmt_dec(o.get_volume(), self.vol_prec), "")
                if fo:
                    fo.write((line+"\n").encode())
                else:
                    print(line.encode())
        if fo:
            fo.write("\n\n ###### \n\n".encode())
        else:
            print("\n\n ###### \n\n".encode())
        for o in ask_orders:
            if o.get_order_ids():
                line = "%d,%s,%s,%s" %(o.get_order_ids()[0], fmt_dec(o.get_price(), self.quote_prec), fmt_dec(o.get_volume(), self.vol_prec), "")
                if fo:
                    fo.write((line + "\n").encode())
                else:
                    print(line.encode())


if __name__ == '__main__':
    px_precision = 6
    vol_precision = 0
    ob = CompactOrdBk(
        1, [[0.18394, 1], [0.18395, 2], [0.18396, 3], [0.18397, 4],[0.18398,5],[0.18399,6]], 
        [[0.18400, 4], [0.18401, 3], [0.18402, 2], [0.18403, 1], [0.18404,5], [0.18406, 6]], px_precision, vol_precision
    )
    print(ob.prefer(Side.ASK, to_dec(7)))
    print(ob.prefer(Side.ASK, to_dec(7.1)))
    print(ob.prefer(Side.ASK, to_dec(9)))
    print(ob.prefer(Side.ASK, to_dec(16)))
    print(ob.snapshot(Side.ASK))
    print(ob.snapshot(Side.BID))
    print(ob.snapshot_txt())
