# -*- coding: utf-8 -*-

from operator import itemgetter
import time

from bitrue.websockets import BitrueSocketManager
from bitrue.helpers import gen_depth_sub_msg, equals_zero


class DepthCache(object):

    def __init__(self, symbol):
        """initialize the DepthCache

        Args:
            symbol (string): symbol to create depth cache for 
        """
        self.symbol = symbol
        self._bids = {}
        self._asks = {}
        self.update_time = None

    def add_bid(self, bid):
        """add a bid to the cache

        Args:
            bid (array): [price, volume]
        """
        self._bids[bid[0]] = bid[1]
        if equals_zero(self._bids[bid[0]]):
            del self._bids[bid[0]]
    
    def add_ask(self, ask):
        """add a ask to the cache

        Args:
            ask (array): [price,volume]
        """
        self._asks[ask[0]] = ask[1]
        if equals_zero(self._asks[ask[0]]):
            del self._asks[ask[0]]
    
    def get_bids(self):
        """get the current bids
        """
        return DepthCache.sort_depth(self._bids, reversed=True)
    
    def get_asks(self):
        """get the current asks
        """
        return DepthCache.sort_depth(self._asks, reversed=False)
    
    # def 
    #     """to string

    #     Returns:
    #         [type]: [description]
    #     """
    #     if not self._asks or not self._bids:
    #         return "%s,%s" %(self._asks[0][0] if self._asks else "empty", self._bids[0][0] if self._bids else "empty")
    #     return "bbo:%d|%s, %d,%d" %(self._bids[0][0], self._asks[0][0], len(self._bids), len(self._asks))
    
    @staticmethod
    def sort_depth(vals, reversed=False):
        """sort bids or asks by price

        Args:
            vals (2d demis array): [[price1,volume1],[price2,volume2]]
            reversed (bool, optional): [description]. Defaults to False.
        """
        lst = [[float(price), quantity] for price, quantity in vals.items()]
        lst = sorted(lst, key=itemgetter(0), reverse=reversed)
        return lst

class DepthCacheManager(object):

    _default_refresh = 60 * 30

    def __init__(self,  symbol, client, callback=None, refresh_interval=_default_refresh, bm=None, limit=0, ws_interval=None):
        """initialize the DepthCacheManager

        Args:
            symbol ([type]): [description]
            callback ([type], optional): [description]. Defaults to None.
            refresh_interval ([type], optional): [description]. Defaults to _default_refresh.
            bm ([type], optional): [description]. Defaults to None.
            limit (int, optional): [description]. Defaults to 500.
            ws_interval ([type], optional): [description]. Defaults to None.
        """
        self._client = client
        self._symbol = symbol
        self._limit = limit
        self._depth_cache = None
        self._callback = callback
        self._last_update_id = None
        self._depth_message_buffer = []
        self._bm = bm
        self._refresh_interval = refresh_interval
        self._conn_key = None
        self._ws_interval = ws_interval

        self._init_cache()
        self._start_socket()
    
    def _start_socket(self):
        """start the depth cache socket
        """
        if self._bm is None:
            self._bm = BitrueSocketManager()
        
        self._conn_key = self._bm.start_depth_socket(self._symbol, self._depth_event, self._subscribe, interval=self._ws_interval)
        if not self._bm.is_alive():
            self._bm.start()
        
        # wait for some socket responses
        # while not len(self._depth_message_buffer):
        #     time.sleep(1)
    
    def _subscribe(self):
        lower_symbol = self._symbol.lower()
        # return '{"event":"sub","params":{"cb_id":"%s","channel":"market_%s_depth_step%d"}}' %(lower_symbol, lower_symbol, self._limit)
        return gen_depth_sub_msg(self._symbol.lower(), self._limit)
    
    def _depth_event(self, data):
        """handle a depth msg

        Args:
            data (json): json object
            {'channel': 'market_ethbtc_depth_step0', 'ts': 1615377930695, 'tick': {'buys': [['0.033085', 0.051]...], 'asks': [['0.033086', 0.046]...]}}
        """   
        self._process_depth_message(data)

    def _init_cache(self):
        """initiailze the depth cache calling REST endpoint
        """
        self._last_update_id = None
        self._depth_message_buffer = []

        # initialize or clear from the order book
        self._depth_cache = DepthCache(self._symbol)

        # process bid and ask from the order book

        # set a time to refresh the depth cache
        if self._refresh_interval:
            self._refresh_time = int(time.time() + self._refresh_interval)
        
        # apply any updates from the websocket
        for data in self._depth_message_buffer:
            self._process_depth_message(data, buffer=True)
        
        # clear the depth buffer
        self._depth_message_buffer = []
    
    def _process_depth_message(self, data, buffer=False):
        """process a depth event message

        Args:
            data (json object): {'channel': 'market_ethbtc_depth_step0', 'ts': 1615377930695, 'tick': {'buys': [['0.033085', 0.051]...], 'asks': [['0.033086', 0.046]...]}}
            buffer (bool, optional): [description]. Defaults to False.
        """
        if 'tick' not in data:
            return
        
        bids = data['tick']['buys']
        asks = data['tick']['asks']
        
        if bids:
            for bid in bids:
                self._depth_cache.add_bid(bid)
        if asks:
            for ask in asks:
                self._depth_cache.add_ask(ask)
        
        # keep update time
        self._depth_cache.update_time = data['ts']

        # call the callback with the udpated depth cache
        if self._callback:
            self._callback(self._depth_cache)
        
        # after processing event to see if we need to refresh the depth cache
        if self._refresh_interval and int(time.time()) > self._refresh_time:
            self._init_cache()
    
    def get_depth_cache(self):
        """get current depth cache
        """
        return self._depth_cache
    
    def close(self, close_socket=False):
        """Close the open socket for this manager

        Args:
            close_socket (bool, optional): [description]. Defaults to False.
        """
        self._bm.stop_socket(self._conn_key)
        if close_socket:
            self._bm.close()
        time.sleep(1)
        self._depth_cache = None
    
    def get_symbol(self):
        """get the symbol
        """
        return self._symbol



if __name__ == '__main__':
    btrusdt = DepthCacheManager('ethbtc', None)
    while True:
        depth_cache = btrusdt.get_depth_cache()
        bids = depth_cache.get_bids()
        asks = depth_cache.get_asks()
        print(bids, "\n########\n", asks)
        time.sleep(2)
    