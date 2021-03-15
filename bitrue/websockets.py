#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import threading
import gzip
import time

from autobahn.twisted.websocket import WebSocketClientFactory, WebSocketClientProtocol, connectWS
from twisted.internet import reactor, ssl
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet.error import ReactorAlreadyRunning

import ujson as json

from bitrue.helpers import gen_depth_channel, gen_ticker_channel, gen_kline_channel, gen_trade_channel

class BitrueClientProtocol(WebSocketClientProtocol):

    def __init__(self):
        super(WebSocketClientProtocol, self).__init__()
            
    def onConnect(self, response):
        # reset the delay after reconnecting
        self.factory.resetDelay()
    
    def onOpen(self):
        msg = self.factory.subscribe()
        # print(msg)
        self.sendMessage(msg.encode("utf8"))
    
    def onMessage(self, playload, isBinary):
        msg = BitrueClientProtocol.gzip_inflate(playload) if isBinary else playload
        # print(msg)
        try:
            payload_obj = json.loads(msg.decode("utf8"))
        except ValueError:
            pass
        else:
            self.factory.callback(payload_obj)
    
    def onClose(self, wasClean, code, reason):
        # print("%s,%s,%s" %(wasClean, code, reason))
        self.factory.callback(None)

    def onPing(self, playload):
        self.sendMessage('{"pong":%d}'%(int(time.time()*1000)).encode("utf8"))

    
    @staticmethod
    def gzip_inflate(data):
        return gzip.decompress(data)

class BitrueReconnectingClientFactory(ReconnectingClientFactory):

    # set initial delay to a short time
    initialDelay = 0.1

    maxDelay = 10

    maxRetries = 5

class BitrueClientFactory(WebSocketClientFactory, BitrueReconnectingClientFactory):

    protocol = BitrueClientProtocol
    _reconnect_error_payload = {
        'e': 'error',
        'm': "Max reconnect retries reached"
    }

    def clientConnectionFailed(self, connector, reason):
        self.retry(connector)
        if self.retries > self.maxRetries:
            self.callback(self._reconnect_error_payload)
    
    def clientConnectionLost(self, connector, reason):
        self.retry(connector)
        if self.retries > self.maxRetries:
            self.callback(self._reconnect_error_payload)


class BitrueSocketManager(threading.Thread):

    STREAM_URL = "wss://ws.bitrue.com/kline-api/ws"

    WEBSOCKET_DEPTH_5 = "5"
    WEBSOCKET_DEPTH_10 = "10"
    WEBSOCKET_DEPTH_20 = "20"

    DEFAULT_USER_TIMEOUT = 30 * 60  # 30 mintes

    def __init__(self, user_timeout=DEFAULT_USER_TIMEOUT):
        """initialize the BitrueSocketManager

        Args:
            user_timeout ([int], optional): [default timeout]. Defaults to DEFAULT_USER_TIMEOUT.
        """
        threading.Thread.__init__(self)
        self._conns = {}
        self._user_timeout = user_timeout
        self._timers = {'user': None, 'margin':None}
        self._listen_keys = {'user':None, 'margin':None}
        self._account_callbacks = {'user': None, 'margin':None}

    def _start_socket(self, name, subscribe, callback):
        if name in self._conns:
            return False
        
        factory = BitrueClientFactory(self.STREAM_URL)
        factory.protocol = BitrueClientProtocol
        factory.subscribe = subscribe
        factory.callback = callback
        factory.reconnect = True
        context_factory = ssl.ClientContextFactory()

        self._conns[name] = connectWS(factory, context_factory)
        return name
    
    def start_depth_socket(self, symbol, callback, subscribe=None, depth=0, interval=None):
        """subscribe depth for symbol

        Args:
            symbol ([type]): [description]
            subscribe ([type]): [description]
            callback (function): [description]
            depth ([type], optional): [description]. Defaults to 0.
            interval ([type], optional): [description]. Defaults to None.
        """
        socket_name = gen_depth_channel(symbol.lower())
        return self._start_socket(socket_name, subscribe, callback=callback)
    
    def start_kline_socket(self, symbol, callback, subscribe=None, interval=''):
        pass

    def start_trade_socket(self, symbol, callback, subscribe=None):
        pass

    def start_symbol_ticker_socket(self, symbol, callback, subscribe=None):
        """subscribe ticker stream for given symbol

        Args:
            symbol ([type]): [description]
            callback (function): [description]
            subscribe (function, optional): subscribe message for ticker subscribe. Defaults to None.

        Returns:
            [type]: [description]
        """
        socket_name = gen_ticker_channel(symbol.lower())
        return self._start_socket(socket_name, subscribe, callback=callback)

    
    def stop_socket(self, conn_key):
        """stop a websocket given the connection key

        Args:
            conn_key (string): the connection key
        """
        if conn_key not in self._conns:
            return
        
        # disable reconnectiong if we are closing
        self._conns[conn_key].factory = WebSocketClientFactory(self.STREAM_URL + "?error")
        self._conns[conn_key].disconnect()
        del(self._conns[conn_key])
    
    def run(self):
        try:
            reactor.run(installSignalHandlers=False)
        except ReactorAlreadyRunning:
            # Ignore error abount reactor already running
            pass
    
    def close(self):
        """Close all connections
        """
        keys = set(self._conns.keys())
        for key in keys:
            self.stop_socket(key)
        
        self._conns = {}