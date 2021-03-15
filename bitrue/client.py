# coding=utf-8

import hashlib
import hmac
import requests
import time
from operator import itemgetter
from bitrue.helpers import date_to_milliseconds, interval_to_milliseconds, extend
from bitrue.exceptions import BitrueAPIException, BitrueRequestException


class Client(object):

    API_URL = 'https://www.bitrue.{}/api'
    WEBSITE_URL = 'https://www.bitrue.{}'
    
    PUBLIC_API_VERSION = 'v1'
    PRIVATE_API_VERSION = 'v1'
    
    SYMBOL_TYPE_SPOT = 'SPOT'

    ORDER_STATUS_NEW = 'NEW'
    ORDER_STATUS_PARTIALLY_FILLED = 'PARTIALLY_FILLED'
    ORDER_STATUS_FILLED = 'FILLED'
    ORDER_STATUS_CANCELED = 'CANCELED'
    ORDER_STATUS_PENDING_CANCEL = 'PENDING_CANCEL'
    ORDER_STATUS_REJECTED = 'REJECTED'
    ORDER_STATUS_EXPIRED = 'EXPIRED'

    KLINE_INTERVAL_1MINUTE = '1m'
    KLINE_INTERVAL_3MINUTE = '3m'
    KLINE_INTERVAL_5MINUTE = '5m'
    KLINE_INTERVAL_15MINUTE = '15m'
    KLINE_INTERVAL_30MINUTE = '30m'
    KLINE_INTERVAL_1HOUR = '1h'
    KLINE_INTERVAL_2HOUR = '2h'
    KLINE_INTERVAL_4HOUR = '4h'
    KLINE_INTERVAL_6HOUR = '6h'
    KLINE_INTERVAL_8HOUR = '8h'
    KLINE_INTERVAL_12HOUR = '12h'
    KLINE_INTERVAL_1DAY = '1d'
    KLINE_INTERVAL_3DAY = '3d'
    KLINE_INTERVAL_1WEEK = '1w'
    KLINE_INTERVAL_1MONTH = '1M'

    SIDE_BUY = 'BUY'
    SIDE_SELL = 'SELL'

    ORDER_TYPE_LIMIT = 'LIMIT'
    ORDER_TYPE_MARKET = 'MARKET'
    ORDER_TYPE_STOP_LOSS = 'STOP_LOSS'
    ORDER_TYPE_STOP_LOSS_LIMIT = 'STOP_LOSS_LIMIT'
    ORDER_TYPE_TAKE_PROFIT = 'TAKE_PROFIT'
    ORDER_TYPE_TAKE_PROFIT_LIMIT = 'TAKE_PROFIT_LIMIT'
    ORDER_TYPE_LIMIT_MAKER = 'LIMIT_MAKER'

    TIME_IN_FORCE_GTC = 'GTC'  # Good till cancelled
    TIME_IN_FORCE_IOC = 'IOC'  # Immediate or cancel
    TIME_IN_FORCE_FOK = 'FOK'  # Fill or kill

    ORDER_RESP_TYPE_ACK = 'ACK'
    ORDER_RESP_TYPE_RESULT = 'RESULT'
    ORDER_RESP_TYPE_FULL = 'FULL'

    # For accessing the data returned by Client.aggregate_trades().
    AGG_ID = 'a'
    AGG_PRICE = 'p'
    AGG_QUANTITY = 'q'
    AGG_FIRST_TRADE_ID = 'f'
    AGG_LAST_TRADE_ID = 'l'
    AGG_TIME = 'T'
    AGG_BUYER_MAKES = 'm'
    AGG_BEST_MATCH = 'M'


    def __init__(self, api_key=None, api_secret=None, requests_params=None, tld='com'):
        """Bitrue API Client constructor
        :param api_key: Api Key
        :type api_key: str.
        :param api_secret: Api Secret
        :type api_secret: str.
        :param requests_params: optional - Dictionary of requests params to use for all calls
        :type requests_params: dict.
        """

        self.API_URL = self.API_URL.format(tld)
        # self.WITHDRAW_API_URL = self.WITHDRAW_API_URL.format(tld)
        # self.MARGIN_API_URL = self.MARGIN_API_URL.format(tld)
        self.WEBSITE_URL = self.WEBSITE_URL.format(tld)
        # self.FUTURES_URL = self.FUTURES_URL.format(tld)
        # self.FUTURES_DATA_URL = self.FUTURES_DATA_URL.format(tld)
        # self.FUTURES_COIN_URL = self.FUTURES_COIN_URL.format(tld)
        # self.FUTURES_COIN_DATA_URL = self.FUTURES_COIN_DATA_URL.format(tld)

        self.API_KEY = api_key
        self.API_SECRET = api_secret
        self.session = self._init_session()
        self._requests_params = requests_params
        self.response = None
        self.timestamp_offset = 0

        # init DNS and SSL cert
        self.ping()
        # calculate timestamp offset between local and Bitrue server
        res = self.get_server_time()
        self.timestamp_offset = res['serverTime'] - int(time.time() * 1000)
    
    def _init_session(self):
        session = requests.session()
        session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'Bitrue/Python',
            'X-MBX-APIKEY': self.API_KEY
        })
        return session
    
    def _create_api_uri(self, path, signed=True, version=PUBLIC_API_VERSION):
        return self.API_URL + '/' + version + '/' + path
    
    def _create_website_uri(self, path):
        return self.WEBSITE_URL + "/" + path
    
    def _generate_signature(self, data):
        # ordered_data = self._order_params(data)
        query_string = '&'.join(["{}={}".format(k, v) for k, v in data.items()])
        m = hmac.new(self.API_SECRET.encode("utf-8"), query_string.encode('utf-8'), hashlib.sha256)
        return m.hexdigest()
    
    def _order_params(self, data):
        """Convert params to list with signature as last element.

        Args:
            data ([type]): [description]
        """
        has_signature = False
        params = []
        for k, v in data.items():
            if k == 'signature':
                has_signature = True
            else:
                params.append((k, v))
        # sort parameters by key
        if has_signature:
            params.append(('signature', data['signature']))
        return params
    
    def _reqeust(self, method, uri, signed, force_params=False, **kwargs):

        # set default request timeout
        kwargs['timeout'] = 10

        # add our global requests params
        if self._requests_params:
            kwargs.update(self._requests_params)
        
        data = kwargs.get('data', None)
        if data and isinstance(data, dict):
            kwargs['data'] = data

            # find any requests params passed and apply them
            if 'requests_params' in kwargs['data']:
                # merge requests params into kwargs
                kwargs.update(kwargs['data']['requests_params'])
                del(kwargs['data']['requests_params'])
        
        if signed:
            # generate signature
            kwargs['data']['timestamp'] = int(time.time() * 1000 + self.timestamp_offset)
            kwargs['data']['signature'] = self._generate_signature(kwargs['data'])
        
        # sort get and post params to match signature order
        if data:
            # sort post params
            kwargs['data'] = self._order_params(kwargs['data'])
            # remove any arguments with values of None
            null_args = [i for i, (k, v) in enumerate(kwargs['data']) if v is None]
            for i in reversed(null_args):
                del(kwargs['data'][i])
        
        # if get request assign data array to params value for requests lib
        if data and (method == 'get' or force_params):
            kwargs['params'] = '&'.join('%s=%s' %(data[0], data[1]) for data in kwargs['data'])
            del(kwargs['data'])
        self.response = getattr(self.session, method)(uri, **kwargs)
        return self._handle_response()
    
    def _request_api(self, method, path, signed=False, version=PUBLIC_API_VERSION, **kwargs):
        uri = self._create_api_uri(path, signed, version)
        return self._reqeust(method, uri, signed, **kwargs)
    
    def _request_website(self, method, path, signed=False, **kwargs):
        uri = self._create_website_uri(pth)
        return self._reqeust(method, uri, signed, **kwargs)
    
    def _handle_response(self):
        """internal helper for handing API responses from the Bitrue server.
        Rasises the appropriate exceptions when necessary; otherwise, returns the response
        """
        if not (200 <= self.response.status_code < 300):
            raise BitrueAPIException(self.response)
        
        try:
            return self.response.json()
        except ValueError:
            raise BitrueRequestException('Invalid Response: %s' %(self.response.text,))

    def _get(self, path, signed=False, version=PUBLIC_API_VERSION, **kwargs):
        return self._request_api('get', path, signed, version, **kwargs)
    
    def _post(self, path, signed=False, version=PUBLIC_API_VERSION, **kwargs):
        return self._request_api('post', path, signed, version, **kwargs)
    
    def _put(self, path, signed=False, version=PUBLIC_API_VERSION, **kwargs):
        return self._request_api('put', path, signed, version, **kwargs)
    
    def _delete(self, path, signed=False, version=PUBLIC_API_VERSION, **kwargs):
        return self._request_api('delete', path, signed, version, **kwargs)
    
    # exchange endpoints
    def get_server_time(self):
        return self._get('time')
    
    def ping(self):
        return self._get('ping')

    def get_exchange_info(self):
        return self._get('exchangeInfo')
    
    def get_symbol_info(self, symbol):
        res = self.get_exchange_info()

        for item in res['symbols']:
            if item['symbol'] == symbol.upper():
                return item
        return None
    
    def get_all_tickers(self):
        """24 hour price change statistics. Careful when accessing this with no symbol.

            Weight: 1 for a single symbol; 40 when the symbol parameter is omitted
        Returns:
            [type]: [description]
        """
        return self._get('ticker/24hr')
    
    def get_ticker(self, **params):
        return self._get('ticker/price', data=params)
    
    def get_orderbook_ticker(self, **params):
        return self._get("ticker/bookTicker", data=params)
    
    def get_order_book(self, **params):
        return self._get("depth", data=params)
    
    def get_recent_trades(self, **params):
        return self._get('trades', data=params)

    def get_historical_trades(self, **params):
        return self._get('historicalTrades', data=params)
    
    def get_aggregate_trades(self, **params):
        return self._get('aggTrades', data=params)
    
    # Account Endpoints

    def create_order(self, **params):
        return self._post('order', True, data=params)

    def order_limit(self, timeInForce=TIME_IN_FORCE_GTC, **params):
        params.update({
            'type': self.ORDER_TYPE_LIMIT,
            'timeInForce': timeInForce
        })
        return self.create_order(**params)
    
    def order_limit_buy(self, timeInForce=TIME_IN_FORCE_GTC, **params):
        params.update({
            'side': self.SIDE_BUY,
        })
        return self.order_limit(timeInForce=timeInForce, **params)
    
    def order_limit_sell(self, timeInForce=TIME_IN_FORCE_GTC, **params):
        """Send in a new limit sell order
        """
        params.update({
            'side': self.SIDE_SELL
        })
        return self.order_limit(timeInForce=timeInForce, **params)
    
    def order_market(self, **params):
        """Send in a new market order
        """
        params.update({
            'type': self.ORDER_TYPE_MARKET
        })
        return self.create_order(**params)
    
    def order_market_buy(self, **params):
        """Send in a new market buy order
        """
        params.update({
            'side': self.SIDE_BUY
        })
        return self.order_market(**params)
    
    def order_market_sell(self, **params):
        """Send in a new market sell order
        """
        params.update({
            'side': self.SIDE_SELL
        })
        return self.order_market(**params)
    
    def get_order(self, **params):
        """Check an order's status. Either orderId or origClientOrderId must be sent.
        """
        return self._get('order', True, data=params)
    
    def get_all_orders(self, **params):
        """Get all account orders; active, canceled, or filled.
        """
        return self._get('allOrders', True, data=params)
    
    def cancel_order(self, **params):
        """Cancel an active order. Either orderId or origClientOrderId must be sent.
        """
        return self._delete('order', True, data=params)
    
    def get_open_orders(self, **params):
        """Get all open orders on a symbol.
        """
        # data = self.extend_timestamp(params)
        # print(data)
        # params['timestamp'] = int(time.time() * 1000 + self.timestamp_offset)
        return self._get('openOrders', True, data=params)
    
    def get_account(self, **params):
        """Get current account information.
        """
        return self._get('account', True, data=params)
    
    def get_asset_balance(self, asset, **params):
        """Get current asset balance.
        """
        res = self.get_account(**params)
        # find asset balance in list of balances
        if "balances" in res:
            for bal in res['balances']:
                if bal['asset'].lower() == asset.lower():
                    return bal
        return None
    
    def get_my_trades(self, **params):
        """Get trades for a specific symbol.
        """
        return self._get('myTrades', True, data=params)


