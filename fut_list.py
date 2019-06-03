#!/usr/bin/env python
#-*- coding: utf8 -*-
import numpy, pandas
from turtle import data, index, trade
from turtle.data import StockDataSource


good_list = [ 
    { # 能源 ine 上海能源中心
        'codes': ['sc.ine'],
        'names': ['原油'],
        'index': ['sc.nh']
    },

    # { # 金融 CFFEX 中金所 CFX
    #     'codes': ['if.cfx', 't.cfx'],
    #     'names': ['沪深３００', '１０年期国债'],
    #     'index': ['399300.SZ', '']
    # },

    { # 金属　SHFE 上期所 SHF 9
        'codes': ['au.shf', 'ag.shf', 'rb.shf', 'zn.shf', 'cu.shf', 'ni.shf', 'al.shf', 'sn.shf', 'pb.shf'],
        'names': ['沪金', '沪银', '螺纹钢', '沪锌', '沪铜', '沪镍', '沪铝', '沪锡', '沪铅'],
        'index': ['au.nh', 'ag.nh', 'rb.nh', 'zn.nh', 'cu.nh', 'ni.nh', 'al.nh', 'sn.nh', 'pb.nh']
    },

    { # 矿产 4
        'codes': ['j.dce', 'i.dce', 'bu.shf', 'zc.zce'],
        'names': ['焦炭', '铁矿', '沥青', '郑煤'],
        'index': ['j.nh', 'i.nh', 'bu.nh', 'zc.nh']
    },

    { # 工业品　 4
        'codes': ['ta.zce', 'v.dce', 'pp.dce', 'l.dce', 'ma.zce', 'eg.dce'],
        'names': ['PTA', 'PVC', 'PP', '塑料', '郑醇', '乙二醇'],
        'index': ['ta.nh', 'v.nh', 'pp.nh', 'l.nh', 'me.nh', 'eg.nh']
    },

    { # 农产品 6
        'codes': ['m.dce', 'c.dce', 'y.dce', 'cf.zce', 'rm.zce', 'sr.dce'],
        'names': ['豆粕', '玉米', '豆油', '棉花', '菜粕', '白糖'],
        'index': ['m.nh', 'c.nh', 'y.nh', 'cf.nh', 'rm.nh', 'sr.nh']
    }
]


class FutList(index.TurtleArgs):
    '列出活跃期货交易品种当前日期的海龟指标'

    def __init__(self, _good_list):
        index.TurtleArgs.__init__(self)
        self.sdsr = data.StockData_SQLite( self.files[0] )
        self.long_list, self.short_list = [], []

        for _row in _good_list:
            for _idx in range(0, len(_row['codes'])):
                self.statis_fut(_row['codes'][_idx], _row['index'][_idx], _row['names'][_idx])

        long_turtle = pandas.DataFrame(self.long_list, columns=[ 'ts_code', 'date', 'state', 'close', 
                'high', 'low', 'rise', 'drop', 'wave', 'ts_name' ])
        short_turtle = pandas.DataFrame(self.short_list, columns=[ 'ts_code', 'date', 'state', 'close', 
                'high', 'low', 'rise', 'drop', 'wave', 'ts_name' ])
        long_turtle.to_csv("fut_turtle.csv")

        # long_turtle = pandas.DataFrame(self.long_list, columns=[ 'ts_code', 'index', 'date', 'close', 
        #         'high', 'low', 'ts_name' ])
        # print( long_turtle )

        print( long_turtle[ long_turtle['state'] > 0 ] )
        print( short_turtle[ short_turtle['state'] > 0 ] )
        print( long_turtle[ long_turtle['rise'] < 5 ] )
        print( short_turtle[ short_turtle['drop'] > -5 ] )


    def statis_fut(self, fut_code, fut_index, fut_name):
        # if len(fut_index):
        #     self.sdsr.load(fut_index, self.dates[0], self.dates[1], 'index')
        # else:
        #     self.sdsr.load(fut_code, self.dates[0], self.dates[1], 'fut')

        self.sdsr.load(fut_code, self.dates[0], self.dates[1], 'fut')
        if not len(self.sdsr.stocks):
            return

        data_list, _dates = self.sdsr.parse_price()
        _data_vec = numpy.transpose( data_list )
        _last = len(_data_vec[0]) - 1
        _close = _data_vec[4][_last]
        _open, _high, _low = _data_vec[1][_last], _data_vec[2][_last], _data_vec[3][_last]

        # self.long_list.append( [fut_code, fut_index, _dates[_last], _close, _high, _low, fut_name] )

        turtle = index.TurtleIndex()
        index_long = index.LongTurtleIndex(turtle, _data_vec, self.turtle_args[2], 
                        self.turtle_args[3], self.turtle_args[0], self.turtle_args[1])

        _price, _wave = index_long['key_price'][_last], index_long['wave'][_last]
        _high, _low = index_long['long'][_last], index_long['short'][_last]

        self.long_list.append( [fut_code, _dates[_last], index_long['state'][_last], _close, _high, _low, \
                float('%.02f'%((_high-_close)*100/_close)), float('%.02f'%((_low-_close)*100/_close)), '%.02f'%_wave, fut_name] )

        turtle = index.TurtleIndex()
        index_short = index.ShortTurtleIndex(turtle, _data_vec, self.turtle_args[2], 
                        self.turtle_args[3], self.turtle_args[0], self.turtle_args[1])
            
        _price, _wave = index_short['key_price'][_last], index_short['wave'][_last]
        # _high, _low = index_short['short'][_last], index_short['long'][_last]
        _high, _low = index_short['long'][_last], index_short['short'][_last]

        self.short_list.append( [fut_code, _dates[_last], index_short['state'][_last], _close, _high, _low, \
                float('%.02f'%((_high-_close)*100/_close)), float('%.02f'%((_low-_close)*100/_close)), '%.02f'%_wave, fut_name] )

    def list_info(self, stock_data, index_data, count = 0):
        list_len = len(stock_data[0])
        starti = 0
        if count:
            starti = list_len - min(count, list_len)
        info_list = []

        for i in range(starti, list_len):
            append_price = index_data['key_price'][i] + index_data['wave'][i]
            stop_price = index_data['key_price'][i] - index_data['wave'][i] * self.turtle_args[0]
            info_list.append( [ StockDataSource.str_date(stock_data[0][i]), stock_data[4][i], 
                    index_data['state'][i], '%.02f'%index_data['key_price'][i], '%.02f'%append_price, 
                    stop_price, index_data['short'][i], '%.02f'%index_data['wave'][i] ] )

        cols = [ 'trade_date', 'close', 'state', 'key_price', 'append', 'stop', 'profit', 'wave' ]
        return pandas.DataFrame( info_list, columns= cols)
        

if __name__ == "__main__":
    # _good_list = [{ 'codes': ['zn.shf'], 'names': ['沪锌'], 'index': ['zn.nh'] }] 
    FutList(good_list)


        # if index_long['state'][_last]:
        #     _append = _price + _wave
        #     _stoploss = _price - _wave*self.turtle_args[0]
        #     _pos_unit = int(1000 / _wave)
        # else:
        #     _append, _stoploss, _pos_unit = 0, 0, 0

        # self.turtle_list.append( [fut_code, _dates[_last], index_long['state'][_last], _price, _append, _stoploss, 
        #             _pos_unit, index_long['short_low'][_last], _wave, _data_vec[4][_last] ])


        # turtle_list = []

        # for code in self.good_codes:
        #     sdsr.load(code, self.dates[0], self.dates[1], self.good_type)
        #     if not len(sdsr.stocks):
        #         continue
        #     data_list, _dates = sdsr.parse_price()
        #     _data_vec = numpy.transpose( data_list )

        #     turtle = index.TurtleIndex()
        #     index_long = index.LongTurtleIndex(turtle, _data_vec, self.turtle_args[2], 
        #                 self.turtle_args[3], self.turtle_args[0], self.turtle_args[1])
        #     turtle = index.TurtleIndex()
        #     index_short = index.LongTurtleIndex(turtle, _data_vec, self.turtle_args[3], 
        #                 self.turtle_args[4], self.turtle_args[0], self.turtle_args[1])
            
        #     long_list = self.list_info(_data_vec, index_long)
        #     long_list.tail(30).to_csv('long.csv')
        #     print( long_list.tail(5) )

        #     short_list = self.list_info(_data_vec, index_short)
        #     short_list.tail(30).to_csv('short.csv')
        #     print( short_list.tail(5) )

        #     _last = len(_data_vec[0]) - 1
        #     _price, _wave = index_long['key_price'][_last], index_long['wave'][_last]

        #     _append = _price + _wave
        #     _stoploss = _price - _wave*self.turtle_args[0]
        #     _pos_unit = int(1000 / _wave)

        #     turtle_list.append( [code, _dates[_last], index_long['state'][_last], _price, _append, _stoploss, 
        #             _pos_unit, index_long['short_low'][_last], _wave, _data_vec[4][_last] ])

    # def list_info(self, stock_data, index_data, count = 0):
    #     list_len = len(stock_data[0])
    #     starti = 0
    #     if count:
    #         starti = list_len - min(count, list_len)
    #     info_list = []

    #     for i in range(starti, list_len):
    #         append_price = index_data['key_price'][i] + index_data['wave'][i]
    #         stop_price = index_data['key_price'][i] - index_data['wave'][i] * self.turtle_args[0]
    #         info_list.append( [ StockDataSource.str_date(stock_data[0][i]), stock_data[4][i], 
    #                 index_data['state'][i], '%.02f'%index_data['key_price'][i], '%.02f'%append_price, 
    #                 stop_price, index_data['short_low'][i], '%.02f'%index_data['wave'][i] ] )

    #     cols = [ 'trade_date', 'close', 'state', 'key_price', 'append', 'stop', 'profit', 'wave' ]
    #     return pandas.DataFrame( info_list, columns= cols)
        
#print( good_list[1]['names'] )
# for _row in good_list:
#     for _idx in range(0, len(_row['codes'])):
#         print( _row['codes'][_idx], _row['names'][_idx] )

    # for _code in _row['codes']:
        # print( _code)
    # print( _row['names'] )


