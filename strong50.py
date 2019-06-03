#!/usr/bin/env python
#-*- coding: utf8 -*-
import sys, argparse, datetime as dt, numpy, pandas, math
import sqlite3, pandas.io.sql as pd_sql
from turtle import index, data, utils
from turtle.data import StockDataSource, StockData_Tushare
# from dateutil.relativedelta import relativedelta


class IndexConts(data.StockData_SQLite):

    def __init__(self, data_db):
        data.StockData_SQLite.__init__(self, data_db)

    def load_data(self, code, sttdate, enddate):
        down_data = self.download_tushare(code, sttdate, enddate, 'stock', 'daily')
        if len(down_data):
            self.write_stock(down_data, 'daily')
        down_data = self.download_tushare(code, sttdate, enddate, 'stock', 'weekly')
        if len(down_data):
            self.write_stock(down_data, 'weekly')
        down_data = self.download_tushare(code, sttdate, enddate, 'stock', 'monthly')
        if len(down_data):
            self.write_stock(down_data, 'monthly')

    def load_const(self, _index_code, stock_count = 300):
        # sdsr = data.StockData_SQLite(data_db)
        y, m = 2005, 7
        sttdate, enddate = '20000101', '20190601'
        count, stock_codes = 0, []

        for i in range(0, 56):
            _end = dt.datetime(y, m, 1)
            _stt  = _end + dt.timedelta(days=-30)
            _stt = StockDataSource.str_date(_stt)
            _end = StockDataSource.str_date(_end)

            m += 3
            if m > 12:
                y, m = y+1, 1

            df = StockData_Tushare.ts_api.index_weight(index_code=_index_code, 
                start_date = _stt, end_date = _end)
            if len(df) < stock_count:
                continue
            consts = df.head(stock_count)

            for index, row in consts.iterrows():
                code = row['con_code']
                self.read_stock(row['con_code'], sttdate, enddate)
                if len(self.stocks):
                    continue
                self.load_data(row['con_code'], sttdate, enddate)

                stock_codes.append(code)
                count += 1
                if count%20 == 0:
                    print( stock_codes )
                    stock_codes = []

        if len(stock_codes):
            print( stock_codes )

            # if len(df) >= stock_count:
            #     count = self.load_data(df.head(stock_count))
            #     print( 'load %d stocks\' data on %s.'%(count, _end) )


class StrongIndex:
    '强势股指数'
    
    def __init__(self, index_db, _cycle_month = [36, 12]):
        self.cycle_month = _cycle_month

        self.conn = sqlite3.connect(index_db)
        self.curs = self.conn.cursor()

        self.curs.execute('''
            CREATE TABLE IF NOT EXISTS strong_indexs
            (
                ts_code char(9), 
                trade_date int, 
                close float,
                chg1 float,
                k1 float,
                b1 float,
                chg2 float,
                k2 float,
                b2 float,
                primary key (ts_code asc, trade_date asc)
            );''' 
        )
        self.curs.execute('create index IF NOT EXISTS strong_chg1_desc \
                on strong_indexs (trade_date asc, chg1 desc);')
        self.curs.execute('create index IF NOT EXISTS strong_chg2_desc \
                on strong_indexs (trade_date asc, chg2 desc);')
        self.curs.execute('create index IF NOT EXISTS strong_k1_desc \
                on strong_indexs (trade_date asc, k1 desc);')
        self.curs.execute('create index IF NOT EXISTS strong_k2_desc \
                on strong_indexs (trade_date asc, k2 desc);')
        self.conn.commit()

    def __del__(self):
        self.conn.commit()
        self.curs.close()
        self.conn.close()

    
    def calc_index(self, data_db, _index_code, stock_count = 300):
        sdsr = data.StockData_SQLite( data_db )
        y, m, count = 2005, 7, 0
        sttdate, enddate = '20000101', '20190601'
        delta1, delta2 = (self.cycle_month[0]/12.)*365., (self.cycle_month[1]/12.)*365.

        for i in range(0, 56):
            _end = dt.datetime(y, m, 1)
            _stt  = _end + dt.timedelta(days=-30)
            _stt = StockDataSource.str_date(_stt)
            _end = StockDataSource.str_date(_end)

            m += 3
            if m > 12:
                y, m = y+1, 1

            df = StockData_Tushare.ts_api.index_weight(index_code=_index_code, 
                start_date = _stt, end_date = _end)
            if len(df) < stock_count:
                continue

            consts = df.head(stock_count)
            for index, row in consts.iterrows():
                code = row['con_code']
                _sql = 'select ts_code, count(*) as rows from strong_indexs where ts_code = "%s";'%code
                _data_row = pd_sql.read_sql(_sql, self.conn)
                if _data_row.iloc[0, 1] > 0:
                    continue

                sdsr.read_stock(code, sttdate, enddate)
                _len = len(sdsr.stocks)
                if not _len:
                    continue

                data_list, _dates = sdsr.parse_price()
                data_vect = numpy.transpose(data_list)
                data_vect[4] = list(map(lambda x: math.log(x), data_vect[4]))

                chgs1, ks1, bs1 = self.calc_index(data_vect[0], data_vect[4], delta1)
                chgs2, ks2, bs2 = self.calc_index(data_vect[0], data_vect[4], delta2)

                _sql = 'INSERT INTO strong_indexs (ts_code, trade_date, close, chg1, k1, b1, chg2, k2, b2)\
                        VALUES (?,?,?,?,?,?,?,?,?);'
                _codes = [code for _ in range(_len)]
                _vect = [_codes, _dates, data_vect[4], chgs1, ks1, bs1, chgs2, ks2, bs2]
                self.curs.executemany( _sql, numpy.transpose( _vect ) )

            # count = self.calc(df.head(30), sdsr)
            print( 'calc %d stocks\' index on %s.'%(count, _end) )

    def calc_strong_index(self, dates, logdata, daydetla = 300):
        # logdata = list(map(lambda x: math.log(x), data_vect[4]))
        il, ir, _len = 0, 0, len(logdata)
        chgsi, ks, bs = logdata, [0 for _ in range(_len)], [0 for _ in range(_len)]
        daydetla -= 10

        while ir < _len:
            while il < ir and dates[ir] - dates[il] > daydetla:
                il += 1
            chgsi[ir] = logdata[ir] - logdata[il]
            ks[ir],bs[ir],r = index.fit_line(dates[il:ir], logdata[il:ir])
            ir += 1
        return chgsi, ks, bs


if __name__ == "__main__":
    files = [':memory:', ':memory:']
    parser = argparse.ArgumentParser(description="show example")

    parser.add_argument("-f", "--files", help="stock data path", nargs='*')
    args = parser.parse_args()

    if args.files:
        for i in range(0, len(args.files)):
            files[i] = args.files[i]

    si = StrongIndex(files[1])
    si.load_const(files[0], '399300.sz', 300)
    # si.calc_index(files[0], '399300.sz', 300)



    # def load_data(self, consts, sdsr):
    #     sttdate, enddate = '20000101', '20190601'
    #     count, stock_codes = 0, []

    #     for index, row in consts.iterrows():
    #         code = row['con_code']
    #         sdsr.read_stock(code, sttdate, enddate)
    #         if len(sdsr.stocks):
    #             continue

    #         down_data = sdsr.download_tushare(code, sttdate, enddate, 'stock', 'daily')
    #         if len(down_data):
    #             sdsr.write_stock(down_data, 'daily')
    #         down_data = sdsr.download_tushare(code, sttdate, enddate, 'stock', 'weekly')
    #         if len(down_data):
    #             sdsr.write_stock(down_data, 'weekly')
    #         down_data = sdsr.download_tushare(code, sttdate, enddate, 'stock', 'monthly')
    #         if len(down_data):
    #             sdsr.write_stock(down_data, 'monthly')

    #         stock_codes.append(code)
    #         count += 1
    #         if count%20 == 0:
    #             print( stock_codes )
    #             stock_codes = []

    #     if len(stock_codes):
    #         print( stock_codes )
    #     return count

    # def load_const(self, data_db, _index_code, stock_count = 300):
    #     sdsr = data.StockData_SQLite(data_db)
    #     y, m = 2005, 7

    #     for i in range(0, 56):
    #         _end = dt.datetime(y, m, 1)
    #         _stt  = _end + dt.timedelta(days=-30)
    #         _stt = StockDataSource.str_date(_stt)
    #         _end = StockDataSource.str_date(_end)

    #         df = StockData_Tushare.ts_api.index_weight(index_code=_index_code, 
    #             start_date = _stt, end_date = _end)
    #         if len(df) >= stock_count:
    #             count = self.load_data(df.head(stock_count), sdsr)
    #             print( 'load %d stocks\' data on %s.'%(count, _end) )

    #         m += 3
    #         if m > 12:
    #             y, m = y+1, 1


# class StockSlope:
#     '股价的斜率'

#     def __init__(self, _cycle_month = 36):
#         self.ds, self.ks, self.bs = [], [], []

    # def calc_group(self, consts, sdsr):
    #     sttdate, enddate = '20000101', '20190601'
    #     count, detla1, delta2 = 0, (self.cycle_month[0]/12.)*365., (self.cycle_month[1]/12.)*365.

    #     for index, row in consts.iterrows():
    #         code = row['con_code']
    #         _sql = 'select ts_code, count(*) as rows from strong_indexs where ts_code = "%s";'%code
    #         _data_row = pd_sql.read_sql(_sql, self.conn)
    #         if _data_row.iloc[0, 1] > 0:
    #             continue

    #         sdsr.read_stock(code, sttdate, enddate)
    #         _len = len(sdsr.stocks)
    #         if not _len:
    #             continue

    #         data_list, _dates = sdsr.parse_price()
    #         data_vect = numpy.transpose(data_list)
    #         data_vect[4] = list(map(lambda x: math.log(x), data_vect[4]))

    #         chgsi1, ks1, bs1 = self.calc_index(data_vect[0], data_vect[4], detla1)
    #         chgsi2, ks2, bs2 = self.calc_index(data_vect[0], data_vect[4], detla2)

    #         _sql = 'INSERT INTO strong_indexs (ts_code, trade_date, close, chg1, k1, b1, chg2, k2, b2)\
    #                  VALUES (?,?,?,?,?,?,?,?,?);'
    #         _codes = [code for _ in range(_len)]
    #         _vect = [_codes, _dates, data_vect[4], chgsi1, ks1, bs1, chgsi2, ks2, bs2]
    #         self.curs.executemany( _sql, numpy.transpose( _vect ) )

    #     return count


    # cycle_month = 42
    # print( (cycle_month/12.)*365. )

    # strong_index = [0]
    # strong_index.append(0)
    # print( strong_index )

    # sdsr = data.StockData_SQLite( files[0] )
    # sdsr.load("600519.sh", '20150101', '20190601', 'stock', 'weekly')
    # data_list, _dates = sdsr.parse_price()
    # _data_vec = numpy.transpose( data_list )
    # print( sdsr.stocks )
    # print( _data_vec[0] )
    # print( _data_vec[1] )
    # print( _data_vec[4] )


            # data_vect[4] = list(map(lambda x: math.log(4), data_vect[1]))
            # strong_index = data_vect[4] # [0]
            # count, il, ir = count+1, 0, 0

            # while ir < _len:
            #     while data_vect[0][ir] - data_vect[0][il] > daydetla:
            #         il += 1
            #     strong_index[ir] = data_vect[4][ir] - data_vect[4][il]
            #     ir += 1


    # y, m = 2005, 7
    # for i in range(0, 3):
        # _end = dt.datetime(y, m, 1)
    #     _stt  = _end + dt.timedelta(days=-30)
    #     _stt = StockDataSource.str_date(_stt)
    #     _end = StockDataSource.str_date(_end)
        # print( _end )

    #     # df = StockData_Tushare.ts_api.index_weight(index_code='399300.sz', 
    #     #         start_date = _stt, end_date = _end)
    #     # print( _end, len(df) )

        # m += 3
        # if m > 12:
        #     y, m = y+1, 1
            # m = 1
            # y+= 1


    # _end = dt.datetime(2014, 7, 1)
    # _stt = _end - relativedelta(months=14)
    # print( _stt, _end )
    
    # df = StockData_Tushare.ts_api.index_weight(index_code='399300.sz', 
    #             start_date=StockDataSource.str_date(_stt), 
    #             end_date=StockDataSource.str_date(_end))
    # print( df )

            # _list = [codes, _dates, data_vect[4], strong_index]
            # _list = stock_data[[ 'ts_code', 'trade_date', 'open', 'high', 'low', 'close', \
            #         'pre_close', 'change', 'pct_chg', 'vol', 'amount' ]].values.tolist()
            # self.curs.executemany( _sql, _list )
            # _dates, strong_index

            # _stt = StockDataSource.datetime(data_vect[0][0])
            # _thr = _stt + relativedelta(months=self.cycle_month)
            # _thr = StockDataSource.float_date( _thr )
            # il, ir = 0, 0

            # while _data_vec[0][idx] < _thr: 
            #     strong_index[idx] = _data_vec[4][idx] - _data_vec[4][0]
            #     idx += 1

            # while idx < _len:
            #     strong_index[idx] = _data_vec[4][idx] - _data_vec[4][0]
            #     idx += 1

            # for idx in range(0, _len):
            #     if _data_vec[0][idx] > _thr:
            #         break;
            #     # strong_index.append( _data_vec[4] )
            #     strong_index[idx] = _data_vec[4][idx] - _data_vec[4][0]

            # _shift = math.log(_data_vec[4][0])
            # _data_vec[4] = list(map(lambda x: math.log(4) - _shift, _data_vec[1]))

            # for i in range(0, _len):
            # for stock_data in data_list:
            # if len(sdsr.stocks):

        # self.curs.execute('create index IF NOT EXISTS long_date_strong \
        #         on turtle_long_indexs(trade_date asc, strong_index desc);')

        # self.stocks = {}
        # self.dates = [ '20050701', ]

    # def __del__(self):
    #     self.conn.commit()
    #     self.curs.close()
    #     self.conn.close()

            # print( _sql )
            # print( _data_row )
            # print( _data_row.iloc[0, 1] )
            # return 0

                # m = 1
                # y+= 1
