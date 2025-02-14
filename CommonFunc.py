#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-

import datetime
import os
import akshare as ak
import pandas as pd
import numpy as np
import baostock as bs
import talib as ta
#from tqsdk import tafunc as tqa  # 替换导入TqSdk的技术分析模块
#from tqsdk import TqApi, TqAuth
#from tqsdk.ta import MACD

# globle value
stock_pnl = []  # Net profit
stock_list = []  # stock list
special_info = []  # 特别注意的事项
special_code = ""


class MyStock:
    # 通用日志打印函数，可以打印下单、交易记录，非必须，可选
    def log(self, txt, dt=None):
        dt = dt or self.date
        print('MyStock：%s , %s' % (dt.isoformat(), txt))

    # 初始化函数，初始化属性、指标的计算，only once time
    def __init__(self, stake, start_cash, comm_value, code):
        self.code = code  # stock code
        self.name = ""  # stock name
        self.date = '1900-01-01'
        self.open = 0.0
        self.close = 0.0
        self.high = 0.0
        self.low = 0.0
        self.volume = 0.0
        self.outstanding_share = 0
        self.turnover = 0
        self.stake = stake
        self.comm_value = comm_value
        self.start_cash = start_cash

    def get_commonvalue(self, mystock):
        self.stake = mystock.stake
        self.comm_value = mystock.comm_value
        self.start_cash = mystock.start_cash

    def get_df(self):
        s_code = getcodebytype(self.code, ctype='String')
        file_path = get_file(s_code)
        df = pd.read_csv(file_path, parse_dates=True, index_col='date')
        df.index = pd.to_datetime(df.index, format='%Y-%m-%d', utc=True)
        return df

    def get_stock_by_date(self, s_date):
        code = getcodebytype(self.code, ctype='Numeral')
        sdf = ak.stock_individual_info_em(symbol=code)  # code begin with numeral
        self.name = sdf.values[5][1]
        df = self.get_df()
        i = 0
        stock_info = None
        while i < len(df):
            t_date = df['open'].index[i]
            if datetime.datetime.strftime(t_date, "%Y-%m-%d") == s_date:
                self.date = s_date
                self.open = df['open'].values[i]
                self.close = df['close'].values[i]
                self.high = df['high'].values[i]
                self.low = df['low'].values[i]
                self.volume = df['volume'].values[i]
                self.outstanding_share = df['outstanding_share'].values[i]
                self.turnover = df['turnover'].values[i]
                stock_info = {'date': self.date, 'open': self.open, 'close': self.close, 'high': self.high,
                              'low': self.low, 'volume': self.volume, 'outstanding_share': self.outstanding_share,
                              'turnover': self.turnover}
                break
            i += 1
        return stock_info


class StockFile:
    # 初始化函数，初始化属性、指标的计算，only once time
    def __init__(self, code_file: str = ""):
        self.codefile = code_file

    def get_stocks(self):
        filename = self.codefile
        '''
        if MyStrategy.params.pslow == 20:
            filename = "text520.txt"
        elif MyStrategy.params.pslow == 30:
            filename = "text530.txt"
        '''
        return filename

    def get_context(self):
        file = None
        try:
            path = os.path.join(get_work_path(""), self.codefile)
            file = open(path, 'r', encoding='utf-8')
            return file.readlines()
        finally:
            if file is not None:
                file.close()

    def getStartdate(self):
        codes = self.get_context()
        if len(codes) > 2:
            return str(codes[0]).replace('\n', '')  # 回测开始时间 date type："20211227"
        else:
            return ""

    def getEnddate(self):
        codes = self.get_context()
        if len(codes) > 2:
            return str(codes[1]).replace('\n', '')  # 回测开始时间 date type："20211227"
        else:
            return ""

    def getStake(self):
        codes = self.get_context()
        if len(codes) > 8:
            stake_str = str((codes[2]).replace('\n', ''))  # STAKE ："1500"
            return int(stake_str[stake_str.find('=') + 1:])
        else:
            return 0

    def getStart_cash(self):
        if len(self.get_context()) > 8:
            start_cash_str = str((self.get_context())[3]).replace('\n', '')
            return round(int(start_cash_str[start_cash_str.find('=') + 1:]), 2)
        else:
            return 0

    def getComm_value(self):
        if len(self.get_context()) > 8:
            comm_value_str = str((self.get_context())[4]).replace('\n', '')
            return round(float(comm_value_str[comm_value_str.find('=') + 1:]), 3)
        else:
            return 0

    def getMacd_period(self):
        if len(self.get_context()) > 8:
            macd_period_str = str((self.get_context())[5]).replace('\n', '')
            return int(macd_period_str[macd_period_str.find('=') + 1:])  # MACD计算周期
        else:
            return 0

    def getLog_file(self):
        if len(self.get_context()) > 8:
            log_file_str = str((self.get_context())[6]).replace('\n', '')  # '../log.csv'
            return log_file_str[log_file_str.find('=') + 1:]
        else:
            return ""

    def getPackname(self):
        if len(self.get_context()) > 8:
            pack_name = str((self.get_context())[7]).replace('\n', '')  # 'stocks'
            return pack_name[pack_name.find('=') + 1:]
        else:
            return ""

    def getCodes(self):
        codes = self.get_context()
        if len(codes) > 8:
            del codes[0:8]
            return codes


def get_work_path(pack_name):
    return os.path.join(os.getcwd(), pack_name)


def get_file(f_code, filemode=None):
    if filemode is not None and filemode == 'MACD':
        stock_file = os.path.join(get_work_path('stocks'), f'{f_code}_k.csv')
    else:
        stock_file = os.path.join(get_work_path('stocks'), f'{f_code}.csv')
    return stock_file


# This is to get the stock code style
# function: getcodebytype
# input:
#     code(String)
#     ctype(String):ctype='Numeral', means return string begin with Numeral, such as 600202;
#                   ctype='String' , means return string with character, such as sh600202;
#                   ctype=None, means return string with character, such as sh600202
# return: stock code (string)
def getcodebytype(code, ctype: str = 'Numeral'):
    s_code = code
    num_code_type = False
    if len(code) == 6:
        num_code_type = True

    if ctype is None:
        if num_code_type:
            if "6" == code[:1]:
                s_code = "sh" + code
            else:
                s_code = "sz" + code
    elif ctype == 'Numeral':
        if num_code_type:
            return s_code
        else:
            return s_code[2:]
    elif ctype == 'SpecialString':
        if num_code_type:
            if "6" == code[:1]:
                s_code = "sh." + code
            else:
                s_code = "sz." + code
        else:
            if s_code.find('.') < 0:
                s_code = s_code[:2] + '.' + s_code[2:]
    else:
        if num_code_type:
            if "6" == code[:1]:
                s_code = "sh" + code
            else:
                s_code = "sz" + code
        elif len(s_code) > 8:
            s_code = s_code[:2] + s_code[3:]
    return s_code


# calculate the business date before one date, if the date is weekend, the get the nearest business date before
# days=-1: yesterday, days=-2: the day before yesterday
# return date string type
def get_business_day(s_date, days=-1):
    d_days = 0
    week_days = int(datetime.datetime.strptime(s_date, "%Y-%m-%d").weekday())
    if days is not None:
        d_days = days
    if week_days == 0:
        d_days = d_days - 2
        before_date = (datetime.datetime.strptime(s_date, "%Y-%m-%d") +
                       datetime.timedelta(d_days)).strftime("%Y-%m-%d")
    elif week_days == 6:
        d_days = d_days - 1
        before_date = (datetime.datetime.strptime(s_date, "%Y-%m-%d") +
                       datetime.timedelta(d_days)).strftime("%Y-%m-%d")
    else:
        before_date = (datetime.datetime.strptime(s_date, "%Y-%m-%d") +
                       datetime.timedelta(d_days)).strftime("%Y-%m-%d")
    return before_date


def get_sh_stock(s_code):   # stock code must be begin with numeral, s_code=600602
    code = getcodebytype(s_code, ctype='Numeral')
    df = ak.stock_individual_info_em(symbol=code)
    return df


# 准备历史数据做预测评估
def prepare_data(f_code, f_startdate, f_enddate):
    csv_file = str(get_file(f_code))
    print("\nprepare_data: file path=" + csv_file)
    get_sh_stock(f_code)  # 去掉代码前缀 sh or sz
    file = open(csv_file, 'w', encoding='utf-8')
    # 默认返回不复权的数据; qfq: 返回前复权后的数据; hfq: 返回后复权后的数据; hfq-factor: 返回后复权因子; qfq-factor: 返回前复权因子
    stock_hfq_df = ak.stock_zh_a_daily(symbol=f_code, start_date=f_startdate, end_date=f_enddate,
                                       adjust="qfq")  # 接口参数格式 股票代码必须含有sh或sz的前缀
    if stock_hfq_df is None:
        print("Warning, run_strategy: stock_hfq_df is None!")
    else:
        stock_hfq_df.to_csv(file, encoding='utf-8')
        file.close()
    return csv_file


def prepare_data_k(f_code, f_startdate, f_enddate):
    csv_file = str(get_file(f_code, 'MACD'))
    print("file path k info: " + csv_file)
    get_sh_stock(f_code)
    file = open(csv_file, 'w', encoding='utf-8')
    login_result = bs.login()
    print(login_result)
    code = getcodebytype(f_code, ctype='SpecialString')
    startdate = datetime.datetime.strftime(pd.to_datetime(f_startdate), "%Y-%m-%d")
    enddate = datetime.datetime.strftime(pd.to_datetime(f_enddate), "%Y-%m-%d")
    rs = None
    if str(login_result).find("fail") > 0:
        print(" no MACD info!")
    else:
        # 获取股票日 K 线数据
        rs = bs.query_history_k_data_plus(code,
                                     "date,code,close,tradeStatus",
                                     start_date=startdate,
                                     end_date=enddate,
                                     frequency='d', adjustflag='3')
        if rs is None:
            print("Warning, run_strategy: stock_hfq_df is None!")
        else:
            rs_df = pd.DataFrame(rs.data, columns=['date', 'code', 'close', 'tradeStatus'])
            rs_df.to_csv(file, encoding='utf-8')
            file.close()
    return rs


# 计算MACD指标参数
def computeMACD(f_code, f_startdate, f_enddate):

    rs = prepare_data_k(f_code, f_startdate, f_enddate)
    # 打印结果集
    result_list = []
    df2 = None
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        result_list.append(rs.get_row_data())
    if len(result_list) > 0:
        df = pd.DataFrame(result_list, columns=rs.fields)
        # 剔除停盘数据
        df2 = df[df['tradeStatus'] == '1']

        # 获取 dif,dea,hist，它们的数据类似是 tuple，且跟 df2 的 date 日期一一对应
        # 记住了 dif,dea,hist 前 33 个为 Nan，所以推荐用于计算的数据量一般为你所求日期之间数据量的 3 倍
   
        # 这里计算的 hist 就是 dif-dea,而很多证券商计算的 MACD=hist*2=(dif-dea)*2
        dif, dea, hist = ta.MACD(df2['close'].astype(float).values, fastperiod=12, slowperiod=26, signalperiod=9)
        #df3 = pd.DataFrame({'dif': dif[33:], 'dea': dea[33:], 'hist': hist[33:]},
        #                   index=df2['date'][33:],
        #                   columns=['dif', 'dea', 'hist']) 

        
        # 检查NaN数量（TqSdk可能产生不同的预热长度）
        # 示例：计算有效数据起始点（假设最大周期为slow_period=26）
        valid_start = 26 + 9 - 1  # 根据MACD算法特性调整
        dif = dif[valid_start:]
        dea = dea[valid_start:]
        hist = hist[valid_start:]
        
    # 调整DataFrame索引切片
    df3 = pd.DataFrame(
        {'dif': dif, 'dea': dea, 'hist': hist},
        index=df2['date'][valid_start:],  # 对齐有效数据区间
        columns=['dif', 'dea', 'hist']
    ) 

    code = getcodebytype(f_code, ctype="")
    df_info = get_sh_stock(code)
    #code_name = df_info.values[5][1]
    code_name = df_info.values[1][1]
    plot_name = 'MACD_' + str(code_name)
    #df3.plot(title=plot_name)

    Special_ops = []
    # 寻找 MACD 金叉和死叉
    datenumber = int(df3.shape[0])
    for i in range(datenumber - 1):
        tmp = df3.iloc[i + 1, 0]
        dif = round(float(df3.iloc[i + 1, 0]), 2)
        dea = round(float(df3.iloc[i + 1, 1]), 2)
        hist = round(float(df3.iloc[i + 1, 2]), 2)
        txt = "MACD date：" + df3.index[i + 1] + ";  dif:" \
              + str(dif) + ";  dea: " + str(dea) + "; hist:" + str(hist)
        print(txt)
        if ((df3.iloc[i, 0] <= df3.iloc[i, 1]) & (df3.iloc[i + 1, 0] >= df3.iloc[i + 1, 1])):
            txt = "MACD golden cross date：" + df3.index[i + 1] + ";  dif:" \
              + str(dif) + ";  dea: " + str(dea) + "; hist:" + str(hist)
            Special_ops.append({'code': f_code, 'date': df3.index[i + 1], 'msg': txt})
            print("MACD 金叉的日期：" + df3.index[i + 1])
        if ((df3.iloc[i, 0] >= df3.iloc[i, 1]) & (df3.iloc[i + 1, 0] <=df3.iloc[i + 1, 1])):
            print("MACD 死叉的日期：" + df3.index[i + 1])
            txt = "MACD dead cross date：" + df3.index[i + 1] + ";  dif:" \
              + str(dif) + ";  dea: " + str(dea) + "; hist:" + str(hist)
            Special_ops.append({'code': f_code, 'date': df3.index[i + 1], 'msg': txt})
    bs.logout()
    return (df3, Special_ops)


# 计算MACD指标趋势
def trendMACD(code, df, period=3):
    str_period = str(period)
    datanumber = int(df.shape[0])
    Special_ops = []
    for i in range(datanumber - period):
        txt = ""
        dif = round(float(df.iloc[i + period, 0]), 2)
        dea = round(float(df.iloc[i + period, 1]), 2)
        hist = round(float(df.iloc[i + period, 2]), 2)
        delta_dif = dif - df.iloc[i, 0]
        delta_dea = dea - df.iloc[i, 1]
        delta_hist = hist - df.iloc[i, 2]
        if (hist > 0) & (dif < 0) & (delta_hist > 0):
            txt = "MACD " + str_period + "日趋势分析：0轴下，上升趋势：" + df.index[i + period] + ";  dif:" \
              + str(dif) + ";  dea: " + str(dea) + "; hist:" + str(hist)
        elif (hist > 0) & (dif > 0) & (delta_hist > 0):
            txt = "MACD " + str_period + "日趋势分析：0轴上，上升趋势：" + df.index[i + period] + ";  dif:" \
                  + str(dif) + ";  dea: " + str(dea) + "; hist:" + str(hist)
        elif (hist < 0) & (delta_hist < 0) & (dea > 0):
            txt = "MACD " + str_period + "日趋势分析：0轴上，下降趋势：" + df.index[i + period] + ";  dif:" \
              + str(dif) + ";  dea: " + str(dea) + "; hist:" + str(hist)
        elif (hist < 0) & (delta_hist < 0) & (dea < 0):
            txt = "MACD " + str_period + "日趋势分析：0轴下，下降趋势：" + df.index[i + period] + ";  dif:" \
              + str(dif) + ";  dea: " + str(dea) + "; hist:" + str(hist)
        if len(txt) > 0:
            Special_ops.append({'code': code, 'date': df.index[i + period], 'msg': txt})
    return Special_ops


# get the stock information from csv file, then to calculate the stock's min, max, std and mean value
def get_consider(f_filepath):
    file_path = f_filepath
    sdf = pd.read_csv(file_path, parse_dates=True, index_col='date')
    sdf.index = pd.to_datetime(sdf.index, format="%Y-%m-%d", utc=True)

    info = []
    max_list = sdf['high'].values.tolist()
    close_list = sdf['close'].values.tolist()
    min_list = sdf['low'].values.tolist()

    max_median = np.median(max_list)
    max_value = np.max(max_list)
    max_std = np.std(max_list)
    max_mean = np.mean(max_list)
    min_median = np.median(min_list)
    min_value = np.min(min_list)
    min_std = np.std(min_list)
    min_mean = np.mean(min_list)
    close_median = np.median(close_list)
    close_max = np.max(close_list)
    close_min = np.min(close_list)
    close_std = np.std(close_list)
    close_mean = np.mean(close_list)
    print('Max median value: {:.2f}, max value: {:.2f}, max std: {:.2f}, max mean: {:.2f}'.format(max_median,
                                                                                                  max_value,
                                                                                                  max_std,
                                                                                                  max_mean))
    print('Min median value: {:.2f}, min value: {:.2f}, min std: {:.2f}, min mean: {:.2f}'.format(min_median,
                                                                                                  min_value,
                                                                                                  min_std,
                                                                                                  min_mean))
    print('Close median value: {:.2f}, Close value: {:.2f}, Close std: {:.2f}, Close mean: {:.2f}'.format(close_median,
                                                                                                          close_max,
                                                                                                          close_min,
                                                                                                          close_std,
                                                                                                          close_mean))
    txt1 = 'Max median value: {:.2f}, max value: {:.2f}, max std: {:.2f}, max mean: {:.2f}'.format(max_median,
                                                                                                  max_value,
                                                                                                  max_std,
                                                                                                  max_mean)
    txt2 = 'Min median value: {:.2f}, min value: {:.2f}, min std: {:.2f}, min mean: {:.2f}'.format(min_median,
                                                                                                  min_value,
                                                                                                  min_std,
                                                                                                  min_mean)
    txt3 = 'Close median value: {:.2f}, Close value: {:.2f}, Close std: {:.2f}, Close mean: {:.2f}'.format(close_median,
                                                                                                          close_max,
                                                                                                          close_min,
                                                                                                          close_std,
                                                                                                          close_mean)
    info.append(txt1)
    info.append(txt2)
    info.append(txt3)
    return sdf, close_mean, info


# 通用日志打印函数，可以打印下单、交易记录，非必须，可选
def log(filename, txt):
    # 使用 'a' 模式打开文件，如果文件已存在追加记录
    log2file(filename, "a", txt)


def loginit(filename, write_mode: str = "w"):
    today = datetime.datetime.today()
    # 使用 'w' 模式打开文件，如果文件不存在则创建它，如果文件已存在则覆盖它
    log2file(filename, write_mode, "Test Date: " + today.strftime("%Y-%m-%d"))


def log2file(
        filename: str = "2024-05-09",
        writemode: str = "w",
        txt: str = "",
):
    # 使用 'w' 模式打开文件，如果文件不存在则创建它，如果文件已存在则覆盖它
    with open(filename, writemode, encoding='utf-8') as file:
        file.write(f"{txt}\n")





