#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
import datetime
import backtrader as bt
import akshare as ak
# from MyStockAnalyze import CommonFunc as CF
import CommonFunc as CF

class MyStrategy(bt.Strategy):
    """
    主策略程序
    """
    params = (
        ('pfast', 5),  # period for the fast moving average
        ('pslow', 10),  # period for the slow moving average
        ('stake', 1500),
        ('start_cash', 150000),
        ('comm_value', 0.02)
    )

    # 初始化函数，初始化属性、指标的计算，only once time
    def __init__(self):
        self.data_close = self.datas[0].close  # close data
        self.date = self.datas[0].datetime.date(0)
        # initial data
        self.order = None
        self.buy_price = None
        self.buy_comm = None  # trial fund
        self.buy_cost = None  # cost
        self.pos = None  # pos
        self.cash_valid = None  # available fund
        self.valued = None  # total fund
        self.pnl = None  # profit
        self.sma1 = bt.ind.SMA(period=self.p.pfast)  # fast moving average
        self.sma2 = bt.ind.SMA(period=self.p.pslow)  # slow moving average
        self.dif = self.sma1 - self.sma2
        self.crossover = bt.ind.CrossOver(self.sma1, self.sma2)  # crossover signal
        self.crossover_buy = False
        self.crossover_sell = False
        self.specialinfo = []  # 特别注意的事项

    # 通用日志打印函数，可以打印下单、交易记录，非必须，可选
    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def is_special(self, special_date, txt):
        operator = ""
        if txt.lower().find('buy') > 0:
            operator = 'BUY'
        elif txt.lower().find('sell') > 0:
            operator = 'SELL'
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        yesterday_spe = CF.get_business_day(today, days=-1)
        if datetime.datetime.strftime(special_date, "%Y-%m-%d") == yesterday_spe:
            self.specialinfo.append({'date': yesterday_spe,
                                     'code': "",
                                     'info': txt,
                                     'operator': operator})
        elif datetime.datetime.strftime(special_date, "%Y-%m-%d") == today:
            self.specialinfo.append({'date': today,
                                     'code': "",
                                     'info': txt,
                                     'operator': operator})

    def get_specialinfo(self):
        return self.specialinfo

    # order statement information
    def notify_order(self, order):
        txt = ""
        if order.status in [order.Submitted, order.Accepted]:
            return
        # 检查订单是否完成
        if order.status in [order.Completed]:
            price = order.executed.price
            comm = order.executed.comm
            cost = order.executed.value
            pos = self.getposition(self.data).size
            fund = self.broker.getvalue()
            if order.isbuy():
                self.crossover_buy = True
                txt = 'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f, fund Value %.2f, pos Size %.2f' % \
                      (price, cost, comm, fund, pos)
                self.log(txt)
            elif order.issell():
                self.crossover_sell = True
                self.pos = self.getposition(self.data).size
                txt = 'SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f, fund Value %.2f, pos Size %.2f' % \
                      (price, cost, comm, fund, pos)
                self.log(txt)
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            if order.status == order.Canceled:
                self.log('order cancel!')
            elif order.status == order.Margin:
                self.log('fund not enough!')
            elif order.status == order.Rejected:
                self.log('order reject!')
        self.order = None
        if self.crossover_buy:
            self.is_special(self.datas[0].datetime.date(0), txt)
        elif self.crossover_sell:
            self.is_special(self.datas[0].datetime.date(0), txt)
        self.crossover_buy = False
        self.crossover_sell = False

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.log('business profit: %.2f, Net profit: %.2f' % (trade.pnl, trade.pnlcomm))

    #  loop in every business day

    def next(self):
        size = self.getposition(self.data).size
        price = self.getposition(self.data).price
        valid_cash = self.broker.getcash()
        fund = self.broker.getvalue()
        buy_comm = price * size * self.p.comm_value
        if not self.position:  # Outside, buy
            if self.crossover > 0:  # if golden cross, valid=datetime.datetime.now() + datetime.timedelta(days=3)
                self.log('Available Cash: %.2f, Total fund: %.2f, pos: %.2f' % (valid_cash, fund, size))
                self.order = self.buy()
                txt = 'Outside, golden cross buy, close: %.2f，Total fund：%.2f, pos: %.0f' % \
                      (self.data_close[0], fund, size)
                self.is_special(self.datas[0].datetime.date(0), txt)
                self.log('Outside, golden cross buy, close: %.2f，Total fund：%.2f, pos: %.2f' %
                         (self.data_close[0], fund, size))
        else:
            if self.crossover > 0:
                if (valid_cash - price * self.p.stake - buy_comm) > 0:
                    self.log('Available Cash: %.2f, Total fund: %.2f, pos: %.2f' % (valid_cash, fund, size))
                    self.order = self.buy()
                    self.is_special(self.datas[0].datetime.date(0),
                                    'Outside, golden cross buy, close: %.2f，Total fund：%.2f， pos: %.2f' %
                                    (self.data_close[0], valid_cash, size))
                    self.log('Outside, golden cross buy, close: %.2f，Total fund：%.2f， pos: %.2f' %
                             (self.data_close[0], valid_cash, size))
            elif self.crossover < 0:  # Inside and dead cross
                if fund > self.p.start_cash * 1.03:
                    self.order = self.close(size=size)
                    self.is_special(self.datas[0].datetime.date(0),
                                    'Inside dead cross, sell, close: %.2f，Total fund：%.2f, pos: %.2f' %
                                    (self.data_close[0], fund, size))
                    self.log('Inside dead cross, sell, close:  %.2f，Total fund：%.2f, pos: %.2f' %
                             (self.data_close[0], fund, size))

    # 策略结束后的清理工作
    def stop(self):
        special_info = self.get_specialinfo()
        if len(special_info) > 0:
            today = datetime.datetime.today()
            output_file = str(CF.get_file(today.strftime("%Y-%m-%d")))
            print("special ops raw file: file path=" + output_file)
            # 打开文件用于追加，如果文件不存在则创建新文件
            with open(output_file, 'a', encoding='utf-8') as file:
                for ops in special_info:
                    txt = "Special_info: date=" + ops['date'] + ", " + ops['info'] + ", " + ops['operator']
                    CF.log2file(output_file, "a", txt)


# sorted by strategy result
def stock_rank(stock_info):
    pnl = 0.0
    rank_info = []
    today = datetime.date.today().strftime("%Y-%m-%d")
    sorted_dict = {k: v for k, v in sorted(stock_info.items(), key=lambda item: item[1], reverse=True)}
    # 依次取出排序后的键值对
    for key, value in sorted_dict.items():
        pnl += value
        print(f"name: {key}, pnl: {round(value,2)}")
        txt = "name: " + str(key) + ", pnl: " + str(round(value,2)) + '\n'
        rank_info.append(txt)
    print(f"截止到{today} total pnl: {round(pnl,2)}\n")
    txt = "截止到" + today + ", total pnl: " + str(round(pnl, 2)) + '\n'
    rank_info.append(txt)
    return rank_info


def market_info(date):
    date_ = str(date) # date type："20211227"
    stock_szse_deal_daily_df = ak.stock_sse_deal_daily(date_)
    stock_sse_deal_df = ak.stock_szse_summary(date_)
    print(stock_szse_deal_daily_df)
    print(stock_sse_deal_df)
    # 涨停股池
    stock_zt_pool_previous_em_df = ak.stock_zt_pool_previous_em(date=date_)
    print(stock_zt_pool_previous_em_df)
    # 跌停股池
    stock_zt_pool_dtgc_em_df = ak.stock_zt_pool_dtgc_em(date=date_)
    print(stock_zt_pool_dtgc_em_df)
    return stock_zt_pool_previous_em_df, stock_zt_pool_dtgc_em_df


# 从指定文件中读取数据，并运行回测函数
def run_strategy(f_startdate, f_enddate, mystock):
    if mystock is None:
        print("mystock info is none!!!")
        return 0
    else:
        code = mystock.code
        if (code is None) or (len(code) == 0):
            print("mystock.code is not valid!! pls check")
            return
        filepath = CF.get_file(code)
        sdf, close_mean, info = CF.get_consider(filepath)
        from_date = datetime.datetime.strptime(f_startdate, "%Y%m%d")
        end_date = datetime.datetime.strptime(f_enddate, "%Y%m%d")
        data = bt.feeds.PandasData(dataname=sdf, fromdate=from_date, todate=end_date)  # 加载数据
        # 创建Cerebro引擎
        cerebro = bt.Cerebro()  # 初始化回测系统
        cerebro.adddata(data)  # 将数据传入回测系统
        cerebro.broker.setcash(mystock.start_cash)  # set initial fund
        cerebro.broker.setcommission(commission=mystock.comm_value)  # set trad rate 0.2%
        stake = mystock.stake
        if close_mean > 90:
            stake = 500
        cerebro.addsizer(bt.sizers.FixedSize, stake=stake)  # set trade volume
        cerebro.addstrategy(MyStrategy)  # period = [(5, 10), (20, 100), (2, 10)]) , 运行策略

        cerebro.run(maxcpus=1)  # 运行回测系统
        port_value = cerebro.broker.getvalue()  # trade over, get total fund
        pnl = port_value - mystock.start_cash  # figure out profit
        print('Begin Fund: %.2f, Total Cash: %.2f' % (mystock.start_cash, round(port_value, 2)))
        print(f"Net Profit: {round(pnl, 2)}")
    return pnl