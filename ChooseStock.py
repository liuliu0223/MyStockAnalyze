import akshare as ak
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta

# 配置参数
CONFIG = {
    'strategy_params': {
        'ma_periods': [5, 20, 60],
        'rsi_period': 14,
        'macd_params': (12, 26, 9),
        'volume_ratio': 1.2,  # 量比阈值
        'pe_max': 40,  # 市盈率上限
        'pb_max': 5,  # 市净率上限
        'roe_min': 0.15,  # ROE最低要求
        'growth_min': 0.25,  # 净利润增长率
        'market_cap_min': 50  # 最小市值（亿元）
    }
}

class StockSelector:
    def __init__(self):
        self.stock_list = []
        self.selected = []

    def get_all_stocks(self):
        """获取全量股票列表"""
        try:
            # 获取A股所有股票代码
            stock_info = ak.stock_info_a_code_name()
            for _, row in stock_info.iterrows():
                code = row['code']
                name = row['name']
                # 根据股票代码判断市场
                market = 0 if code.startswith(('0', '3')) else 1  # 0: 深圳, 1: 上海
                self.stock_list.append((market, code, name))
            print(f"获取到{len(self.stock_list)}只股票")
        except Exception as e:
            print(f"获取股票列表失败：{str(e)}")

    def get_kline_data(self, market, code, num=100):
        """获取K线数据"""
        try:
            # 获取当前日期
            end_date = datetime.now()
            start_date = end_date - timedelta(days=num*2)  # 多获取一些数据以确保有足够的数据
            
            # 根据市场选择正确的股票代码格式
            if market == 0:  # 深圳
                symbol = f"{code}.SZ"
            else:  # 上海
                symbol = f"{code}.SH"
            
            # 获取日K线数据
            df = ak.stock_zh_a_hist(symbol=symbol, period="daily", 
                                  start_date=start_date.strftime('%Y%m%d'),
                                  end_date=end_date.strftime('%Y%m%d'),
                                  adjust="qfq")
            
            # 重命名列以匹配原有代码
            df = df.rename(columns={
                '日期': 'datetime',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount'
            })
            
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.sort_values('datetime')
            return df.tail(num)  # 只返回请求的数量
        except Exception as e:
            print(f"获取{code}K线失败：{str(e)}")
            return None

    def get_financial_data(self, market, code):
        """获取财务数据"""
        try:
            # 获取实时行情数据
            if market == 0:  # 深圳
                symbol = f"{code}.SZ"
            else:  # 上海
                symbol = f"{code}.SH"
            
            # 获取实时行情
            quote = ak.stock_zh_a_spot_em()
            stock_quote = quote[quote['代码'] == code].iloc[0]
            
            # 获取财务指标
            financial = ak.stock_financial_analysis_indicator(symbol=code)
            latest_financial = financial.iloc[0]
            
            return {
                'pe': float(stock_quote['市盈率']),
                'pb': float(stock_quote['市净率']),
                'roe': float(latest_financial['净资产收益率(%)']) / 100,
                'profit_growth': float(latest_financial['净利润增长率(%)']) / 100,
                'market_cap': float(stock_quote['总市值']) / 100000000  # 转换为亿元
            }
        except Exception as e:
            print(f"获取{code}财务数据失败：{str(e)}")
            return None

    def calculate_technical(self, df):
        """计算技术指标"""
        # 计算均线
        for period in CONFIG['strategy_params']['ma_periods']:
            df[f'MA{period}'] = df['close'].rolling(period).mean()

        # 计算RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))

        # 计算MACD
        exp12 = df['close'].ewm(span=12, adjust=False).mean()
        exp26 = df['close'].ewm(span=26, adjust=False).mean()
        df['DIF'] = exp12 - exp26
        df['DEA'] = df['DIF'].ewm(span=9, adjust=False).mean()
        df['MACD'] = (df['DIF'] - df['DEA']) * 2

        return df.dropna()

    def analyze_stock(self, market, code, name):
        """核心分析逻辑"""
        try:
            # 获取K线数据
            kline = self.get_kline_data(market, code)
            if kline is None or len(kline) < 60:
                return None

            # 技术分析
            df = self.calculate_technical(kline)
            latest = df.iloc[-1]
            prev = df.iloc[-2]

            # 技术条件
            tech_cond = (
                    (latest['MA5'] > latest['MA20']) &
                    (latest['MA20'] > latest['MA60']) &
                    (latest['DIF'] > latest['DEA']) &
                    (prev['DIF'] <= prev['DEA']) &  # MACD金叉
                    (40 < latest['RSI'] < 70) &
                    (latest['volume'] > 1e5))

            # 财务分析（示例数据）
            financial = self.get_financial_data(market, code)
            funda_cond = (
                    (financial['pe'] < CONFIG['strategy_params']['pe_max']) &
                    (financial['pb'] < CONFIG['strategy_params']['pb_max']) &
                    (financial['roe'] > CONFIG['strategy_params']['roe_min']) &
                    (financial['profit_growth'] > CONFIG['strategy_params']['growth_min']) &
                    (financial['market_cap'] > CONFIG['strategy_params']['market_cap_min'])
            )

            if tech_cond and funda_cond:
                return {
                    '市场': '深圳A股' if market == 0 else '上海A股',
                    '代码': code,
                    '名称': name,
                    '价格': latest['close'],
                    '5日均线': latest['MA5'],
                    'MACD': latest['MACD'],
                    'RSI': latest['RSI'],
                    '市盈率': financial['pe'],
                    'ROE': f"{financial['roe'] * 100:.1f}%"
                }
        except Exception as e:
            print(f"分析{code}时出错：{str(e)}")
        return None

    def run_strategy(self):
        """执行选股策略"""
        print("开始执行多因子筛选...")
        for market, code, name in self.stock_list:
            result = self.analyze_stock(market, code, name)
            if result:
                self.selected.append(result)
            time.sleep(0.1)  # 控制请求频率

        if self.selected:
            df = pd.DataFrame(self.selected)
            df.sort_values('RSI', ascending=False, inplace=True)
            print("\n最终选股结果：")
            print(df[['市场', '代码', '名称', '价格', 'RSI', '市盈率']])
        else:
            print("今日未发现符合策略的标的")


def get_stock_data():
    """获取股票数据"""
    try:
        # 获取A股所有股票信息
        stock_info = ak.stock_info_a_code_name()
        print("成功获取股票数据！")
        return stock_info
    except Exception as e:
        print(f"获取数据时发生错误: {str(e)}")
        return None


if __name__ == "__main__":
    # 添加重试机制
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        print(f"第 {retry_count + 1} 次尝试获取数据...")
        df = get_stock_data()
        
        if df is not None:
            print("成功获取数据！")
            print(df.head())
            break
            
        retry_count += 1
        if retry_count < max_retries:
            print(f"等待 5 秒后重试...")
            time.sleep(5)