# This is a sample Python script.
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

# This is a sample Python script.
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import datetime
import os
import sys
# from MyStockAnalyze import CommonFunc as CF
import CommonFunc as CF
import StrategyStock as SS

# const value
STAKE = 1500          # volume once
START_CASH = 150000   # initial cost
COMM_VALUE = 0.002    # 费率
MACD_PERIOD = 5       # MACD计算周期
FILEDIR = 'stocks'
CODES_FILE = 'text.txt'  # 股票代号文件;
LOG_FILE = '../log.csv'
# globle value
stock_pnl = []  # Net profit
stock_list = []  # stock list
special_info = []  # 特别注意的事项


def main(*args):
    if len(args) < 1:
        print(" Usage: python main.py <arg1>")
        return
    codes_file = args[0]
    return codes_file


if __name__ == '__main__':

    #codefile = main(sys.argv[1])  # 其中 sys.argv[0] 是脚本的名称，其余的元素是传递给脚本的命令行参数。main 函数接收 sys.argv 作为参数，然后从中提取并使用这些参数。
    codefile = "text.txt"
    cf = CF.StockFile(codefile)
    startdate = cf.getStartdate() # 回测开始时间 date type："20211227"
    enddate = cf.getEnddate()     # 回测结束时间 date type："20211227"
    stake = cf.getStake()         # STAKE ："1500"
    start_cash = cf.getStart_cash()
    comm_value = cf.getComm_value()
    macd_period = cf.getMacd_period()  # MACD计算周期
    log_file = cf.getLog_file()   # '../log.csv'

    CF.loginit(log_file, "w")
    today = datetime.datetime.today()
    output_file = str(CF.get_file(today.strftime("%Y-%m-%d")))
    # 使用 'w' 模式打开文件，如果文件不存在则创建它，如果文件已存在则覆盖它
    CF.log2file(output_file, "w", "Test Date: " + today.strftime("%Y-%m-%d"))
    txt = "special ops raw file: file path=" + output_file
    CF.log(log_file, txt)

    it = 0
    code = ""
    Special_ops = []
    result_list = []
    pnl = 0
    stock_name = ""
    stock_info = {}
    strategy = []

    # 第一段：买卖回测模拟
    codes = cf.getCodes()
    for code in codes:
        if it > 0:
            code = str(codes[it]).replace('\n', '')  # "sz300598"
            code_value = CF.get_sh_stock(code)
            stock_name = code_value.value[1]
            print(f"\n{code_value}")
            my_stock = CF.MyStock(stake, start_cash, comm_value, code)
            filepath = CF.prepare_data(code, startdate, enddate)
            file_size = os.path.getsize(filepath)
            if file_size > 0:
                txt = code_value
                CF.log(log_file, txt)
                stock_list.append(stock_name)
                CF.log2file(output_file, "a", code)
                pnl = SS.run_strategy(startdate, enddate, my_stock)
                if pnl is None:
                    pnl = 0
                stock_pnl.append(pnl)
                stock_info.update({stock_name: pnl})
                (df_macd, Special_ops) = CF.computeMACD(code, startdate, enddate)
                if len(Special_ops) > 0:
                    result_list.append(Special_ops)
                Special_ops2 = CF.trendMACD(code, df_macd, period=macd_period)
                if len(Special_ops2) > 0:
                    result_list.append(Special_ops2)
            else:
                break
        it += 1

    print(f"\nTest Date: {datetime.date.today()}")
    print(f"Initial Fund: {start_cash}\nStake: {stake}\nPeriod：{startdate}~{enddate}")
    txt = "Initial Fund: " + str(start_cash) + "\n" + "Stake: " + str(stake) + "\n" + "Period：" + startdate \
          + "~" + enddate
    CF.log(log_file, txt)

    # 第二段：优选股
    rank_info = SS.stock_rank(stock_info)  # 列出优选对象
    for txt in rank_info:
        CF.log(log_file, txt)

    # 第三段：特殊操作提醒 SMA均线长短期交叉买卖提示
    # MACD均线 操作提醒
    print("MACD info Begin:")
    CF.log(log_file, "MACD info Begin:")
    it5 = 0
    new_enddate = datetime.datetime.strptime(enddate, "%Y%m%d")
    new_enddate_str = new_enddate.strftime("%Y-%m-%d")
    if new_enddate > today:
        new_enddate_str = today.strftime("%Y-%m-%d")
    b4_s_date = CF.get_business_day(new_enddate_str, days=-1)
    while it5 < len(result_list) & len(result_list) > 0:
        tmp_result = result_list[it5]
        it6 = 0
        while it6 < len(tmp_result):
            c_code = CF.getcodebytype(tmp_result[it6]['code'], "")
            my_stock = CF.MyStock(stake, start_cash, comm_value, c_code)
            c_date = tmp_result[it6]['date']
            c_msg = tmp_result[it6]['msg']
            if c_date == b4_s_date:
                code_value = CF.get_sh_stock(c_code)  # code include sh
                code_name = code_value.values[1][1]
                print(f"{code_name} : {c_msg}")
                CF.log(log_file, code_name + ":" + c_msg)
            it6 += 1
        it5 += 1
    print("MACD info End!\n")
    CF.log(log_file, "MACD info End!\n")

    # set special operation in the special date
    CF.log(log_file, "[Specal ops Begin]:")
    it2 = 0
    code = ""
    code_name = ""
    if len(special_info) == 0:
        filename = CF.get_file(today.strftime("%Y-%m-%d"))
        cf = CF.StockFile(filename)
        strategy = cf.get_context()
        tmp_str = ""
        for ops in strategy:
            if ops.find("Special_info:", 0) != -1:
                code = tmp_str
                special_info.append({'code': code, 'info': ops})
            else:
                tmp_str = ops
    while it2 < len(special_info):
        tmp_str = special_info[it2]['code']
        if tmp_str is not None:
            code = CF.getcodebytype(tmp_str.replace('\n', ''), ctype='Numeral')
        else:
            print("special_info is None!")
            break
        s_code = CF.getcodebytype(code, ctype='String')
        code_value = CF.get_sh_stock(s_code)   # code include sh
        code_name = code_value.values[1][1]
        my_stock = CF.MyStock(STAKE, START_CASH, COMM_VALUE, code)
        print(f"\nSpecal opt is :{code_name}")
        print(f"{special_info[it2]['info']}")
        operation = special_info[it2]['info']
        CF.log(log_file, "Specal opt :" + code_name)
        CF.log(log_file, operation)
        ind_begin = operation.find("date")
        s_date = operation[ind_begin+5:ind_begin+15]
        b4_s_date = CF.get_business_day(s_date, days=-1)
        b4_s_date_info = my_stock.get_stock_by_date(b4_s_date)
        if b4_s_date_info is None:
            print("b4 date is close!")
        else:
            print(f"{b4_s_date}, open: {b4_s_date_info['open']}, close: {b4_s_date_info['close']}, "
              f"high: {b4_s_date_info['high']}, low: {b4_s_date_info['low']}")
            txt = b4_s_date + ", open: " + str(b4_s_date_info['open']) + ", close: " + str(b4_s_date_info['close']) \
                + ", high: " + str(b4_s_date_info['high']) + ", low: " + str(b4_s_date_info['low'])
            CF.log(log_file, txt)

        # MACD均线 操作提醒
        it3 = 0
        while it3 < len(result_list) & len(result_list) > 0:
            tmp_result = result_list[it3]
            it4 = 0
            while it4 < len(tmp_result):
                c_code = CF.getcodebytype(tmp_result[it4]['code'], "")
                my_stock = CF.MyStock(STAKE, START_CASH, COMM_VALUE, c_code)
                c_date = tmp_result[it4]['date']
                c_msg = tmp_result[it4]['msg']
                b4_s_date = CF.get_business_day(s_date, days=-1)
                b4_s_date_info = my_stock.get_stock_by_date(b4_s_date)
                if c_code == s_code and c_date == s_date:
                    b4_s_date = CF.get_business_day(c_date, days=-1)
                    b4_s_date_info = my_stock.get_stock_by_date(b4_s_date)
                    print(f"{c_date}, {c_msg}")
                    txt = c_date + ", " + c_msg
                    CF.log(log_file, txt)
                it4 += 1
            it3 += 1
        # MACD均线 操作提醒结束

        filepath = CF.get_file(s_code)  # code needs sh or sz
        # 第四段：特殊操作关注的均值，极值提示
        df, close_mean, info = CF.get_consider(filepath)
        for txt in info:
            CF.log(log_file, txt)
        it2 += 1
        CF.log(log_file, "[Specal ops End]")
