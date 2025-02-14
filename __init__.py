# __init__.py

try:
    import datetime
    import os
    import sys
    import numpy
    import akshare
    import pandas
    import baostock
    import talib
    import backtrader

    FILEDIR = 'stocks'

    # 你的初始化代码
except ImportError as e:
    # 打印错误信息，或者设置一个标志，提示用户缺少依赖
    print(f"Missing required dependency: {e.name}")