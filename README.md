在binance API 现货交易查询2023-01-01至2023-03-15 BTC 4小时级别
（一根k线代表4小时）的交易数据，开盘/收盘/最高价/最低价/交易量，并计算
当价格有效涨/跌破MA20 （20根k线柱-4小时一根）的24小时内最大涨跌幅及收盘涨跌幅。

任务：
1，获取该时段内开盘/收盘/最高价/最低价/交易量 五个指标的数据
2，统计时间段内共有多少次涨破或跌破4小时级别MA20，有多少次有效涨破或跌破.(有效涨跌的定义：当价格涨/跌破MA20 后的24 小时内收盘价没有回到MA20 下方/上方)
3，计算每次有效涨/跌破MA20后的最大涨跌幅及收盘涨跌幅

Binance0301_0315.csv是爬虫所得数据，最终的分析输出结果结果保存在output.csv中