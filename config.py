# -*- coding: utf-8 -*-
"""
配置文件：定义系统所有参数
对应文档章节：步骤1：明确鱼盆策略核心规则
"""
import os
from datetime import time

# 企业微信机器人Webhook地址
# 文档无此参数，按用户提供配置
WECHAT_WEBHOOK = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=2f594b04-eb03-42b6-ad41-ff148bd57183"

# 资金配置
# 对应文档章节：1. 基础资金与仓位划分
TOTAL_CAPITAL = 20000  # 总资金2万元
STABLE_CAPITAL = 12000  # 稳健仓1.2万元(60%)
AGGRESSIVE_CAPITAL = 8000  # 激进仓0.8万元(40%)
ARBITRAGE_CAPITAL = 2000  # 套利仓0.2万元(10%)

# ETF筛选条件
# 对应文档章节：2. 原有买卖信号与仓位管理 - 标的筛选
MIN_DAILY_TURNOVER = 50000000  # 最小日均成交额5000万
MIN_SIZE = 500000000  # 最小规模5亿
MAX_TRACKING_ERROR = 0.02  # 最大年化跟踪误差2%

# 交易时间配置(北京时间)
# 文档无具体时间，按A股规则补充
TRADING_HOURS = {
    "morning_start": time(9, 30),
    "morning_end": time(11, 30),
    "afternoon_start": time(13, 0),
    "afternoon_end": time(15, 0)
}

# 推送时间配置(北京时间)
# 对应文档项目功能11、12
POOL_PUSH_TIME = time(11, 0)  # 每天早上11点推送股票池
STRATEGY_CHECK_TIME = time(14, 0)  # 下午2点执行策略判断

# ETF池配置
# 对应文档章节：4. ETF池构建与调仓机制、项目功能8
ETF_POOL_SIZE = 10  # 股票池固定10只ETF
POOL_UPDATE_DAY = 4  # 每周五更新(0=周一,4=周五)
POOL_UPDATE_TIME = time(16, 0)  # 周五下午4点强制更新

# 重试与推送间隔
# 对应文档项目功能13、15
RETRY_INTERVAL = 1800  # 失败重试间隔30分钟(秒)
MESSAGE_INTERVAL = 60  # 消息间隔1分钟(秒)

# 数据源优先级
# 对应文档章节：19. 主数据源与备用数据源
DATA_SOURCE_PRIORITY = [
    "akshare", 
    "baostock", 
    "sina", 
    "tushare"
]

# Tushare Token（用户可补充，不填不影响其他数据源）
TUSHARE_TOKEN = ""

# 缓存路径
# 对应文档章节：20. 数据保存要求
CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")
os.makedirs(CACHE_DIR, exist_ok=True)  # 确保缓存目录存在

# 测试模式开关
TEST_MODE = False