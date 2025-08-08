# -*- coding: utf-8 -*-
"""
工具函数：提供通用功能
"""
import time
from datetime import datetime, timedelta
import pytz

def utc_to_beijing(utc_time=None):
    """
    将UTC时间转换为北京时间(UTC+8)
    对应文档章节：6. 时区转换要求
    """
    if utc_time is None:
        utc_time = datetime.utcnow()  # 无输入时取当前UTC时间
    beijing_tz = pytz.timezone('Asia/Shanghai')  # 北京时区
    # 转换时区并返回
    return utc_time.replace(tzinfo=pytz.utc).astimezone(beijing_tz)

def is_trading_time(beijing_time=None):
    """
    判断是否为A股交易时间(北京时间)
    对应文档章节：15. 交易时段重试规则
    """
    if beijing_time is None:
        beijing_time = utc_to_beijing()  # 无输入时取当前北京时间
    
    # 非交易日(周末)直接返回False
    if beijing_time.weekday() >= 5:
        return False
    
    current_time = beijing_time.time()
    from config import TRADING_HOURS  # 导入交易时间配置
    
    # 上午9:30-11:30 或 下午13:00-15:00为交易时间
    return (TRADING_HOURS["morning_start"] <= current_time <= TRADING_HOURS["morning_end"]) or \
           (TRADING_HOURS["afternoon_start"] <= current_time <= TRADING_HOURS["afternoon_end"])

def is_market_open_day(date=None):
    """判断指定日期是否为交易日（简化版：排除周末）"""
    if date is None:
        date = utc_to_beijing().date()
    return date.weekday() < 5  # 0-4为周一至周五

def get_last_trading_day(date=None):
    """获取上一个交易日"""
    if date is None:
        date = utc_to_beijing().date()
    day = date
    while True:
        day -= timedelta(days=1)
        if day.weekday() < 5:  # 找到最近的周一至周五
            return day

def format_time(dt):
    """格式化时间为字符串（YYYY-MM-DD HH:MM:SS）"""
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def wait_seconds(seconds):
    """等待指定秒数（用于消息间隔和重试）"""
    time.sleep(seconds)