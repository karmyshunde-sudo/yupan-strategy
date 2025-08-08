# -*- coding: utf-8 -*-
"""
主程序入口：协调各模块执行，实现定时任务和消息推送
对应文档章节：步骤1：明确鱼盆策略核心逻辑（整体执行流程）
"""
import time
from datetime import datetime, time as dt_time

def main():
    """主程序函数：初始化组件并执行策略"""
    print("===== 鱼盆策略自动交易信号系统 =====")
    print(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 初始化组件
    from config import TUSHARE_TOKEN
    from etf_data_fetcher import ETFDataFetcher
    from data_cache import DataCache
    from wechat_notify import WechatNotifier
    from etf_pool import ETFPoolManager
    from strategy_core import StrategyCore
    
    # 创建实例
    data_fetcher = ETFDataFetcher(tushare_token=TUSHARE_TOKEN)
    cache_manager = DataCache()
    notifier = WechatNotifier()
    etf_pool_manager = ETFPoolManager(data_fetcher, cache_manager)
    strategy = StrategyCore(data_fetcher, cache_manager, etf_pool_manager)
    
    try:
        # 首次运行强制更新ETF池（确保初始化时有足够标的）
        print("首次运行，强制更新ETF池...")
        etf_pool = etf_pool_manager.update_etf_pool()
        if etf_pool:
            # 推送初始ETF池信息
            pool_info = "\n".join([f"{etf['code']} {etf['name']}" for etf in etf_pool])
            notifier.send_text_message(f"初始ETF池构建完成（{len(etf_pool)}只）：\n{pool_info}")
        
        # 主循环：定时执行任务
        while True:
            current_time = datetime.now()
            beijing_time = current_time.astimezone()  # 转为北京时间
            print(f"\n当前时间: {beijing_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # 1. 每周五下午4点更新ETF池（对应文档4. ETF池构建与调仓机制）
            from config import POOL_UPDATE_DAY, POOL_UPDATE_TIME
            if (beijing_time.weekday() == POOL_UPDATE_DAY and  # 周五（0=周一，4=周五）
                beijing_time.hour == POOL_UPDATE_TIME.hour and
                beijing_time.minute == POOL_UPDATE_TIME.minute):
                
                print("执行每周ETF池更新...")
                new_pool = etf_pool_manager.update_etf_pool()
                pool_info = "\n".join([f"{etf['code']} {etf['name']}" for etf in new_pool])
                notifier.send_text_message(f"ETF池更新完成（{len(new_pool)}只）：\n{pool_info}")
                time.sleep(60)  # 避免重复执行
            
            # 2. 每天上午11点推送ETF池信息（对应文档项目功能11）
            from config import POOL_PUSH_TIME
            last_push = cache_manager.get_last_push_time()
            if (beijing_time.hour == POOL_PUSH_TIME.hour and
                beijing_time.minute == POOL_PUSH_TIME.minute and
                (not last_push["pool"] or last_push["pool"] != beijing_time.date().isoformat())):
                
                print("推送ETF池信息...")
                etf_pool = etf_pool_manager.get_etf_pool()
                pool_info = "\n".join([f"{etf['code']} {etf['name']}" for etf in etf_pool])
                notifier.send_text_message(f"当前ETF池（{len(etf_pool)}只）：\n{pool_info}")
                cache_manager.save_last_push_time("pool", beijing_time.date().isoformat())
                time.sleep(60)
            
            # 3. 每天下午2点执行策略判断（对应文档项目功能12）
            from config import STRATEGY_CHECK_TIME
            if (beijing_time.hour == STRATEGY_CHECK_TIME.hour and
                beijing_time.minute == STRATEGY_CHECK_TIME.minute and
                (not last_push["strategy"] or last_push["strategy"] != beijing_time.date().isoformat())):
                
                print("执行策略判断...")
                result = strategy.execute_strategy()
                notifier.send_text_message(f"今日策略建议：\n{result['summary']}")
                cache_manager.save_last_push_time("strategy", beijing_time.date().isoformat())
                time.sleep(60)
            
            # 检查是否为交易日（非交易日减少循环频率）
            from utils import is_market_open_day
            if not is_market_open_day(beijing_time.date()):
                print("非交易日，休眠1小时...")
                time.sleep(3600)
            else:
                # 交易日每5分钟检查一次
                time.sleep(300)
    
    except KeyboardInterrupt:
        print("\n用户中断程序，正在退出...")
    except Exception as e:
        print(f"程序运行出错: {str(e)}")
        # 发送错误通知
        notifier.send_text_message(f"程序运行出错: {str(e)}，请检查日志")
    finally:
        # 清理资源
        data_fetcher.close()
        print("程序已退出")

if __name__ == "__main__":
    main()