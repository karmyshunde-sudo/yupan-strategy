# -*- coding: utf-8 -*-
"""
数据缓存管理：持久化存储ETF池、持仓、交易记录等
对应文档章节：20. 数据保存要求
"""
import json
import os
from datetime import datetime, timedelta

class DataCache:
    def __init__(self):
        """初始化缓存管理器（确保缓存目录存在）"""
        from config import CACHE_DIR
        self.cache_dir = CACHE_DIR
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
            print(f"创建缓存目录: {self.cache_dir}")
        
        # 定义缓存文件路径
        self.etf_pool_file = os.path.join(self.cache_dir, "etf_pool.json")  # ETF池
        self.positions_file = os.path.join(self.cache_dir, "positions.json")  # 持仓
        self.trade_history_file = os.path.join(self.cache_dir, "trade_history.json")  # 交易记录
        self.last_push_time_file = os.path.join(self.cache_dir, "last_push_time.json")  # 推送时间
    
    def save_etf_pool(self, etf_pool):
        """
        保存ETF池到本地
        对应文档章节：4. ETF池构建与调仓机制
        """
        with open(self.etf_pool_file, 'w', encoding='utf-8') as f:
            json.dump({
                "timestamp": datetime.now().isoformat(),
                "data": etf_pool
            }, f, ensure_ascii=False, indent=2)
        print(f"已保存ETF池（{len(etf_pool)}只）")
    
    def load_etf_pool(self):
        """
        加载本地保存的ETF池
        对应文档章节：4. ETF池构建与调仓机制
        """
        if not os.path.exists(self.etf_pool_file):
            print("ETF池缓存不存在（首次运行）")
            return []
        
        try:
            with open(self.etf_pool_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                pool = data.get("data", [])
                print(f"加载ETF池（{len(pool)}只）")
                return pool
        except Exception as e:
            print(f"加载ETF池出错: {str(e)}（返回空池）")
            return []
    
    def save_positions(self, positions):
        """
        保存持仓信息
        对应文档章节：1. 基础资金与仓位划分
        """
        with open(self.positions_file, 'w', encoding='utf-8') as f:
            json.dump({
                "timestamp": datetime.now().isoformat(),
                "data": positions
            }, f, ensure_ascii=False, indent=2)
        print("已保存持仓信息")
    
    def load_positions(self):
        """
        加载持仓信息
        对应文档章节：1. 基础资金与仓位划分
        """
        if not os.path.exists(self.positions_file):
            print("持仓缓存不存在（首次运行）")
            return {
                "stable": None,  # 稳健仓
                "aggressive": None,  # 激进仓
                "arbitrage": None  # 套利仓
            }
        
        try:
            with open(self.positions_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                print("加载持仓信息")
                return data.get("data", {"stable": None, "aggressive": None, "arbitrage": None})
        except Exception as e:
            print(f"加载持仓出错: {str(e)}（返回空持仓）")
            return {"stable": None, "aggressive": None, "arbitrage": None}
    
    def add_trade_record(self, record):
        """
        添加交易记录
        对应文档章节：10. 记录每笔操作
        """
        # 获取现有记录
        history = self.get_trade_history()
        # 添加新记录（包含时间戳）
        history.append({
            "timestamp": datetime.now().isoformat(),
            **record
        })
        
        # 保存更新后的记录
        with open(self.trade_history_file, 'w', encoding='utf-8') as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
        print(f"添加交易记录: {record.get('type')} {record.get('etf', {}).get('code', '')}")
    
    def get_trade_history(self):
        """
        获取交易历史记录
        对应文档章节：10. 记录每笔操作
        """
        if not os.path.exists(self.trade_history_file):
            print("交易记录缓存不存在（首次运行）")
            return []
        
        try:
            with open(self.trade_history_file, 'r', encoding='utf-8') as f:
                history = json.load(f)
                print(f"加载交易记录（{len(history)}条）")
                return history
        except Exception as e:
            print(f"加载交易记录出错: {str(e)}（返回空记录）")
            return []
    
    def save_last_push_time(self, push_type, time_str):
        """保存最后推送时间（避免重复推送）"""
        data = self.get_last_push_time()
        data[push_type] = time_str  # 更新指定类型的推送时间
        
        with open(self.last_push_time_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"更新{push_type}最后推送时间: {time_str}")
    
    def get_last_push_time(self):
        """获取最后推送时间"""
        if not os.path.exists(self.last_push_time_file):
            print("推送时间缓存不存在（首次运行）")
            return {"pool": None, "strategy": None, "pool_update": None}
        
        try:
            with open(self.last_push_time_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                print("加载最后推送时间")
                return data
        except Exception as e:
            print(f"加载推送时间出错: {str(e)}（返回默认值）")
            return {"pool": None, "strategy": None, "pool_update": None}
    
    def clear_expired_cache(self, max_age_days=7):
        """清理7天前的过期缓存"""
        max_age = timedelta(days=max_age_days)
        for filename in os.listdir(self.cache_dir):
            file_path = os.path.join(self.cache_dir, filename)
            try:
                if os.path.isfile(file_path):
                    file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                    if (datetime.now() - file_time) > max_age:
                        os.remove(file_path)
                        print(f"清理过期缓存: {filename}")
            except Exception as e:
                print(f"清理缓存出错({filename}): {str(e)}")