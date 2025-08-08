# -*- coding: utf-8 -*-
"""
多数据源ETF数据获取：从AkShare、Baostock等获取数据，统一格式
对应文档章节：19. 主数据源与备用数据源
"""
import pandas as pd
import json
import os
import time
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union

class ETFDataFetcher:
    def __init__(self, tushare_token: Optional[str] = None):
        """
        初始化数据获取器
        参数: tushare_token - Tushare的访问令牌（可选）
        """
        self.tushare_token = tushare_token  # 存储Tushare令牌
        self.baostock_initialized = False  # Baostock初始化状态
        self._initialize_cache()  # 初始化缓存
    
    def _initialize_cache(self):
        """初始化缓存目录并清理过期文件"""
        from config import CACHE_DIR  # 从配置获取缓存目录
        if not os.path.exists(CACHE_DIR):
            os.makedirs(CACHE_DIR)  # 目录不存在则创建
            print(f"创建缓存目录: {CACHE_DIR}")
        
        self._clean_expired_cache()  # 清理过期缓存
    
    def _clean_expired_cache(self):
        """清理过期缓存（按不同数据类型设置过期时间）"""
        # 缓存过期时间配置（对应文档20. 数据保存要求）
        CACHE_EXPIRE_HOURS = {
            "etf_list": 24,        # ETF列表缓存24小时
            "etf_quote": 1,        # 行情数据缓存1小时
            "etf_basic": 12        # 基本信息缓存12小时
        }
        
        now = datetime.now()
        from config import CACHE_DIR
        
        for filename in os.listdir(CACHE_DIR):
            file_path = os.path.join(CACHE_DIR, filename)
            try:
                if os.path.isfile(file_path):
                    # 解析文件名获取数据类型
                    parts = filename.split("_")
                    if len(parts) >= 2 and parts[0] in CACHE_EXPIRE_HOURS:
                        cache_type = parts[0]
                        expire_hours = CACHE_EXPIRE_HOURS[cache_type]
                        
                        # 检查文件是否过期
                        file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                        if (now - file_mtime) > timedelta(hours=expire_hours):
                            os.remove(file_path)
                            print(f"清理过期缓存: {filename}")
            except Exception as e:
                print(f"清理缓存出错({filename}): {str(e)}")
    
    def _get_cache_key(self, data_type: str, identifier: str = "") -> str:
        """生成缓存键（包含时间戳，便于过期管理）"""
        return f"{data_type}_{identifier}_{int(time.time() // 3600)}"
    
    def _cache_data(self, data_type: str, identifier: str, data) -> None:
        """缓存数据到本地文件"""
        cache_key = self._get_cache_key(data_type, identifier)
        from config import CACHE_DIR
        cache_file = os.path.join(CACHE_DIR, f"{cache_key}.json")
        
        try:
            # 转换数据为可序列化格式
            if isinstance(data, pd.DataFrame):
                data_to_cache = {
                    "type": "dataframe",
                    "values": data.to_dict(orient="records"),
                    "columns": data.columns.tolist()
                }
            else:
                data_to_cache = {"type": "list", "values": data}
            
            # 写入缓存文件
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump({
                    "timestamp": datetime.now().isoformat(),
                    "data": data_to_cache
                }, f, ensure_ascii=False, indent=2)
            
            print(f"缓存{data_type}数据: {identifier}")
        except Exception as e:
            print(f"缓存{data_type}出错: {str(e)}")
    
    def _load_cached_data(self, data_type: str, identifier: str) -> Optional[Union[pd.DataFrame, List[Dict]]]:
        """从缓存加载数据（过期则返回None）"""
        cache_key = self._get_cache_key(data_type, identifier)
        from config import CACHE_DIR
        cache_file = os.path.join(CACHE_DIR, f"{cache_key}.json")
        
        # 检查是否有未过期的缓存文件
        if not os.path.exists(cache_file):
            for filename in os.listdir(CACHE_DIR):
                if filename.startswith(f"{data_type}_{identifier}_") and filename.endswith(".json"):
                    file_path = os.path.join(CACHE_DIR, filename)
                    file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                    CACHE_EXPIRE_HOURS = {"etf_list":24, "etf_quote":1, "etf_basic":12}
                    if (datetime.now() - file_mtime) < timedelta(hours=CACHE_EXPIRE_HOURS[data_type]):
                        cache_file = file_path
                        break
            else:
                return None  # 无有效缓存
        
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                cached = json.load(f)
            
            # 检查缓存是否过期
            cache_time = datetime.fromisoformat(cached["timestamp"])
            CACHE_EXPIRE_HOURS = {"etf_list":24, "etf_quote":1, "etf_basic":12}
            if (datetime.now() - cache_time) > timedelta(hours=CACHE_EXPIRE_HOURS[data_type]):
                os.remove(cache_file)
                return None
            
            # 恢复数据格式
            data = cached["data"]
            if data["type"] == "dataframe":
                return pd.DataFrame(data["values"], columns=data["columns"])
            return data["values"]
        except Exception as e:
            print(f"加载缓存出错({data_type}): {str(e)}")
            if os.path.exists(cache_file):
                os.remove(cache_file)
            return None
    
    def _initialize_baostock(self) -> bool:
        """初始化Baostock连接（自动安装依赖）"""
        if self.baostock_initialized:
            return True  # 已初始化则直接返回
        
        try:
            import baostock as bs
        except ImportError:
            print("安装baostock...")
            import subprocess
            import sys
            subprocess.check_call([sys.executable, "-m", "pip", "install", "baostock"])
            import baostock as bs
        
        # 登录Baostock
        lg = bs.login()
        if lg.error_code != '0':
            print(f"Baostock登录失败: {lg.error_msg}")
            return False
        
        self.baostock_initialized = True
        print("Baostock初始化成功")
        return True
    
    def _initialize_tushare(self):
        """初始化Tushare连接（自动安装依赖）"""
        if not self.tushare_token:
            raise ValueError("使用Tushare需在config.py填写TUSHARE_TOKEN")
        
        try:
            import tushare as ts
        except ImportError:
            print("安装tushare...")
            import subprocess
            import sys
            subprocess.check_call([sys.executable, "-m", "pip", "install", "tushare"])
            import tushare as ts
        
        ts.set_token(self.tushare_token)
        return ts.pro_api()
    
    def get_etf_list(self, source: Optional[str] = None) -> List[Dict]:
        """
        获取全市场ETF列表（无硬编码，动态获取）
        对应文档章节：4. ETF池构建与调仓机制
        返回: 格式为[{"code": "代码", "name": "名称", "type": "类型"}]
        """
        from config import DATA_SOURCE_PRIORITY
        sources = [source] if source else DATA_SOURCE_PRIORITY  # 数据源优先级
        
        for src in sources:
            try:
                # 尝试从缓存加载
                cached_data = self._load_cached_data("etf_list", src)
                if cached_data:
                    print(f"从缓存加载{src}的ETF列表（{len(cached_data)}只）")
                    return cached_data
                
                print(f"从{src}获取ETF列表...")
                etf_list = []
                
                if src == "akshare":
                    import akshare as ak  # 自动安装见下方异常处理
                    etf_df = ak.fund_etf_category_sina(symbol="ETF基金")  # 获取全量ETF
                    # 提取代码、名称并推断类型
                    for _, row in etf_df.iterrows():
                        etf_list.append({
                            "code": row["代码"],
                            "name": row["名称"],
                            "type": self._infer_etf_type(row["名称"])
                        })
                
                elif src == "baostock":
                    if not self._initialize_baostock():
                        continue  # 初始化失败则尝试下一个数据源
                    import baostock as bs
                    # 获取上一交易日所有股票，筛选ETF类型
                    last_trade_day = get_last_trading_day().strftime("%Y-%m-%d")
                    rs = bs.query_all_stock(day=last_trade_day)
                    while (rs.error_code == '0') & rs.next():
                        item = rs.get_row_data()
                        if item[3] == "ETF":  # 筛选ETF类型
                            etf_list.append({
                                "code": item[0].split(".")[0],  # 提取纯代码（去除市场后缀）
                                "name": item[2],
                                "type": self._infer_etf_type(item[2])
                            })
                
                elif src == "sina":
                    # 新浪财经ETF列表API
                    url = "https://finance.sina.com.cn/api/roll/get?page=1&num=500&channel=finance.stock.etf&format=json"
                    response = requests.get(url, timeout=10)
                    data = response.json()
                    for item in data["result"]["data"]:
                        etf_list.append({
                            "code": item["code"],
                            "name": item["title"],
                            "type": self._infer_etf_type(item["title"])
                        })
                
                elif src == "tushare":
                    pro = self._initialize_tushare()
                    df = pro.fund_basic(market='E', status='L')  # 获取ETF基本信息
                    for _, row in df.iterrows():
                        etf_list.append({
                            "code": row["ts_code"],
                            "name": row["name"],
                            "type": self._infer_etf_type(row["name"])
                        })
                
                if etf_list:
                    print(f"从{src}获取到{len(etf_list)}只ETF")
                    self._cache_data("etf_list", src, etf_list)  # 缓存数据
                    return etf_list
                print(f"{src}未返回有效ETF列表")
            
            except ImportError:
                # 自动安装缺失的依赖
                print(f"安装{src}依赖...")
                import subprocess
                import sys
                if src == "akshare":
                    subprocess.check_call([sys.executable, "-m", "pip", "install", "akshare"])
                continue  # 安装后重试下一个数据源（会重新进入循环）
            except Exception as e:
                print(f"{src}获取ETF列表出错: {str(e)}")
                continue
        
        # 若所有数据源失败，返回空列表（由上层处理补充逻辑）
        print("所有数据源均无法获取ETF列表")
        return []
    
    def get_etf_quote(self, code: str, source: Optional[str] = None, 
                     start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """
        获取ETF行情数据（开盘价、收盘价等）
        对应文档章节：2. 原有买卖信号与仓位管理
        """
        from config import DATA_SOURCE_PRIORITY
        sources = [source] if source else DATA_SOURCE_PRIORITY
        
        # 处理默认日期（默认取最近90天）
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            start_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
        
        for src in sources:
            try:
                cache_id = f"{code}_{start_date}_{end_date}"
                cached_data = self._load_cached_data("etf_quote", cache_id)
                if isinstance(cached_data, pd.DataFrame) and not cached_data.empty:
                    print(f"从缓存加载{code}行情（{len(cached_data)}条）")
                    return cached_data
                
                print(f"从{src}获取{code}行情...")
                df = None
                
                if src == "akshare":
                    import akshare as ak
                    df = ak.fund_etf_hist_sina(symbol=code, adjust="qfq")  # 前复权数据
                    # 统一列名
                    df = df.rename(columns={
                        "日期": "date", "开盘价": "open", "最高价": "high",
                        "最低价": "low", "收盘价": "close", "成交量": "volume", "成交额": "amount"
                    })
                    # 筛选日期范围
                    df["date"] = pd.to_datetime(df["date"])
                    mask = (df["date"] >= start_date) & (df["date"] <= end_date)
                    df = df.loc[mask]
                    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
                
                elif src == "baostock":
                    if not self._initialize_baostock():
                        continue
                    import baostock as bs
                    # Baostock代码格式为"代码.市场"（如510300.SH）
                    bs_code = f"{code}.SH" if code.startswith("5") else f"{code}.SZ"
                    rs = bs.query_history_k_data_plus(
                        bs_code,
                        "date,open,high,low,close,volume,amount",
                        start_date=start_date,
                        end_date=end_date,
                        frequency="d",
                        adjustflag="3"  # 前复权
                    )
                    # 解析结果为DataFrame
                    data_list = []
                    while (rs.error_code == '0') & rs.next():
                        data = rs.get_row_data()
                        data_list.append({
                            "date": data[0],
                            "open": float(data[1]) if data[1] else None,
                            "high": float(data[2]) if data[2] else None,
                            "low": float(data[3]) if data[3] else None,
                            "close": float(data[4]) if data[4] else None,
                            "volume": float(data[5]) if data[5] else None,
                            "amount": float(data[6]) if data[6] else None
                        })
                    df = pd.DataFrame(data_list)
                
                elif src == "sina":
                    # 新浪代码格式为"sh代码"或"sz代码"
                    sina_code = f"sh{code}" if code.startswith("5") else f"sz{code}"
                    url = f"https://finance.sina.com.cn/realstock/company/{sina_code}/lishi/{start_date}-{end_date}.js"
                    response = requests.get(url, timeout=10)
                    # 解析新浪JS格式数据
                    data_str = response.text.split("=")[1].rsplit(";", 1)[0]
                    data = json.loads(data_str)
                    # 转换为DataFrame
                    rows = []
                    for item in data["data"]:
                        date_str, open_p, high, low, close, volume = item
                        rows.append({
                            "date": date_str, "open": float(open_p), "high": float(high),
                            "low": float(low), "close": float(close), "volume": float(volume), "amount": None
                        })
                    df = pd.DataFrame(rows)
                
                elif src == "tushare":
                    pro = self._initialize_tushare()
                    # Tushare日期格式为YYYYMMDD
                    ts_start = start_date.replace("-", "")
                    ts_end = end_date.replace("-", "")
                    df = pro.fund_daily(ts_code=code, start_date=ts_start, end_date=ts_end)
                    if not df.empty:
                        df = df.rename(columns={"trade_date": "date", "vol": "volume"})
                        df["date"] = df["date"].apply(lambda x: f"{x[:4]}-{x[4:6]}-{x[6:]}")  # 转换日期格式
                        df = df.sort_values("date")  # 按日期排序
                
                if df is not None and not df.empty:
                    # 确保列名统一（补充缺失列）
                    required_cols = ["date", "open", "high", "low", "close", "volume"]
                    for col in required_cols:
                        if col not in df.columns:
                            df[col] = None
                    # 转换为数值类型
                    for col in ["open", "high", "low", "close", "volume", "amount"]:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors="coerce")
                    
                    print(f"从{src}获取到{code}的{len(df)}条行情数据")
                    self._cache_data("etf_quote", cache_id, df)  # 缓存数据
                    return df
                print(f"{src}未返回{code}的有效行情数据")
            
            except Exception as e:
                print(f"{src}获取{code}行情出错: {str(e)}")
                continue
        
        # 若所有数据源失败，返回空DataFrame（由上层处理）
        return pd.DataFrame()
    
    def get_etf_basic_info(self, code: str, source: Optional[str] = None) -> Dict:
        """
        获取ETF基本信息（规模、跟踪误差等）
        对应文档章节：2. 标的筛选标准
        """
        from config import DATA_SOURCE_PRIORITY
        sources = [source] if source else DATA_SOURCE_PRIORITY
        
        for src in sources:
            try:
                cached_data = self._load_cached_data("etf_basic", code)
                if cached_data:
                    print(f"从缓存加载{code}基本信息")
                    return cached_data
                
                print(f"从{src}获取{code}基本信息...")
                info = {
                    "规模": None, "跟踪误差": None, "日均成交额": None,
                    "成立日期": None, "基金公司": None, "跟踪指数": None
                }
                
                if src == "akshare":
                    import akshare as ak
                    fund_info = ak.fund_info_em(code=code)  # 获取基金信息
                    info_dict = {item[0]: item[1] for item in fund_info}
                    
                    # 解析规模（转换为元）
                    if "基金规模" in info_dict:
                        size_str = info_dict["基金规模"].replace("亿元", "").strip()
                        info["规模"] = float(size_str) * 100000000 if size_str else None
                    
                    # 解析跟踪误差（转换为小数）
                    if "跟踪误差" in info_dict:
                        te_str = info_dict["跟踪误差"].replace("%", "").strip()
                        info["跟踪误差"] = float(te_str) / 100 if te_str else None
                    
                    info["成立日期"] = info_dict.get("成立日期")
                    info["基金公司"] = info_dict.get("基金公司")
                    info["跟踪指数"] = info_dict.get("跟踪标的")
                
                elif src == "tushare":
                    pro = self._initialize_tushare()
                    # 获取基本信息
                    basic_df = pro.fund_basic(ts_code=code, market='E')
                    if not basic_df.empty:
                        basic = basic_df.iloc[0]
                        info["基金公司"] = basic.get("mgr")
                        info["成立日期"] = basic.get("found_date")
                        info["跟踪误差"] = basic.get("tracking_error")  # 已为小数
                    
                    # 获取规模（总份额×净值）
                    size_df = pro.fund_share(ts_code=code, limit=1)
                    if not size_df.empty and not basic_df.empty:
                        info["规模"] = size_df.iloc[0]["total_share"] * basic_df.iloc[0]["nav"]
                    
                    # 计算日均成交额（最近30天）
                    end_date = datetime.now().strftime("%Y%m%d")
                    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
                    trade_df = pro.fund_daily(ts_code=code, start_date=start_date, end_date=end_date)
                    info["日均成交额"] = trade_df["amount"].mean() if not trade_df.empty else None
                
                # 若获取到有效信息则返回
                if any(v is not None for v in info.values()):
                    print(f"从{src}获取到{code}的基本信息")
                    self._cache_data("etf_basic", code, info)  # 缓存数据
                    return info
                print(f"{src}未返回{code}的有效基本信息")
            
            except Exception as e:
                print(f"{src}获取{code}基本信息出错: {str(e)}")
                continue
        
        # 所有数据源失败，返回空信息
        return info
    
    def _infer_etf_type(self, name: str) -> str:
        """根据名称推断ETF类型（宽基/行业/主题）"""
        name_lower = name.lower()
        
        # 宽基ETF（如沪深300、中证500）
        broad_based = ["沪深300", "中证500", "上证50", "创业板", "科创板", "中证1000"]
        for idx in broad_based:
            if idx.lower() in name_lower:
                return "宽基"
        
        # 行业ETF（如半导体、医疗）
        sectors = ["半导体", "医疗", "证券", "银行", "地产", "消费", "新能源", "光伏"]
        for sector in sectors:
            if sector.lower() in name_lower:
                return "行业"
        
        # 主题ETF（如AI、大数据）
        themes = ["ai", "人工智能", "大数据", "云计算", "区块链", "碳中和"]
        for theme in themes:
            if theme.lower() in name_lower:
                return "主题"
        
        return "其他"
    
    def close(self):
        """关闭数据源连接（释放资源）"""
        if self.baostock_initialized:
            try:
                import baostock as bs
                bs.logout()
                print("Baostock已注销")
            except:
                pass
            self.baostock_initialized = False


# 工具函数引用（避免循环导入）
from utils import get_last_trading_day