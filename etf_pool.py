# -*- coding: utf-8 -*-
"""
ETF池管理：动态生成10只ETF，无硬编码，支持初始化不足时补充
对应文档章节：4. ETF池构建与调仓机制
"""
class ETFPoolManager:
    def __init__(self, data_fetcher, cache_manager):
        """
        初始化ETF池管理器
        参数:
            data_fetcher - 数据获取器实例
            cache_manager - 缓存管理器实例
        """
        self.data_fetcher = data_fetcher
        self.cache_manager = cache_manager
        # 移除硬编码的默认ETF列表，完全通过动态筛选生成
    
    def _filter_etf(self, etf):
        """
        筛选符合条件的ETF（严格按文档标准）
        对应文档章节：2. 标的筛选标准（规模≥5亿、成交额≥5000万等）
        """
        try:
            from config import MIN_DAILY_TURNOVER, MIN_SIZE, MAX_TRACKING_ERROR
            
            # 获取基本信息（规模、跟踪误差等）
            basic_info = self.data_fetcher.get_etf_basic_info(etf["code"])
            
            # 1. 规模≥5亿（单位：元）
            if basic_info["规模"] is not None and basic_info["规模"] < MIN_SIZE:
                print(f"{etf['code']} {etf['name']} 规模不足（{basic_info['规模']/1e8:.2f}亿）")
                return False
            
            # 2. 跟踪误差≤2%
            if basic_info["跟踪误差"] is not None and basic_info["跟踪误差"] > MAX_TRACKING_ERROR:
                print(f"{etf['code']} {etf['name']} 跟踪误差过大（{basic_info['跟踪误差']*100:.2f}%）")
                return False
            
            # 3. 日均成交额≥5000万（单位：元）
            if basic_info["日均成交额"] is not None and basic_info["日均成交额"] < MIN_DAILY_TURNOVER:
                print(f"{etf['code']} {etf['name']} 成交额不足（{basic_info['日均成交额']/1e4:.2f}万）")
                return False
            
            # 4. 行情数据充足（至少20天，用于计算均线）
            quote = self.data_fetcher.get_etf_quote(etf["code"])
            if not quote or len(quote) < 20:
                print(f"{etf['code']} {etf['name']} 行情数据不足（{len(quote)}天）")
                return False
            
            print(f"{etf['code']} {etf['name']} 符合筛选条件")
            return True
        except Exception as e:
            print(f"筛选{etf['code']}出错: {str(e)}（视为不符合）")
            return False
    
    def _score_etf(self, etf):
        """
        为ETF打分（综合指标排序）
        对应文档章节：4. 调仓触发条件a（20日均线趋势+成交量+行业景气度）
        """
        try:
            # 获取行情数据
            quote = self.data_fetcher.get_etf_quote(etf["code"])
            if not quote:
                print(f"{etf['code']} 无行情数据（得分0）")
                return 0
            
            # 计算20日均线（若数据源未提供）
            if "20日均线" not in quote[-1]:
                closes = [day.get("close", 0) for day in quote]
                for i in range(19, len(quote)):
                    quote[i]["20日均线"] = sum(closes[i-19:i+1]) / 20  # 20日平均值
            
            # 计算5日均量（若数据源未提供）
            if "5日均量" not in quote[-1]:
                volumes = [day.get("volume", 0) for day in quote]
                for i in range(4, len(quote)):
                    quote[i]["5日均量"] = sum(volumes[i-4:i+1]) / 5  # 5日平均成交量
            
            latest = quote[-1]  # 最新数据
            prev_day = quote[-2] if len(quote) >= 2 else latest  # 前一天数据
            
            # 基础分50分
            score = 50
            
            # 1. 20日均线趋势（最高加10分）
            if len(quote) >= 20 and latest["20日均线"] > quote[-20]["20日均线"]:
                score += 10
                print(f"{etf['code']} 均线上升（+10分）")
            
            # 2. 近期涨幅（最高加10分）
            if latest["close"] > prev_day["close"]:
                rise_rate = (latest["close"] - prev_day["close"]) / prev_day["close"] * 100
                add_score = min(10, rise_rate)  # 涨幅超过10%也只加10分
                score += add_score
                print(f"{etf['code']} 涨幅{rise_rate:.2f}%（+{add_score:.2f}分）")
            
            # 3. 成交量放大（最高加5分）
            if latest["volume"] > latest.get("5日均量", 0) * 1.2:
                score += 5
                print(f"{etf['code']} 成交量放大（+5分）")
            
            # 4. 行业景气度（简化：宽基/行业/主题分别加分）
            if etf.get("type") == "宽基":
                score += 3
            elif etf.get("type") in ["行业", "主题"]:
                score += 5
            print(f"{etf['code']} 最终得分: {score:.2f}")
            return score
        except Exception as e:
            print(f"为{etf['code']}打分出错: {str(e)}（得分0）")
            return 0
    
    def update_etf_pool(self):
        """
        更新ETF池（确保10只，无硬编码）
        对应文档章节：4. ETF池构建与调仓机制（每周五更新，固定10只）
        处理初始化空池/不足10只的情况
        """
        print("开始更新ETF池（严格按文档规则）")
        
        # 1. 获取全市场ETF列表（无硬编码）
        etf_list = self.data_fetcher.get_etf_list()
        if not etf_list:
            # 极端情况：全市场列表为空，尝试按行业分类补充
            etf_list = self._get_fallback_etf_list()
        
        # 2. 筛选符合条件的ETF（规模、成交额等）
        filtered_etfs = [etf for etf in etf_list if self._filter_etf(etf)]
        print(f"筛选后符合条件的ETF共{len(filtered_etfs)}只")
        
        # 3. 若筛选后不足10只，扩大筛选范围（降低标准但保留核心条件）
        from config import ETF_POOL_SIZE
        if len(filtered_etfs) < ETF_POOL_SIZE:
            print(f"符合条件的ETF不足{ETF_POOL_SIZE}只，扩大筛选范围...")
            # 降低非核心条件（如跟踪误差放宽至3%）
            relaxed_etfs = [
                etf for etf in etf_list 
                if etf not in filtered_etfs and self._filter_etf_relaxed(etf)
            ]
            filtered_etfs.extend(relaxed_etfs)
            print(f"扩大范围后共{len(filtered_etfs)}只")
        
        # 4. 若仍不足，从全市场未筛选的ETF中补充（仅保留规模≥3亿的底线）
        if len(filtered_etfs) < ETF_POOL_SIZE:
            print(f"仍不足{ETF_POOL_SIZE}只，补充底线ETF...")
           底线_etfs = [
                etf for etf in etf_list 
                if etf not in filtered_etfs and self._filter_etf_minimal(etf)
            ]
            filtered_etfs.extend(底线_etfs)
            print(f"补充后共{len(filtered_etfs)}只")
        
        # 5. 按得分排序，取前10只
        scored_etfs = [(etf, self._score_etf(etf)) for etf in filtered_etfs]
        scored_etfs.sort(key=lambda x: x[1], reverse=True)  # 按得分降序
        new_pool = [etf for etf, score in scored_etfs[:ETF_POOL_SIZE]]
        
        # 6. 保存更新后的ETF池
        self.cache_manager.save_etf_pool(new_pool)
        print(f"ETF池更新完成（{len(new_pool)}只，完全动态生成）")
        return new_pool
    
    def _filter_etf_relaxed(self, etf):
        """放宽条件的筛选（用于ETF不足时）"""
        try:
            from config import MIN_DAILY_TURNOVER, MIN_SIZE
            basic_info = self.data_fetcher.get_etf_basic_info(etf["code"])
            
            # 核心条件不变（规模≥5亿、成交额≥5000万）
            if basic_info["规模"] is not None and basic_info["规模"] < MIN_SIZE:
                return False
            if basic_info["日均成交额"] is not None and basic_info["日均成交额"] < MIN_DAILY_TURNOVER:
                return False
            
            # 放宽跟踪误差至3%
            if basic_info["跟踪误差"] is not None and basic_info["跟踪误差"] > 0.03:
                return False
            
            # 行情数据≥15天（原20天）
            quote = self.data_fetcher.get_etf_quote(etf["code"])
            return len(quote) >= 15
        except:
            return False
    
    def _filter_etf_minimal(self, etf):
        """底线筛选（仅保留规模≥3亿的ETF，用于极端情况）"""
        try:
            basic_info = self.data_fetcher.get_etf_basic_info(etf["code"])
            # 仅要求规模≥3亿
            return basic_info["规模"] is not None and basic_info["规模"] >= 300000000
        except:
            return False
    
    def _get_fallback_etf_list(self):
        """极端情况：全市场列表为空时，按行业动态生成备选列表"""
        print("全市场ETF列表为空，生成备选列表...")
        fallback_list = []
        # 按行业分类，通过数据源搜索（无硬编码）
        sectors = ["宽基", "半导体", "医疗", "新能源", "证券", "军工", "消费", "科技", "光伏", "AI"]
        for sector in sectors:
            # 尝试从AkShare搜索行业ETF（无硬编码代码）
            try:
                import akshare as ak
                etf_df = ak.fund_etf_category_sina(symbol="ETF基金")
                # 模糊匹配行业名称
                sector_etfs = etf_df[etf_df["名称"].str.contains(sector)].to_dict('records')
                if sector_etfs:
                    # 取该行业第一只ETF
                    fallback_list.append({
                        "code": sector_etfs[0]["代码"],
                        "name": sector_etfs[0]["名称"],
                        "type": "行业" if sector != "宽基" else "宽基"
                    })
            except:
                continue
        return fallback_list[:10]  # 最多10只
    
    def get_etf_pool(self, force_update=False):
        """
        获取当前ETF池（不足则自动更新）
        对应文档章节：4. ETF池构建与调仓机制
        """
        current_pool = self.cache_manager.load_etf_pool()
        from config import ETF_POOL_SIZE
        
        # 若强制更新、空池或不足10只，触发更新
        if force_update or not current_pool or len(current_pool) < ETF_POOL_SIZE:
            print(f"ETF池不足{ETF_POOL_SIZE}只，触发更新...")
            return self.update_etf_pool()
        
        # 检查池中ETF是否仍符合条件（剔除不符合的）
        valid_pool = [etf for etf in current_pool if self._filter_etf(etf)]
        if len(valid_pool) < ETF_POOL_SIZE:
            print(f"部分ETF已不符合条件，更新ETF池...")
            return self.update_etf_pool()
        
        print(f"使用现有ETF池（{len(valid_pool)}只）")
        return valid_pool
    
    def get_stable_candidates(self):
        """
        获取稳健仓候选ETF（宽基为主）
        对应文档章节：1. 基础资金与仓位划分 - 稳健仓（宽基ETF）
        """
        pool = self.get_etf_pool()
        # 优先选择宽基ETF
        stable_candidates = [etf for etf in pool if etf.get("type") == "宽基"]
        print(f"宽基ETF候选（{len(stable_candidates)}只）")
        
        # 不足则从其他类型补充
        if len(stable_candidates) < 3:
            other_etfs = [etf for etf in pool if etf not in stable_candidates]
            stable_candidates.extend(other_etfs[:3 - len(stable_candidates)])
        
        # 按得分排序
        stable_candidates.sort(key=lambda x: self._score_etf(x), reverse=True)
        return stable_candidates
    
    def get_aggressive_candidates(self):
        """
        获取激进仓候选ETF（行业/主题为主）
        对应文档章节：1. 基础资金与仓位划分 - 激进仓（行业/主题ETF）
        """
        pool = self.get_etf_pool()
        # 优先选择行业/主题ETF
        aggressive_candidates = [etf for etf in pool if etf.get("type") in ["行业", "主题"]]
        print(f"行业/主题ETF候选（{len(aggressive_candidates)}只）")
        
        # 不足则从其他类型补充
        if len(aggressive_candidates) < 3:
            other_etfs = [etf for etf in pool if etf not in aggressive_candidates]
            aggressive_candidates.extend(other_etfs[:3 - len(aggressive_candidates)])
        
        # 按得分排序
        aggressive_candidates.sort(key=lambda x: self._score_etf(x), reverse=True)
        return aggressive_candidates