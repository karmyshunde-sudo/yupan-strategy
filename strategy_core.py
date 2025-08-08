#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
鱼盆策略核心逻辑模块 (strategy_core.py)

【策略整体说明】
本模块实现鱼盆策略的完整核心逻辑，基于文档要求细化了买、卖、调仓、清仓四大机制，
将资金按6:3:1比例分配为稳健仓、激进仓和套利仓，通过多维度条件判断实现智能化操作。

【核心机制详解】

一、买入机制
1. 基础买入条件（适用于所有仓位）
   - 技术面：价格突破20日均线且均线呈上升趋势（连续2日收盘价在均线上方）
   - 量能：当日成交量≥5日均量的1.2倍（确认突破有效性）
   - 估值：PE/PB百分位＜60%（宽基ETF适用），行业景气度＞中性（行业ETF适用）

2. 分仓买入规则
   - 稳健仓：首次买入30%仓位，后续每满足加仓条件增仓20%，最高70%
   - 激进仓：首次买入20%仓位，后续每满足加仓条件增仓15%，最高60%
   - 套利仓：单只标的投入不超过套利资金的30%，多标的分散配置

3. 加仓触发条件
   - 价格沿20日均线上行，回调幅度≤5%且未跌破均线
   - 成交量保持温和（5日均量±30%范围内）
   - 距离上次加仓≥5个交易日

二、卖出机制
1. 基础卖出条件（适用于所有仓位）
   - 技术面：价格跌破20日均线且均线走平/向下（连续2日收盘价在均线下方）
   - 止损：单只标的亏损≥5%（稳健仓）/8%（激进仓）/2%（套利仓）
   - 止盈：单只标的盈利≥15%（稳健仓）/25%（激进仓）/5%（套利仓）

2. 分仓卖出规则
   - 稳健仓：分批卖出（先卖50%，剩余部分跌破10日均线清仓）
   - 激进仓：达到止盈/止损线一次性卖出
   - 套利仓：达到预期收益80%或持仓超3日强制平仓

三、调仓机制
1. 同类型调仓（同仓位内标的切换）
   - 触发条件：当前持仓跌破卖出条件，但存在更优候选标的（满足买入条件）
   - 操作：先卖后买，单日完成，避免空仓风险
   - 频率限制：每月不超过3次

2. 跨类型调仓（不同仓位间资金转移）
   - 牛市环境（沪深300指数月涨幅≥5%）：激进仓比例可提高至40%，稳健仓降至50%
   - 熊市环境（沪深300指数月跌幅≥5%）：稳健仓比例提高至70%，激进仓降至20%
   - 震荡市：维持6:3:1基础比例

四、清仓机制
1. 部分清仓
   - 单只标的基本面恶化（如成分股暴雷≥3只）
   - 行业政策重大利空（如监管收紧、补贴退坡）
   - 执行：清仓该标的，保留同类型其他标的

2. 全部清仓
   - 系统性风险（沪深300指数单日跌幅≥5%）
   - 策略失效（连续3个月跑输基准指数≥5%）
   - 个人资金规划调整（需手动触发）

【模块依赖】
- data_fetcher: 提供ETF行情、估值、事件等数据
- cache_manager: 管理持仓记录、交易历史、策略参数
- etf_pool_manager: 维护ETF候选池及分类（宽基/行业/主题）
"""

from datetime import datetime, timedelta
import numpy as np

class StrategyCore:
    """策略核心类，实现鱼盆策略的买、卖、调仓、清仓全流程"""
    
    def __init__(self, data_fetcher, cache_manager, etf_pool_manager):
        """
        初始化策略核心组件
        参数:
            data_fetcher: 数据获取器实例
            cache_manager: 缓存管理器实例
            etf_pool_manager: ETF池管理器实例
        """
        # 保存数据获取器引用（用于获取各类市场数据）
        self.data_fetcher = data_fetcher
        # 保存缓存管理器引用（用于读写持仓和交易记录）
        self.cache_manager = cache_manager
        # 保存ETF池管理器引用（用于获取候选标的）
        self.etf_pool_manager = etf_pool_manager
        
        # 从缓存加载当前持仓状态（分稳健仓、激进仓、套利仓）
        self.positions = self.cache_manager.load_positions()
        # 从缓存加载历史交易记录（用于计算调仓频率等）
        self.trade_history = self.cache_manager.load_trade_history()
        # 打印初始化信息
        print(f"策略初始化完成 - 加载持仓: {len([p for p in self.positions.values() if p is not None])}个标的")

    # ------------------------------
    # 买入机制相关方法
    # ------------------------------
    def _check_basic_buy_conditions(self, etf_code, position_type):
        """
        检查基础买入条件（适用于所有仓位类型）
        参数:
            etf_code: ETF代码
            position_type: 仓位类型（'stable'/'aggressive'/'arbitrage'）
        返回:
            (是否满足条件, 原因说明)
        """
        try:
            # 1. 获取基础行情数据（至少20天）
            quote = self.data_fetcher.get_etf_quote(etf_code)
            if len(quote) < 20:
                return False, "基础买入条件不满足：行情数据不足20天"
            latest = quote[-1]  # 最新交易日数据
            prev_day = quote[-2]  # 前一交易日数据
            
            # 2. 计算20日均线（若数据源未提供）
            if "ma20" not in latest:
                closes = [day["close"] for day in quote]
                ma20 = np.convolve(closes, np.ones(20)/20, mode='valid')[-1]
            else:
                ma20 = latest["ma20"]
            
            # 3. 技术面条件：价格突破20日均线且连续2日在均线上方
            price_above_ma = (latest["close"] > ma20) and (prev_day["close"] > ma20)
            ma_trend_up = ma20 > quote[-3]["ma20"] if "ma20" in quote[-3] else True
            if not (price_above_ma and ma_trend_up):
                return False, "基础买入条件不满足：未形成有效均线突破趋势"
            
            # 4. 量能条件：当日成交量≥5日均量的1.2倍
            if "volume_ma5" not in latest:
                volumes = [day["volume"] for day in quote[-5:]]
                volume_ma5 = sum(volumes) / 5
            else:
                volume_ma5 = latest["volume_ma5"]
            volume_condition = latest["volume"] >= volume_ma5 * 1.2
            if not volume_condition:
                return False, "基础买入条件不满足：量能未有效放大"
            
            # 5. 分类型附加条件
            if position_type == "stable":
                # 稳健仓：PE百分位＜60%
                pe_percentile = self.data_fetcher.get_etf_valuation(etf_code).get("pe_percentile", 100)
                if pe_percentile >= 60:
                    return False, f"稳健仓买入条件不满足：PE百分位{pe_percentile}%≥60%"
            
            elif position_type == "aggressive":
                # 激进仓：行业景气度＞中性
                industry_sentiment = self.data_fetcher.get_industry_sentiment(etf_code)
                if industry_sentiment <= 0:  # 0=中性，正数=景气，负数=低迷
                    return False, f"激进仓买入条件不满足：行业景气度{industry_sentiment}≤中性"
            
            # 所有条件满足
            return True, "所有基础买入条件均满足"
        except Exception as e:
            return False, f"检查基础买入条件出错：{str(e)}"

    def _check_add_position_conditions(self, etf_code, last_add_date):
        """
        检查加仓条件（适用于稳健仓和激进仓）
        参数:
            etf_code: ETF代码
            last_add_date: 上次加仓日期
        返回:
            (是否满足条件, 原因说明)
        """
        try:
            # 1. 时间条件：距离上次加仓≥5个交易日
            days_since_last_add = (datetime.now() - last_add_date).days
            if days_since_last_add < 5:
                return False, f"加仓条件不满足：距离上次加仓仅{days_since_last_add}天＜5天"
            
            # 2. 价格条件：回调幅度≤5%且未跌破20日均线
            quote = self.data_fetcher.get_etf_quote(etf_code)
            if len(quote) < 2:
                return False, "加仓条件不满足：行情数据不足"
            
            latest = quote[-1]
            recent_high = max(day["high"] for day in quote[-10:])  # 近10日高点
            pullback_ratio = (recent_high - latest["close"]) / recent_high  # 回调幅度
            
            ma20 = latest["ma20"] if "ma20" in latest else np.mean([day["close"] for day in quote[-20:]])
            if pullback_ratio > 0.05 or latest["close"] < ma20:
                return False, f"加仓条件不满足：回调幅度{pullback_ratio:.2%}＞5%或跌破均线"
            
            # 3. 量能条件：成交量保持温和（5日均量±30%）
            volume_ma5 = latest["volume_ma5"] if "volume_ma5" in latest else np.mean([day["volume"] for day in quote[-5:]])
            volume_ratio = latest["volume"] / volume_ma5
            if not (0.7 <= volume_ratio <= 1.3):
                return False, f"加仓条件不满足：成交量波动过大（当前/均量={volume_ratio:.2f}）"
            
            return True, "所有加仓条件均满足"
        except Exception as e:
            return False, f"检查加仓条件出错：{str(e)}"

    # ------------------------------
    # 卖出机制相关方法
    # ------------------------------
    def _check_basic_sell_conditions(self, etf_code, buy_price, position_type):
        """
        检查基础卖出条件（适用于所有仓位类型）
        参数:
            etf_code: ETF代码
            buy_price: 买入价格
            position_type: 仓位类型（'stable'/'aggressive'/'arbitrage'）
        返回:
            (是否满足条件, 原因说明, 操作类型)
        """
        try:
            # 1. 获取最新行情
            quote = self.data_fetcher.get_etf_quote(etf_code)
            if not quote:
                return False, "无行情数据", None
            latest = quote[-1]
            current_price = latest["close"]
            
            # 2. 计算收益/亏损率
            return_ratio = (current_price - buy_price) / buy_price
            
            # 3. 止盈条件（分类型）
            target_profit = 0.15 if position_type == "stable" else 0.25 if position_type == "aggressive" else 0.05
            if return_ratio >= target_profit:
                return True, f"达到止盈线{target_profit*100}%（当前收益{return_ratio*100:.2f}%）", "profit_take"
            
            # 4. 止损条件（分类型）
            stop_loss = -0.05 if position_type == "stable" else -0.08 if position_type == "aggressive" else -0.02
            if return_ratio <= stop_loss:
                return True, f"达到止损线{stop_loss*100}%（当前亏损{return_ratio*100:.2f}%）", "stop_loss"
            
            # 5. 技术面卖出条件：跌破20日均线且连续2日在下方
            ma20 = latest["ma20"] if "ma20" in latest else np.mean([day["close"] for day in quote[-20:]])
            if len(quote) >= 2:
                price_below_ma = (latest["close"] < ma20) and (quote[-2]["close"] < ma20)
                ma_trend_down = ma20 < quote[-3]["ma20"] if "ma20" in quote[-3] else False
                if price_below_ma and ma_trend_down:
                    return True, "价格连续2日跌破20日均线且均线走弱", "technical"
            
            return False, "未满足任何卖出条件", None
        except Exception as e:
            return False, f"检查基础卖出条件出错：{str(e)}", None

    def _check_liquidation_conditions(self, etf_code, position_type):
        """
        检查清仓条件（适用于部分清仓场景）
        参数:
            etf_code: ETF代码
            position_type: 仓位类型
        返回:
            (是否需要清仓, 原因说明)
        """
        try:
            # 1. 基本面恶化：成分股暴雷≥3只
            problematic_stocks = self.data_fetcher.get_etf_problematic_stocks(etf_code)
            if len(problematic_stocks) >= 3:
                return True, f"成分股暴雷{len(problematic_stocks)}只≥3只，触发清仓"
            
            # 2. 行业政策重大利空
            policies = self.data_fetcher.get_industry_policies(etf_code)
            for policy in policies:
                if policy["impact"] == "重大利空" and (datetime.now() - datetime.strptime(policy["date"], "%Y-%m-%d")).days <= 5:
                    return True, f"近5天内出现重大利空政策：{policy['title']}"
            
            # 3. 套利仓特殊条件：持仓超3日
            if position_type == "arbitrage":
                position = self.positions["arbitrage"]
                if position:
                    hold_days = (datetime.now() - datetime.strptime(position["open_date"], "%Y-%m-%d")).days
                    if hold_days >= 3:
                        return True, f"套利仓持仓{hold_days}天≥3天，强制清仓"
            
            return False, "未满足清仓条件"
        except Exception as e:
            return False, f"检查清仓条件出错：{str(e)}"

    # ------------------------------
    # 调仓机制相关方法
    # ------------------------------
    def _check_market_environment(self):
        """
        判断市场环境（牛/熊/震荡），用于跨类型调仓
        返回:
            市场环境标识（'bull'/'bear'/'shock'）
        """
        try:
            # 获取沪深300指数近1个月涨跌幅
            hs300_data = self.data_fetcher.get_index_quote("000300.SH")
            if len(hs300_data) < 22:  # 约1个月交易日
                return "shock"  # 数据不足默认震荡市
            
            # 计算月涨跌幅
            month_ago_price = hs300_data[-22]["close"]
            latest_price = hs300_data[-1]["close"]
            month_return = (latest_price - month_ago_price) / month_ago_price
            
            if month_return >= 0.05:
                return "bull"  # 牛市（月涨幅≥5%）
            elif month_return <= -0.05:
                return "bear"  # 熊市（月跌幅≥5%）
            else:
                return "shock"  # 震荡市
        except Exception as e:
            print(f"判断市场环境出错：{str(e)}")
            return "shock"  # 出错默认震荡市

    def _check_intra_position_switch(self, current_etf, candidates, position_type):
        """
        检查同类型调仓条件（同仓位内标的切换）
        参数:
            current_etf: 当前持仓ETF
            candidates: 候选ETF列表
            position_type: 仓位类型
        返回:
            (最佳替代标的, 调仓原因) 或 (None, 原因)
        """
        try:
            # 1. 检查当前持仓是否需要卖出
            sell, sell_reason, _ = self._check_basic_sell_conditions(
                current_etf["code"], current_etf["buy_price"], position_type
            )
            if not sell:
                return None, "当前持仓未满足卖出条件，无需调仓"
            
            # 2. 检查调仓频率：每月不超过3次
            current_month = datetime.now().month
            monthly_switches = [
                t for t in self.trade_history 
                if t["type"] == "switch" and t["position"] == position_type 
                and datetime.strptime(t["timestamp"], "%Y-%m-%dT%H:%M:%S").month == current_month
            ]
            if len(monthly_switches) >= 3:
                return None, f"本月已调仓{len(monthly_switches)}次≥3次，限制调仓"
            
            # 3. 从候选中寻找最佳替代标的
            best_candidate = None
            best_score = 0
            for candidate in candidates:
                if candidate["code"] == current_etf["code"]:
                    continue  # 跳过自身
                
                # 买入条件评分（100分制）
                buy, buy_reason = self._check_basic_buy_conditions(candidate["code"], position_type)
                if not buy:
                    continue
                
                # 额外评分项：流动性（20分）、估值优势（30分）
                liquidity_score = min(candidate["volume"] / 10000000, 20)  # 成交量越高分越高
                val_score = 30 if self.data_fetcher.get_etf_valuation(candidate["code"])["pe_percentile"] < 40 else 15
                
                total_score = 50 + liquidity_score + val_score  # 基础分50
                if total_score > best_score:
                    best_score = total_score
                    best_candidate = candidate
            
            if best_candidate:
                return best_candidate, f"当前持仓需卖出（{sell_reason}），找到更优替代标的"
            return None, "未找到符合条件的替代标的"
        except Exception as e:
            return None, f"检查同类型调仓条件出错：{str(e)}"

    # ------------------------------
    # 分仓评估方法
    # ------------------------------
    def evaluate_stable_position(self):
        """评估稳健仓操作策略（宽基ETF为主）"""
        # 获取当前稳健仓持仓
        current = self.positions["stable"]
        # 获取稳健仓候选ETF（宽基为主）
        candidates = self.etf_pool_manager.get_stable_candidates()
        # 导入稳健仓配置
        from config import STABLE_CAPITAL
        
        # 情况1：无持仓时，寻找最佳买入标的
        if not current:
            for candidate in candidates:
                # 检查基础买入条件
                buy, reason = self._check_basic_buy_conditions(candidate["code"], "stable")
                if buy:
                    return {
                        "action": "buy",
                        "etf": candidate,
                        "reason": reason,
                        "amount": STABLE_CAPITAL * 0.3,  # 首次买入30%
                        "position_ratio": 0.3
                    }
            return {"action": "hold", "reason": "无符合条件的稳健仓买入标的"}
        
        # 情况2：有持仓时，检查是否需要清仓
        liquidate, liquidate_reason = self._check_liquidation_conditions(current["code"], "stable")
        if liquidate:
            return {"action": "sell", "etf": current, "reason": liquidate_reason, "full_liquidation": True}
        
        # 情况3：检查是否需要卖出（基础卖出条件）
        sell, sell_reason, sell_type = self._check_basic_sell_conditions(
            current["code"], current["buy_price"], "stable"
        )
        if sell:
            # 稳健仓采用分批卖出策略
            if current["position_ratio"] > 0.3:  # 持仓比例较高时先卖部分
                return {
                    "action": "partial_sell",
                    "etf": current,
                    "reason": sell_reason,
                    "sell_ratio": 0.5,  # 先卖50%
                    "remaining_ratio": current["position_ratio"] * 0.5
                }
            else:  # 持仓比例较低时直接清仓
                return {"action": "sell", "etf": current, "reason": sell_reason}
        
        # 情况4：检查是否需要调仓（同类型切换）
        best_switch, switch_reason = self._check_intra_position_switch(
            current, candidates, "stable"
        )
        if best_switch:
            return {
                "action": "switch",
                "sell": current,
                "buy": best_switch,
                "reason": switch_reason,
                "amount": STABLE_CAPITAL * 0.3  # 新标的首次买入30%
            }
        
        # 情况5：检查是否需要加仓
        last_add_date = datetime.strptime(current.get("last_add_date", "2000-01-01"), "%Y-%m-%d")
        add, add_reason = self._check_add_position_conditions(current["code"], last_add_date)
        if add and current["position_ratio"] < 0.7:
            # 计算加仓比例（最多20%，不超过70%上限）
            add_ratio = min(0.2, 0.7 - current["position_ratio"])
            return {
                "action": "add",
                "etf": current,
                "reason": add_reason,
                "amount": STABLE_CAPITAL * add_ratio,
                "new_ratio": current["position_ratio"] + add_ratio,
                "last_add_date": datetime.now().strftime("%Y-%m-%d")
            }
        
        # 情况6：继续持有
        return {
            "action": "hold", 
            "etf": current, 
            "reason": "稳健仓持仓符合持有条件，无需操作"
        }

    def evaluate_aggressive_position(self):
        """评估激进仓操作策略（行业/主题ETF为主）"""
        # 获取当前激进仓持仓
        current = self.positions["aggressive"]
        # 获取激进仓候选ETF（行业/主题为主）
        candidates = self.etf_pool_manager.get_aggressive_candidates()
        # 导入激进仓配置
        from config import AGGRESSIVE_CAPITAL
        
        # 情况1：无持仓时，寻找最佳买入标的
        if not current:
            for candidate in candidates:
                # 检查基础买入条件
                buy, reason = self._check_basic_buy_conditions(candidate["code"], "aggressive")
                if buy:
                    return {
                        "action": "buy",
                        "etf": candidate,
                        "reason": reason,
                        "amount": AGGRESSIVE_CAPITAL * 0.2,  # 首次买入20%
                        "position_ratio": 0.2
                    }
            return {"action": "hold", "reason": "无符合条件的激进仓买入标的"}
        
        # 情况2：有持仓时，检查是否需要清仓
        liquidate, liquidate_reason = self._check_liquidation_conditions(current["code"], "aggressive")
        if liquidate:
            return {"action": "sell", "etf": current, "reason": liquidate_reason, "full_liquidation": True}
        
        # 情况3：检查是否需要卖出（基础卖出条件）
        sell, sell_reason, sell_type = self._check_basic_sell_conditions(
            current["code"], current["buy_price"], "aggressive"
        )
        if sell:
            # 激进仓采用一次性卖出策略
            return {"action": "sell", "etf": current, "reason": sell_reason}
        
        # 情况4：检查是否需要调仓（同类型切换）
        best_switch, switch_reason = self._check_intra_position_switch(
            current, candidates, "aggressive"
        )
        if best_switch:
            return {
                "action": "switch",
                "sell": current,
                "buy": best_switch,
                "reason": switch_reason,
                "amount": AGGRESSIVE_CAPITAL * 0.2  # 新标的首次买入20%
            }
        
        # 情况5：检查是否需要加仓
        last_add_date = datetime.strptime(current.get("last_add_date", "2000-01-01"), "%Y-%m-%d")
        add, add_reason = self._check_add_position_conditions(current["code"], last_add_date)
        if add and current["position_ratio"] < 0.6:
            # 计算加仓比例（最多15%，不超过60%上限）
            add_ratio = min(0.15, 0.6 - current["position_ratio"])
            return {
                "action": "add",
                "etf": current,
                "reason": add_reason,
                "amount": AGGRESSIVE_CAPITAL * add_ratio,
                "new_ratio": current["position_ratio"] + add_ratio,
                "last_add_date": datetime.now().strftime("%Y-%m-%d")
            }
        
        # 情况6：继续持有
        return {
            "action": "hold", 
            "etf": current, 
            "reason": "激进仓持仓符合持有条件，无需操作"
        }

    def evaluate_arbitrage_position(self):
        """评估套利仓操作策略（短期机会）"""
        # 当前套利仓持仓
        current = self.positions["arbitrage"]
        # 获取潜在套利机会（已实现文档三类套利）
        opportunities = self._check_arbitrage_opportunity()
        # 导入套利仓配置
        from config import ARBITRAGE_CAPITAL
        
        # 情况1：当前有套利持仓，检查是否需要平仓
        if current:
            # 检查清仓条件
            liquidate, liquidate_reason = self._check_liquidation_conditions(current["code"], "arbitrage")
            if liquidate:
                return {"action": "close", "etf": current["etf"], "reason": liquidate_reason}
            
            # 检查止盈（达到预期收益80%）
            current_price = self.data_fetcher.get_etf_real_time_data(current["etf"]["code"])["price"]
            return_ratio = (current_price - current["open_price"]) / current["open_price"]
            if return_ratio >= current["expected_return"] * 0.8:
                return {
                    "action": "close", 
                    "etf": current["etf"], 
                    "reason": f"达到预期收益80%（当前{return_ratio:.2%}）"
                }
            
            # 继续持有
            return {"action": "hold", "etf": current["etf"], "reason": "套利条件仍满足"}
        
        # 情况2：当前无持仓，寻找最佳套利机会
        if opportunities:
            best_opp = opportunities[0]
            # 单只标的投入不超过套利资金的30%
            invest_amount = min(ARBITRAGE_CAPITAL * 0.3, ARBITRAGE_CAPITAL)
            
            # 构建操作建议（按套利类型区分）
            if best_opp["type"] == "premium":
                return {
                    "action": best_opp["direction"],
                    "etf": best_opp["etf"],
                    "type": "premium",
                    "amount": invest_amount,
                    "expected_return": best_opp["expected_return"],
                    "reason": best_opp["reason"],
                    "open_date": datetime.now().strftime("%Y-%m-%d"),
                    "open_price": self.data_fetcher.get_etf_real_time_data(best_opp["etf"]["code"])["price"]
                }
            
            elif best_opp["type"] == "event":
                return {
                    "action": "buy",
                    "etf": best_opp["etf"],
                    "type": "event",
                    "amount": invest_amount,
                    "expected_return": best_opp["expected_return"],
                    "reason": best_opp["reason"],
                    "open_date": datetime.now().strftime("%Y-%m-%d"),
                    "open_price": self.data_fetcher.get_etf_real_time_data(best_opp["etf"]["code"])["price"]
                }
            
            elif best_opp["type"] == "cross_market":
                return {
                    "action": "pair_trade",
                    "main_etf": best_opp["main_etf"],
                    "related_etf": best_opp["related_etf"],
                    "type": "cross_market",
                    "amount": invest_amount / 2,  # 两边各投一半
                    "expected_return": best_opp["expected_return"],
                    "reason": best_opp["reason"],
                    "open_date": datetime.now().strftime("%Y-%m-%d")
                }
        
        # 情况3：无合适套利机会
        return {"action": "hold", "reason": "无符合条件的套利机会（预期收益≥0.3%）"}

    # ------------------------------
    # 套利机制复用（保持文档要求的三类套利）
    # ------------------------------
    def _check_premium_arbitrage(self, etf):
        """检查折溢价套利机会（文档3.1节）"""
        try:
            quote = self.data_fetcher.get_etf_real_time_data(etf["code"])
            if not quote or "iopv" not in quote or "price" not in quote:
                return None
            
            premium_rate = (quote["price"] - quote["iopv"]) / quote["iopv"]
            if abs(premium_rate) >= 0.01 and quote.get("volume", 0) >= 5000000:
                expected_return = abs(premium_rate) - 0.001  # 扣除手续费
                if expected_return > 0:
                    return {
                        "type": "premium",
                        "etf": etf,
                        "direction": "buy" if premium_rate < -0.01 else "sell",
                        "premium_rate": premium_rate,
                        "expected_return": expected_return,
                        "reason": f"折溢价率{premium_rate:.2%}"
                    }
            return None
        except Exception as e:
            print(f"折溢价套利检查出错: {str(e)}")
            return None

    def _check_event_arbitrage(self, etf):
        """检查事件驱动套利机会（文档3.2节）"""
        try:
            events = self.data_fetcher.get_etf_events(etf["code"])
            if not events:
                return None
            
            valid_events = []
            for event in events:
                event_date = datetime.strptime(event["date"], "%Y-%m-%d")
                if 0 <= (event_date - datetime.now()).days <= 3:
                    valid_events.append(event)
            
            if valid_events:
                event_priority = {"份额折算": 3, "分红": 2, "成分股调整": 1}
                valid_events.sort(key=lambda x: event_priority.get(x["type"], 0), reverse=True)
                best_event = valid_events[0]
                return {
                    "type": "event",
                    "etf": etf,
                    "event": best_event,
                    "expected_return": 0.015,
                    "reason": f"{best_event['date']} {best_event['type']}"
                }
            return None
        except Exception as e:
            print(f"事件套利检查出错: {str(e)}")
            return None

    def _check_cross_market_arbitrage(self, etf):
        """检查跨市场套利机会（文档3.3节）"""
        try:
            related_etfs = self.etf_pool_manager.get_related_etfs(etf["code"])
            if not related_etfs:
                return None
            
            main_data = self.data_fetcher.get_etf_real_time_data(etf["code"])
            if not main_data or "price" not in main_data:
                return None
            
            opportunities = []
            for related in related_etfs:
                related_data = self.data_fetcher.get_etf_real_time_data(related["code"])
                if not related_data or "price" not in related_data:
                    continue
                
                spread_rate = (main_data["price"] - related_data["price"]) / related_data["price"]
                if (abs(spread_rate) >= 0.005 and 
                    main_data.get("volume", 0) >= 3000000 and 
                    related_data.get("volume", 0) >= 3000000):
                    
                    opportunities.append({
                        "type": "cross_market",
                        "main_etf": etf,
                        "related_etf": related,
                        "spread_rate": spread_rate,
                        "expected_return": abs(spread_rate) - 0.002,
                        "reason": f"价差{spread_rate:.2%}"
                    })
            
            return max(opportunities, key=lambda x: x["expected_return"]) if opportunities else None
        except Exception as e:
            print(f"跨市场套利检查出错: {str(e)}")
            return None

    def _check_arbitrage_opportunity(self):
        """综合检查所有套利机会（文档3.4节）"""
        try:
            etf_pool = self.etf_pool_manager.get_etf_pool()
            all_opportunities = []
            
            for etf in etf_pool:
                premium_opp = self._check_premium_arbitrage(etf)
                if premium_opp:
                    all_opportunities.append((premium_opp, 3))  # 优先级3
                
                event_opp = self._check_event_arbitrage(etf)
                if event_opp:
                    all_opportunities.append((event_opp, 2))  # 优先级2
                
                cross_opp = self._check_cross_market_arbitrage(etf)
                if cross_opp:
                    all_opportunities.append((cross_opp, 1))  # 优先级1
            
            # 按优先级和预期收益排序
            all_opportunities.sort(key=lambda x: (x[1], x[0]["expected_return"]), reverse=True)
            valid_opps = [opp for opp, _ in all_opportunities if opp["expected_return"] >= 0.003]
            
            # 去重同类机会，最多保留3个
            filtered_opps = []
            seen_types = set()
            for opp in valid_opps:
                if opp["type"] not in seen_types or len(seen_types) < 3:
                    filtered_opps.append(opp)
                    seen_types.add(opp["type"])
            
            return filtered_opps[:3]
        except Exception as e:
            print(f"综合套利检查出错: {str(e)}")
            return []

    # ------------------------------
    # 策略执行与持仓更新
    # ------------------------------
    def execute_strategy(self):
        """执行完整策略评估，生成最终操作建议"""
        # 评估市场环境，决定是否需要跨类型调仓
        market_env = self._check_market_environment()
        print(f"当前市场环境: {market_env}")
        
        # 评估各仓位操作建议
        stable_action = self.evaluate_stable_position()
        aggressive_action = self.evaluate_aggressive_position()
        arbitrage_action = self.evaluate_arbitrage_position()
        
        # 生成综合建议
        strategy_result = {
            "timestamp": datetime.now().isoformat(),
            "market_environment": market_env,
            "stable": stable_action,
            "aggressive": aggressive_action,
            "arbitrage": arbitrage_action,
            "summary": self._generate_summary(stable_action, aggressive_action, arbitrage_action)
        }
        
        # 更新持仓并记录交易
        self._update_positions(stable_action, aggressive_action, arbitrage_action)
        
        return strategy_result

    def _generate_summary(self, stable, aggressive, arbitrage):
        """生成策略执行摘要（用于推送消息）"""
        summary = []
        if stable["action"] != "hold":
            summary.append(f"稳健仓: {stable['action']} {stable['etf']['code'] if 'etf' in stable else ''} ({stable['reason']})")
        if aggressive["action"] != "hold":
            summary.append(f"激进仓: {aggressive['action']} {aggressive['etf']['code'] if 'etf' in aggressive else ''} ({aggressive['reason']})")
        if arbitrage["action"] != "hold":
            summary.append(f"套利仓: {arbitrage['action']} {arbitrage['etf']['code'] if 'etf' in arbitrage else ''} ({arbitrage['reason']})")
        
        return "\n".join(summary) if summary else "所有仓位维持不变，无操作建议"

    def _update_positions(self, stable_action, aggressive_action, arbitrage_action):
        """更新持仓并记录交易"""
        # 处理稳健仓操作
        if stable_action["action"] == "buy":
            self.cache_manager.add_trade_record({
                "type": "buy",
                "position": "stable",
                "etf": stable_action["etf"],
                "amount": stable_action["amount"],
                "reason": stable_action["reason"],
                "timestamp": datetime.now().isoformat()
            })
            self.positions["stable"] = {
                **stable_action["etf"],
                "position_ratio": stable_action["position_ratio"],
                "buy_price": self.data_fetcher.get_etf_quote(stable_action["etf"]["code"])[-1]["close"],
                "buy_date": datetime.now().strftime("%Y-%m-%d")
            }
        
        elif stable_action["action"] in ["sell", "partial_sell"]:
            sell_type = "sell" if stable_action["action"] == "sell" else "partial_sell"
            self.cache_manager.add_trade_record({
                "type": sell_type,
                "position": "stable",
                "etf": stable_action["etf"],
                "amount": stable_action["etf"]["position_ratio"] * 12000 * (1 if sell_type == "sell" else 0.5),
                "reason": stable_action["reason"],
                "timestamp": datetime.now().isoformat()
            })
            if sell_type == "sell":
                self.positions["stable"] = None
            else:
                self.positions["stable"]["position_ratio"] = stable_action["remaining_ratio"]
        
        elif stable_action["action"] == "switch":
            self.cache_manager.add_trade_record({
                "type": "sell",
                "position": "stable",
                "etf": stable_action["sell"],
                "amount": stable_action["sell"]["position_ratio"] * 12000,
                "reason": stable_action["reason"],
                "timestamp": datetime.now().isoformat()
            })
            self.cache_manager.add_trade_record({
                "type": "buy",
                "position": "stable",
                "etf": stable_action["buy"],
                "amount": stable_action["amount"],
                "reason": stable_action["reason"],
                "timestamp": datetime.now().isoformat()
            })
            self.positions["stable"] = {
                **stable_action["buy"],
                "position_ratio": 0.3,
                "buy_price": self.data_fetcher.get_etf_quote(stable_action["buy"]["code"])[-1]["close"],
                "buy_date": datetime.now().strftime("%Y-%m-%d")
            }
        
        elif stable_action["action"] == "add":
            self.cache_manager.add_trade_record({
                "type": "add",
                "position": "stable",
                "etf": stable_action["etf"],
                "amount": stable_action["amount"],
                "reason": stable_action["reason"],
                "timestamp": datetime.now().isoformat()
            })
            self.positions["stable"]["position_ratio"] = stable_action["new_ratio"]
            self.positions["stable"]["last_add_date"] = stable_action["last_add_date"]
        
        # 处理激进仓操作（逻辑类似稳健仓）
        if aggressive_action["action"] == "buy":
            self.cache_manager.add_trade_record({
                "type": "buy",
                "position": "aggressive",
                "etf": aggressive_action["etf"],
                "amount": aggressive_action["amount"],
                "reason": aggressive_action["reason"],
                "timestamp": datetime.now().isoformat()
            })
            self.positions["aggressive"] = {
                **aggressive_action["etf"],
                "position_ratio": aggressive_action["position_ratio"],
                "buy_price": self.data_fetcher.get_etf_quote(aggressive_action["etf"]["code"])[-1]["close"],
                "buy_date": datetime.now().strftime("%Y-%m-%d")
            }
        
        elif aggressive_action["action"] == "sell":
            self.cache_manager.add_trade_record({
                "type": "sell",
                "position": "aggressive",
                "etf": aggressive_action["etf"],
                "amount": aggressive_action["etf"]["position_ratio"] * 8000,
                "reason": aggressive_action["reason"],
                "timestamp": datetime.now().isoformat()
            })
            self.positions["aggressive"] = None
        
        elif aggressive_action["action"] == "switch":
            self.cache_manager.add_trade_record({
                "type": "sell",
                "position": "aggressive",
                "etf": aggressive_action["sell"],
                "amount": aggressive_action["sell"]["position_ratio"] * 8000,
                "reason": aggressive_action["reason"],
                "timestamp": datetime.now().isoformat()
            })
            self.cache_manager.add_trade_record({
                "type": "buy",
                "position": "aggressive",
                "etf": aggressive_action["buy"],
                "amount": aggressive_action["amount"],
                "reason": aggressive_action["reason"],
                "timestamp": datetime.now().isoformat()
            })
            self.positions["aggressive"] = {
                **aggressive_action["buy"],
                "position_ratio": 0.2,
                "buy_price": self.data_fetcher.get_etf_quote(aggressive_action["buy"]["code"])[-1]["close"],
                "buy_date": datetime.now().strftime("%Y-%m-%d")
            }
        
        elif aggressive_action["action"] == "add":
            self.cache_manager.add_trade_record({
                "type": "add",
                "position": "aggressive",
                "etf": aggressive_action["etf"],
                "amount": aggressive_action["amount"],
                "reason": aggressive_action["reason"],
                "timestamp": datetime.now().isoformat()
            })
            self.positions["aggressive"]["position_ratio"] = aggressive_action["new_ratio"]
            self.positions["aggressive"]["last_add_date"] = aggressive_action["last_add_date"]
        
        # 处理套利仓操作
        if arbitrage_action["action"] in ["buy", "sell"]:
            self.cache_manager.add_trade_record({
                "type": arbitrage_action["action"],
                "position": "arbitrage",
                "etf": arbitrage_action["etf"],
                "amount": arbitrage_action["amount"],
                "reason": arbitrage_action["reason"],
                "timestamp": datetime.now().isoformat()
            })
            self.positions["arbitrage"] = {
                **arbitrage_action["etf"],
                "direction": arbitrage_action["action"],
                "open_price": arbitrage_action["open_price"],
                "open_date": arbitrage_action["open_date"],
                "expected_return": arbitrage_action["expected_return"]
            }
        
        elif arbitrage_action["action"] == "close":
            self.cache_manager.add_trade_record({
                "type": "close",
                "position": "arbitrage",
                "etf": arbitrage_action["etf"],
                "amount": 2000,  # 套利仓总资金
                "reason": arbitrage_action["reason"],
                "timestamp": datetime.now().isoformat()
            })
            self.positions["arbitrage"] = None
        
        # 保存更新后的持仓
        self.cache_manager.save_positions(self.positions)
    
