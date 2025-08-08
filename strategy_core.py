# -*- coding: utf-8 -*-
"""
策略核心逻辑：实现买卖信号判断、仓位管理、套利机会识别等核心规则
对应文档章节：步骤1：明确鱼盆策略核心规则（包含基础资金与仓位划分、买卖信号、套利机制等）
"""
from datetime import datetime

class StrategyCore:
    def __init__(self, data_fetcher, cache_manager, etf_pool_manager):
        """
        初始化策略核心组件
        参数:
            data_fetcher: 数据获取器实例（用于获取ETF行情和基本信息）
            cache_manager: 缓存管理器实例（用于读写持仓、交易记录等数据）
            etf_pool_manager: ETF池管理器实例（用于获取候选ETF标的）
        对应文档章节：策略整体逻辑整合需求
        """
        self.data_fetcher = data_fetcher
        self.cache_manager = cache_manager
        self.etf_pool_manager = etf_pool_manager
        
        # 加载当前持仓状态（稳健仓、激进仓、套利仓）
        self.positions = self.cache_manager.load_positions()
        print("已加载当前持仓信息")

    def _check_buy_signal(self, etf_code):
        """
        检查ETF买入信号（严格遵循文档规则）
        参数:
            etf_code: ETF代码
        返回:
            (是否符合买入条件, 原因说明)
        对应文档章节：2. 原有买卖信号与仓位管理 - 买入信号
        """
        try:
            # 获取ETF行情数据（至少需要20天数据计算均线）
            quote = self.data_fetcher.get_etf_quote(etf_code)
            if len(quote) < 20:
                return False, "数据不足（需至少20天行情数据）"
            
            # 计算20日均线（若数据源未直接提供）
            if "20日均线" not in quote[-1]:
                closes = [day.get("close", 0) for day in quote]
                for i in range(19, len(quote)):
                    quote[i]["20日均线"] = sum(closes[i-19:i+1]) / 20  # 20日收盘价平均值
            
            # 计算5日均量（若数据源未直接提供）
            if "5日均量" not in quote[-1]:
                volumes = [day.get("volume", 0) for day in quote]
                for i in range(4, len(quote)):
                    quote[i]["5日均量"] = sum(volumes[i-4:i+1]) / 5  # 5日成交量平均值
            
            latest = quote[-1]  # 最新交易日数据
            prev_day = quote[-2]  # 前一交易日数据
            
            # 基础条件1：价格突破20日均线（当日收盘价>20日均线，前一日收盘价≤20日均线）
            price_break = (latest["close"] > latest["20日均线"] and 
                          prev_day["close"] <= prev_day["20日均线"])
            
            # 基础条件2：20日均线呈上升趋势（最新20日均线 > 20天前的20日均线）
            ma_trend = latest["20日均线"] > quote[-20]["20日均线"]
            
            if not (price_break and ma_trend):
                return False, "未满足买入基础条件（价格突破20日均线且均线上升）"
            
            # 辅助条件1：突破持续2-3天（近3天内有2天收盘价在20日均线上方）
            valid_days = sum(1 for day in quote[-3:] if day["close"] > day["20日均线"])
            if valid_days < 2:
                return False, "突破未持续2-3天（近3天内不足2天在均线上方）"
            
            # 辅助条件2：成交量放大20%以上（当日成交量 > 5日均量的1.2倍）
            if latest["volume"] <= latest["5日均量"] * 1.2:
                return False, "成交量未放大20%以上（未达5日均量的1.2倍）"
            
            return True, "符合买入条件（价格突破均线+量能放大+趋势确认）"
        except Exception as e:
            return False, f"检查买入信号出错: {str(e)}"

    def _check_sell_signal(self, etf_code, current_price=None):
        """
        检查ETF卖出信号（严格遵循文档规则）
        参数:
            etf_code: ETF代码
            current_price: 当前价格（可选，默认用最新收盘价）
        返回:
            (是否符合卖出条件, 原因说明)
        对应文档章节：2. 原有买卖信号与仓位管理 - 卖出信号
        """
        try:
            # 获取ETF行情数据（至少需要20天数据计算均线）
            quote = self.data_fetcher.get_etf_quote(etf_code)
            if len(quote) < 20:
                return False, "数据不足（需至少20天行情数据）"
            
            # 计算20日均线（若数据源未直接提供）
            if "20日均线" not in quote[-1]:
                closes = [day.get("close", 0) for day in quote]
                for i in range(19, len(quote)):
                    quote[i]["20日均线"] = sum(closes[i-19:i+1]) / 20
            
            latest = quote[-1]  # 最新交易日数据
            price = current_price if current_price else latest["close"]  # 优先用当前价格
            
            # 基础条件1：价格跌破20日均线（当前价格<20日均线，最新收盘价≥20日均线）
            price_break = (price < latest["20日均线"] and 
                          latest["close"] >= latest["20日均线"])
            
            # 基础条件2：20日均线开始下降（最新20日均线 < 前一日20日均线）
            ma_turn = latest["20日均线"] < quote[-2]["20日均线"]
            
            if price_break or ma_turn:
                reason = "价格跌破20日均线" if price_break else "20日均线开始下降"
                return True, reason
            
            # 辅助条件：出现M头顶部形态（近5天形成高点-低点-高点-低点的形态）
            if len(quote) >= 5:
                day5, day4, day3, day2, day1 = quote[-5], quote[-4], quote[-3], quote[-2], latest
                if (day5["high"] < day4["high"] and 
                    day4["high"] > day3["high"] and 
                    day3["high"] < day2["high"] and 
                    day2["high"] > day1["high"]):
                    return True, "出现M头顶部形态（见顶信号）"
            
            return False, "未满足卖出条件（价格在均线上方且均线趋势向上）"
        except Exception as e:
            return False, f"检查卖出信号出错: {str(e)}"

    def _check_arbitrage_opportunity(self):
        """
        检查套利机会（严格遵循文档规则）
        返回:
            套利机会列表（按预期收益排序）
        对应文档章节：3. 新增套利仓规则 - 套利机会监测
        """
        try:
            etf_pool = self.etf_pool_manager.get_etf_pool()  # 获取当前ETF池
            opportunities = []
            
            for etf in etf_pool:
                try:
                    # 获取近2天行情数据（判断短期波动）
                    quote = self.data_fetcher.get_etf_quote(etf["code"])
                    if len(quote) < 2:
                        continue  # 数据不足跳过
                    
                    latest = quote[-1]  # 最新数据
                    prev_day = quote[-2]  # 前一天数据
                    
                    # 计算价格波动幅度（绝对值）
                    price_change = abs(latest["close"] - prev_day["close"]) / prev_day["close"]
                    
                    # 波动超过3%视为潜在套利机会（文档建议阈值）
                    if price_change > 0.03:
                        opportunities.append({
                            "etf": etf,
                            "reason": f"短期价格波动异常（{price_change:.2%}）",
                            "expected_return": min(0.02, price_change * 0.5),  # 预期收益上限2%
                            "direction": "buy" if latest["close"] < prev_day["close"] else "sell"
                        })
                except Exception as e:
                    print(f"检查{etf['code']}套利机会出错: {str(e)}")
                    continue
            
            # 按预期收益降序排序，取前2个机会
            opportunities.sort(key=lambda x: x["expected_return"], reverse=True)
            return opportunities[:2]
        except Exception as e:
            print(f"检查套利机会总出错: {str(e)}")
            return []

    def evaluate_stable_position(self):
        """
        评估稳健仓操作策略（宽基ETF为主）
        返回:
            操作建议字典（包含动作、标的、原因、金额等）
        对应文档章节：1. 基础资金与仓位划分 - 稳健仓（占比60%）
        """
        current = self.positions["stable"]  # 当前稳健仓持仓
        candidates = self.etf_pool_manager.get_stable_candidates()  # 稳健仓候选ETF（宽基为主）
        
        # 情况1：无持仓时，寻找最佳买入标的
        if not current:
            best_candidate = candidates[0] if candidates else None
            if best_candidate:
                buy, reason = self._check_buy_signal(best_candidate["code"])
                if buy:
                    from config import STABLE_CAPITAL
                    return {
                        "action": "buy",
                        "etf": best_candidate,
                        "reason": reason,
                        "amount": STABLE_CAPITAL * 0.3  # 首次买入30%仓位
                    }
            return {"action": "hold", "reason": "无符合条件的稳健仓买入标的"}
        
        # 情况2：有持仓时，检查是否需要卖出
        sell, reason = self._check_sell_signal(current["code"])
        if sell:
            # 寻找替代标的（优先宽基ETF）
            for candidate in candidates:
                if candidate["code"] != current["code"]:  # 排除当前持仓
                    buy, buy_reason = self._check_buy_signal(candidate["code"])
                    if buy:
                        return {
                            "action": "switch",
                            "sell": current,
                            "buy": candidate,
                            "reason": f"卖出原因: {reason}; 买入原因: {buy_reason}"
                        }
            # 无替代标的则仅卖出
            return {"action": "sell", "etf": current, "reason": reason}
        
        # 情况3：无需卖出时，检查是否需要加仓（总仓位≤70%）
        current_ratio = current.get("position_ratio", 0)
        if current_ratio < 0.7:
            from config import STABLE_CAPITAL
            add_amount = STABLE_CAPITAL * (0.2 if current_ratio + 0.2 <= 0.7 else 0.7 - current_ratio)
            return {
                "action": "add",
                "etf": current,
                "reason": f"当前仓位{current_ratio*100}%，未达70%上限",
                "amount": add_amount
            }
        
        # 情况4：继续持有
        return {"action": "hold", "etf": current, "reason": "符合持有条件（价格在均线上方且趋势向上）"}

    def evaluate_aggressive_position(self):
        """
        评估激进仓操作策略（行业/主题ETF为主）
        返回:
            操作建议字典（包含动作、标的、原因、金额等）
        对应文档章节：1. 基础资金与仓位划分 - 激进仓（占比40%）
        """
        current = self.positions["aggressive"]  # 当前激进仓持仓
        candidates = self.etf_pool_manager.get_aggressive_candidates()  # 激进仓候选ETF（行业/主题为主）
        
        # 情况1：无持仓时，寻找最佳买入标的
        if not current:
            best_candidate = candidates[0] if candidates else None
            if best_candidate:
                buy, reason = self._check_buy_signal(best_candidate["code"])
                if buy:
                    from config import AGGRESSIVE_CAPITAL
                    return {
                        "action": "buy",
                        "etf": best_candidate,
                        "reason": reason,
                        "amount": AGGRESSIVE_CAPITAL * 0.3  # 首次买入30%仓位
                    }
            return {"action": "hold", "reason": "无符合条件的激进仓买入标的"}
        
        # 情况2：有持仓时，检查是否需要卖出
        sell, reason = self._check_sell_signal(current["code"])
        if sell:
            # 寻找替代标的（优先行业/主题ETF）
            for candidate in candidates:
                if candidate["code"] != current["code"]:  # 排除当前持仓
                    buy, buy_reason = self._check_buy_signal(candidate["code"])
                    if buy:
                        return {
                            "action": "switch",
                            "sell": current,
                            "buy": candidate,
                            "reason": f"卖出原因: {reason}; 买入原因: {buy_reason}"
                        }
            # 无替代标的则仅卖出
            return {"action": "sell", "etf": current, "reason": reason}
        
        # 情况3：无需卖出时，检查是否需要加仓（总仓位≤70%）
        current_ratio = current.get("position_ratio", 0)
        if current_ratio < 0.7:
            from config import AGGRESSIVE_CAPITAL
            add_amount = AGGRESSIVE_CAPITAL * (0.2 if current_ratio + 0.2 <= 0.7 else 0.7 - current_ratio)
            return {
                "action": "add",
                "etf": current,
                "reason": f"当前仓位{current_ratio*100}%，未达70%上限",
                "amount": add_amount
            }
        
        # 情况4：继续持有
        return {"action": "hold", "etf": current, "reason": "符合持有条件（价格在均线上方且趋势向上）"}

    def evaluate_arbitrage_position(self):
        """
        评估套利仓操作策略（短期机会）
        返回:
            操作建议字典（包含动作、标的、原因、金额等）
        对应文档章节：3. 新增套利仓规则（占比10%）
        """
        current = self.positions["arbitrage"]  # 当前套利仓持仓
        opportunities = self._check_arbitrage_opportunity()  # 潜在套利机会
        
        # 情况1：有持仓时，检查是否需要平仓
        if current:
            # 检查持仓是否仍有套利价值
            is_still_valid = any(
                opp["etf"]["code"] == current["code"] and opp["expected_return"] > 0.005
                for opp in opportunities
            )
            if not is_still_valid:
                return {"action": "close", "etf": current, "reason": "套利机会消失或收益不足0.5%"}
            return {"action": "hold", "etf": current, "reason": "套利机会仍有效"}
        
        # 情况2：无持仓时，寻找最佳套利机会
        if opportunities:
            best_opp = opportunities[0]
            from config import ARBITRAGE_CAPITAL
            return {
                "action": best_opp["direction"],
                "etf": best_opp["etf"],
                "reason": best_opp["reason"],
                "amount": ARBITRAGE_CAPITAL,  # 全额投入套利仓资金
                "expected_return": f"{best_opp['expected_return']*100:.2f}%"
            }
        
        # 情况3：无套利机会
        return {"action": "hold", "reason": "无符合条件的套利机会"}

    def execute_strategy(self):
        """
        执行完整策略评估，生成最终操作建议
        返回:
            综合操作建议字典
        对应文档章节：步骤1：明确鱼盆策略核心规则（整体执行流程）
        """
        # 评估各仓位操作建议
        stable_action = self.evaluate_stable_position()
        aggressive_action = self.evaluate_aggressive_position()
        arbitrage_action = self.evaluate_arbitrage_position()
        
        # 生成综合建议
        strategy_result = {
            "timestamp": datetime.now().isoformat(),
            "stable": stable_action,
            "aggressive": aggressive_action,
            "arbitrage": arbitrage_action,
            "summary": self._generate_summary(stable_action, aggressive_action, arbitrage_action)
        }
        
        # 更新持仓并记录交易（模拟操作，实际交易需人工确认）
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
        """更新持仓并记录交易（模拟操作）"""
        # 处理稳健仓操作
        if stable_action["action"] == "buy":
            # 记录买入交易
            self.cache_manager.add_trade_record({
                "type": "buy",
                "position": "stable",
                "etf": stable_action["etf"],
                "amount": stable_action["amount"],
                "reason": stable_action["reason"]
            })
            # 更新持仓（首次买入仓位30%）
            self.positions["stable"] = {
                **stable_action["etf"],
                "position_ratio": 0.3,
                "buy_price": self.data_fetcher.get_etf_quote(stable_action["etf"]["code"])[-1]["close"]
            }
        
        elif stable_action["action"] == "sell":
            # 记录卖出交易
            self.cache_manager.add_trade_record({
                "type": "sell",
                "position": "stable",
                "etf": stable_action["etf"],
                "amount": stable_action["etf"]["position_ratio"] * 12000,  # 12000为稳健仓总资金
                "reason": stable_action["reason"]
            })
            # 清空持仓
            self.positions["stable"] = None
        
        elif stable_action["action"] == "switch":
            # 记录卖出交易
            self.cache_manager.add_trade_record({
                "type": "sell",
                "position": "stable",
                "etf": stable_action["sell"],
                "amount": stable_action["sell"]["position_ratio"] * 12000,
                "reason": stable_action["reason"]
            })
            # 记录买入交易
            self.cache_manager.add_trade_record({
                "type": "buy",
                "position": "stable",
                "etf": stable_action["buy"],
                "amount": 12000 * 0.3,
                "reason": stable_action["reason"]
            })
            # 更新持仓
            self.positions["stable"] = {
                **stable_action["buy"],
                "position_ratio": 0.3,
                "buy_price": self.data_fetcher.get_etf_quote(stable_action["buy"]["code"])[-1]["close"]
            }
        
        elif stable_action["action"] == "add":
            # 记录加仓交易
            self.cache_manager.add_trade_record({
                "type": "add",
                "position": "stable",
                "etf": stable_action["etf"],
                "amount": stable_action["amount"],
                "reason": stable_action["reason"]
            })
            # 更新持仓比例
            self.positions["stable"]["position_ratio"] = min(
                self.positions["stable"]["position_ratio"] + 0.2, 0.7
            )
        
        # 处理激进仓操作（逻辑与稳健仓一致）
        if aggressive_action["action"] == "buy":
            self.cache_manager.add_trade_record({
                "type": "buy",
                "position": "aggressive",
                "etf": aggressive_action["etf"],
                "amount": aggressive_action["amount"],
                "reason": aggressive_action["reason"]
            })
            self.positions["aggressive"] = {
                **aggressive_action["etf"],
                "position_ratio": 0.3,
                "buy_price": self.data_fetcher.get_etf_quote(aggressive_action["etf"]["code"])[-1]["close"]
            }
        
        elif aggressive_action["action"] == "sell":
            self.cache_manager.add_trade_record({
                "type": "sell",
                "position": "aggressive",
                "etf": aggressive_action["etf"],
                "amount": aggressive_action["etf"]["position_ratio"] * 8000,  # 8000为激进仓总资金
                "reason": aggressive_action["reason"]
            })
            self.positions["aggressive"] = None
        
        elif aggressive_action["action"] == "switch":
            self.cache_manager.add_trade_record({
                "type": "sell",
                "position": "aggressive",
                "etf": aggressive_action["sell"],
                "amount": aggressive_action["sell"]["position_ratio"] * 8000,
                "reason": aggressive_action["reason"]
            })
            self.cache_manager.add_trade_record({
                "type": "buy",
                "position": "aggressive",
                "etf": aggressive_action["buy"],
                "amount": 8000 * 0.3,
                "reason": aggressive_action["reason"]
            })
            self.positions["aggressive"] = {
                **aggressive_action["buy"],
                "position_ratio": 0.3,
                "buy_price": self.data_fetcher.get_etf_quote(aggressive_action["buy"]["code"])[-1]["close"]
            }
        
        elif aggressive_action["action"] == "add":
            self.cache_manager.add_trade_record({
                "type": "add",
                "position": "aggressive",
                "etf": aggressive_action["etf"],
                "amount": aggressive_action["amount"],
                "reason": aggressive_action["reason"]
            })
            self.positions["aggressive"]["position_ratio"] = min(
                self.positions["aggressive"]["position_ratio"] + 0.2, 0.7
            )
        
        # 处理套利仓操作
        if arbitrage_action["action"] in ["buy", "sell"]:
            self.cache_manager.add_trade_record({
                "type": arbitrage_action["action"],
                "position": "arbitrage",
                "etf": arbitrage_action["etf"],
                "amount": arbitrage_action["amount"],
                "reason": arbitrage_action["reason"]
            })
            self.positions["arbitrage"] = {
                **arbitrage_action["etf"],
                "direction": arbitrage_action["action"],
                "open_price": self.data_fetcher.get_etf_quote(arbitrage_action["etf"]["code"])[-1]["close"]
            }
        
        elif arbitrage_action["action"] == "close":
            self.cache_manager.add_trade_record({
                "type": "close",
                "position": "arbitrage",
                "etf": arbitrage_action["etf"],
                "amount": 2000,  # 2000为套利仓总资金
                "reason": arbitrage_action["reason"]
            })
            self.positions["arbitrage"] = None
        
        # 保存更新后的持仓
        self.cache_manager.save_positions(self.positions)
