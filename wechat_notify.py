# -*- coding: utf-8 -*-
"""
企业微信推送：通过机器人推送交易信号
对应文档章节：5. 企业微信推送要求
"""
import requests
import json
import time

class WechatNotifier:
    def __init__(self):
        """初始化推送器（从配置获取Webhook）"""
        from config import WECHAT_WEBHOOK, MESSAGE_INTERVAL
        self.webhook_url = WECHAT_WEBHOOK  # 企业微信机器人地址
        self.message_interval = MESSAGE_INTERVAL  # 消息间隔（1分钟）
    
    def _send_request(self, data):
        """发送请求到企业微信机器人"""
        headers = {'Content-Type': 'application/json'}
        try:
            response = requests.post(
                self.webhook_url,
                headers=headers,
                data=json.dumps(data)
            )
            result = response.json()
            if result.get("errcode") != 0:
                raise Exception(f"推送失败: {result.get('errmsg')}")
            print("消息推送成功")
            return True
        except Exception as e:
            print(f"推送请求出错: {str(e)}")
            return False
    
    def send_text_message(self, content):
        """
        发送文本消息（带北京时间前缀）
        对应文档章节：7. 消息格式要求（CF系统时间开头）
        """
        from utils import utc_to_beijing, format_time
        beijing_time = utc_to_beijing()  # 获取当前北京时间
        time_prefix = f"CF系统时间：{format_time(beijing_time)}\n"  # 时间前缀
        full_content = time_prefix + content  # 完整消息内容
        
        # 构建企业微信消息格式
        data = {
            "msgtype": "text",
            "text": {"content": full_content}
        }
        
        return self._send_request(data)
    
    def send_batch_messages(self, messages):
        """
        批量发送消息（每条间隔1分钟）
        对应文档章节：13. 逐条推送规则
        """
        results = []
        for i, msg in enumerate(messages):
            try:
                # 发送单条消息
                success = self.send_text_message(msg)
                results.append({
                    "index": i,
                    "success": success,
                    "message": msg
                })
                
                # 非最后一条消息需等待
                if i < len(messages) - 1:
                    print(f"等待{self.message_interval}秒发送下一条...")
                    time.sleep(self.message_interval)
            except Exception as e:
                results.append({
                    "index": i,
                    "success": False,
                    "message": msg,
                    "error": str(e)
                })
                if i < len(messages) - 1:
                    time.sleep(self.message_interval)
        
        return results