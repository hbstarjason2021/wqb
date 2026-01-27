#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
已提交Alpha数据爬虫
用于爬取WorldQuant平台上已提交的Alpha数据并存储到数据库中
"""

import json
import logging
import time
from typing import Optional, Dict, List, Tuple
from datetime import datetime
import requests
from mysql.connector import Error
import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from alpha_crawler import AlphaCrawler

# 配置日志
# 确保log目录存在
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'log')
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'submitted_alpha_crawler.log'), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class SubmittedAlphaCrawler(AlphaCrawler):
    """已提交Alpha数据爬虫类"""
    
    def __init__(self):
        """初始化已提交Alpha数据爬虫"""
        super().__init__()
        # 已提交Alpha数据的API端点
        self.submitted_base_url = f"{self.base_url}/users/self/alphas"
        
    def get_submitted_alphas_page(self, limit: int = 100, offset: int = 0) -> Optional[Dict]:
        """获取一页已提交Alpha数据"""
        if not self.is_authenticated:
            logger.error("未认证，请先调用authenticate方法")
            return None
        
        try:
            # 构建已提交Alpha数据的API URL
            # 使用status!=UNSUBMITTED%1FIS-FAIL过滤条件排除未提交和失败的Alpha
            api_url = (f"{self.submitted_base_url}?limit={limit}&offset={offset}"
                      "&status!=UNSUBMITTED%1FIS-FAIL&order=-dateSubmitted&hidden=false")
            
            logger.info(f"请求已提交Alpha数据URL: {api_url}")
            logger.info(f"请求第 {offset//limit + 1} 页数据，offset: {offset}")
            
            # 添加重试机制
            max_retries = 30
            retry_delay = 10  # 秒
            
            for attempt in range(max_retries):
                try:
                    # 构建请求头
                    headers = {
                        'Accept': 'application/json, text/plain, */*',
                        'Accept-Language': 'zh-CN,zh;q=0.9',
                        'Cache-Control': 'no-cache',
                        'Pragma': 'no-cache',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
                    }
                    
                    response = self.session.get(api_url, headers=headers, timeout=30)
                    
                    if response.status_code == 200:
                        data = response.json()
                        count = data.get('count', 0)
                        results_count = len(data.get('results', []))
                        logger.info(f"获取已提交Alpha数据成功，总数: {count}, 本页数量: {results_count}")
                        return data
                    elif response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", retry_delay))
                        logger.warning(f"API调用频率限制，{retry_after}秒后重试...")
                        time.sleep(retry_after)
                        continue
                    else:
                        logger.error(f"获取已提交Alpha数据失败，状态码: {response.status_code}")
                        logger.error(f"响应内容: {response.text[:500]}")
                        if attempt < max_retries - 1:
                            logger.info(f"{retry_delay}秒后重试...")
                            time.sleep(retry_delay)
                            continue
                        else:
                            return None
                            
                except Exception as e:
                    logger.error(f"获取已提交Alpha数据请求异常 (尝试 {attempt + 1}/{max_retries}): {e}")
                    if attempt < max_retries - 1:
                        logger.info(f"{retry_delay}秒后重试...")
                        time.sleep(retry_delay)
                        continue
                    else:
                        return None
            
            return None
                
        except Exception as e:
            logger.error(f"获取已提交Alpha数据请求异常: {e}")
            return None
    
    def parse_datetime(self, datetime_str: Optional[str]) -> Optional[str]:
        """解析日期时间，转换为北京时间并返回MySQL兼容的字符串格式"""
        if not datetime_str:
            return None
        
        try:
            # 处理ISO 8601格式，包含时区信息
            if 'T' in datetime_str:
                # 使用dateutil.parser处理各种ISO格式
                from dateutil import parser
                from dateutil import tz
                dt = parser.isoparse(datetime_str)
                # 如果没有时区信息，假设为UTC
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=tz.tzutc())
                # 转换为北京时间 (UTC+8)
                beijing_tz = tz.gettz('Asia/Shanghai')
                dt_beijing = dt.astimezone(beijing_tz)
                # 转换为MySQL兼容的datetime格式
                return dt_beijing.strftime('%Y-%m-%d %H:%M:%S')
            elif ' ' in datetime_str:
                # 处理"2025-07-24 09:43:18"这样的格式
                dt = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                # 纯日期格式，转换为datetime格式
                dt = datetime.strptime(datetime_str, '%Y-%m-%d')
                return dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            logger.warning(f"日期时间解析失败: {datetime_str}, 错误: {e}")
            return None
    
    def parse_date(self, date_str: Optional[str]) -> Optional[str]:
        """解析日期，返回MySQL兼容的日期字符串格式"""
        if not date_str:
            return None
        
        try:
            # 解析日期并返回标准格式
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            return dt.strftime('%Y-%m-%d')
        except Exception as e:
            logger.warning(f"日期解析失败: {date_str}, 错误: {e}")
            return None

    def parse_boolean(self, value: Optional[str]) -> Optional[bool]:
        """解析布尔值，将字符串转换为布尔类型"""
        if value is None:
            return None
        
        if isinstance(value, bool):
            return value
        
        if isinstance(value, str):
            value = value.upper()
            if value in ['TRUE', 'ENABLED', 'ON', 'YES', '1']:
                return True
            elif value in ['FALSE', 'DISABLED', 'OFF', 'NO', '0']:
                return False
        
        return None

    def parse_submitted_alpha_data(self, alpha_data: Dict) -> Optional[Dict]:
        """解析已提交Alpha数据"""
        try:
            # 处理name字段为空的情况，直接填入"anonymous"
            alpha_name = alpha_data.get('name')
            if not alpha_name:
                alpha_name = "anonymous"
            
            # 基本信息
            parsed_data = {
                'id': alpha_data.get('id', 'NULL'),
                'type': alpha_data.get('type', 'NULL'),
                'author': alpha_data.get('author', 'NULL'),
                'date_created': self.parse_datetime(alpha_data.get('dateCreated')),
                'date_submitted': self.parse_datetime(alpha_data.get('dateSubmitted')),
                'date_modified': self.parse_datetime(alpha_data.get('dateModified')),
                'name': alpha_name,
                'favorite': alpha_data.get('favorite', False),
                'hidden': alpha_data.get('hidden', False),
                'color': alpha_data.get('color', 'NULL'),
                'category': alpha_data.get('category', 'NULL'),
                'stage': alpha_data.get('stage', 'NULL'),
                'status': alpha_data.get('status', 'NULL'),
                'grade': alpha_data.get('grade', 'NULL'),
            }
            
            # 设置信息
            settings = alpha_data.get('settings', {})
            parsed_data.update({
                'instrument_type': settings.get('instrumentType', 'NULL'),
                'region': settings.get('region', 'NULL'),
                'universe': settings.get('universe', 'NULL'),
                'delay': settings.get('delay', 'NULL'),
                'decay': settings.get('decay', 'NULL'),
                'neutralization': settings.get('neutralization', 'NULL'),
                'truncation': settings.get('truncation', 'NULL'),
                'pasteurization': settings.get('pasteurization', 'NULL'),
                'unit_handling': settings.get('unitHandling', 'NULL'),
                'nan_handling': settings.get('nanHandling', 'NULL'),
                'selection_handling': settings.get('selectionHandling', 'NULL'),  # SUPER类型特有字段
                'selection_limit': settings.get('selectionLimit', 'NULL'),         # SUPER类型特有字段
                'max_trade': settings.get('maxTrade', 'NULL'),
                'language': settings.get('language', 'NULL'),
                'visualization': self.parse_boolean(settings.get('visualization')),
                'start_date': self.parse_date(settings.get('startDate')),
                'end_date': self.parse_date(settings.get('endDate')),
                'component_activation': settings.get('componentActivation', 'NULL'),  # SUPER类型特有字段
                'test_period': settings.get('testPeriod', 'NULL'),                   # SUPER类型特有字段
            })
            
            # 常规信息或SUPER类型信息
            if alpha_data.get('type') == 'REGULAR':
                regular = alpha_data.get('regular', {})
                parsed_data.update({
                    'code': regular.get('code', 'NULL'),
                    'description': regular.get('description', 'NULL'),
                    'operator_count': regular.get('operatorCount', 'NULL'),
                })
            elif alpha_data.get('type') == 'SUPER':
                # 处理SUPER类型Alpha的combo和selection信息
                combo = alpha_data.get('combo', {})
                selection = alpha_data.get('selection', {})
                
                # 合并策略：将combo和selection的信息都保存下来
                # 代码字段合并：[combo_code: XXXX, selection_code: xxxxx]
                combo_code = combo.get('code')
                selection_code = selection.get('code')
                if combo_code and selection_code:
                    code = f"[combo_code: {combo_code}, selection_code: {selection_code}]"
                elif combo_code:
                    code = f"[combo_code: {combo_code}]"
                elif selection_code:
                    code = f"[selection_code: {selection_code}]"
                else:
                    code = 'NULL'
                
                # 描述字段合并：[combo_description: XXXX, selection_description: xxxxx]
                combo_description = combo.get('description')
                selection_description = selection.get('description')
                if combo_description and selection_description:
                    description = f"[combo_description: {combo_description}, selection_description: {selection_description}]"
                elif combo_description:
                    description = f"[combo_description: {combo_description}]"
                elif selection_description:
                    description = f"[selection_description: {selection_description}]"
                else:
                    description = 'NULL'
                
                # 操作符计数合并：[combo_operator_count: X, selection_operator_count: Y]
                combo_operator_count = combo.get('operatorCount')
                selection_operator_count = selection.get('operatorCount')
                if combo_operator_count is not None and selection_operator_count is not None:
                    operator_count = f"[combo_operator_count: {combo_operator_count}, selection_operator_count: {selection_operator_count}]"
                elif combo_operator_count is not None:
                    operator_count = f"[combo_operator_count: {combo_operator_count}]"
                elif selection_operator_count is not None:
                    operator_count = f"[selection_operator_count: {selection_operator_count}]"
                else:
                    operator_count = 'NULL'
                
                parsed_data.update({
                    'code': code,
                    'description': description,
                    'operator_count': operator_count,
                    # 新增combo和selection的完整信息
                    'combo_code': combo.get('code', 'NULL'),
                    'combo_description': combo.get('description', 'NULL'),
                    'combo_operator_count': combo.get('operatorCount', 'NULL'),
                    'selection_code': selection.get('code', 'NULL'),
                    'selection_description': selection.get('description', 'NULL'),
                    'selection_operator_count': selection.get('operatorCount', 'NULL'),
                })
            
            # 标签信息
            tags = alpha_data.get('tags', [])
            parsed_data['tags'] = json.dumps(tags, ensure_ascii=False) if tags else 'NULL'
            
            # 分类信息
            classifications = alpha_data.get('classifications', [])
            parsed_data['classifications'] = json.dumps(classifications, ensure_ascii=False) if classifications else 'NULL'
            
            # IS阶段性能指标
            is_data = alpha_data.get('is', {})
            parsed_data.update({
                'pnl': is_data.get('pnl', 'NULL'),
                'book_size': is_data.get('bookSize', 'NULL'),
                'long_count': is_data.get('longCount', 'NULL'),
                'short_count': is_data.get('shortCount', 'NULL'),
                'turnover': is_data.get('turnover', 'NULL'),
                'returns': is_data.get('returns', 'NULL'),
                'drawdown': is_data.get('drawdown', 'NULL'),
                'margin': is_data.get('margin', 'NULL'),
                'sharpe': is_data.get('sharpe', 'NULL'),
                'fitness': is_data.get('fitness', 'NULL'),
                'is_start_date': self.parse_date(is_data.get('startDate')),
                'self_correlation': is_data.get('selfCorrelation', 'NULL'),
                'prod_correlation': is_data.get('prodCorrelation', 'NULL'),
            })
            
            # 投资约束性能指标
            invest_constrained = is_data.get('investabilityConstrained', {})
            parsed_data.update({
                'investability_constrained_pnl': invest_constrained.get('pnl', 'NULL'),
                'investability_constrained_book_size': invest_constrained.get('bookSize', 'NULL'),
                'investability_constrained_long_count': invest_constrained.get('longCount', 'NULL'),
                'investability_constrained_short_count': invest_constrained.get('shortCount', 'NULL'),
                'investability_constrained_turnover': invest_constrained.get('turnover', 'NULL'),
                'investability_constrained_returns': invest_constrained.get('returns', 'NULL'),
                'investability_constrained_drawdown': invest_constrained.get('drawdown', 'NULL'),
                'investability_constrained_margin': invest_constrained.get('margin', 'NULL'),
                'investability_constrained_fitness': invest_constrained.get('fitness', 'NULL'),
                'investability_constrained_sharpe': invest_constrained.get('sharpe', 'NULL'),
            })
            
            # 风险中性化性能指标
            risk_neutralized = is_data.get('riskNeutralized', {})
            parsed_data.update({
                'risk_neutralized_pnl': risk_neutralized.get('pnl', 'NULL'),
                'risk_neutralized_book_size': risk_neutralized.get('bookSize', 'NULL'),
                'risk_neutralized_long_count': risk_neutralized.get('longCount', 'NULL'),
                'risk_neutralized_short_count': risk_neutralized.get('shortCount', 'NULL'),
                'risk_neutralized_turnover': risk_neutralized.get('turnover', 'NULL'),
                'risk_neutralized_returns': risk_neutralized.get('returns', 'NULL'),
                'risk_neutralized_drawdown': risk_neutralized.get('drawdown', 'NULL'),
                'risk_neutralized_margin': risk_neutralized.get('margin', 'NULL'),
                'risk_neutralized_fitness': risk_neutralized.get('fitness', 'NULL'),
                'risk_neutralized_sharpe': risk_neutralized.get('sharpe', 'NULL'),
            })
            
            # OS阶段性能指标
            os_data = alpha_data.get('os', {})
            # 注意：OS阶段数据结构可能与IS阶段不同，这里仅提取部分关键字段
            parsed_data.update({
                'os_start_date': self.parse_date(os_data.get('startDate')),
                'os_is_sharpe_ratio': os_data.get('osISSharpeRatio', 'NULL'),
                'pre_close_sharpe_ratio': os_data.get('preCloseSharpeRatio', 'NULL'),
            })
            
            # 检查结果
            checks = is_data.get('checks', [])
            parsed_data['checks'] = json.dumps(checks, ensure_ascii=False) if checks else 'NULL'
            
            # 竞争信息
            competitions = alpha_data.get('competitions', [])
            parsed_data['competitions'] = json.dumps(competitions, ensure_ascii=False) if competitions else None
            
            # 金字塔信息
            pyramids = alpha_data.get('pyramids', [])
            parsed_data['pyramids'] = json.dumps(pyramids, ensure_ascii=False) if pyramids else None
            
            # 主题信息
            themes = alpha_data.get('themes', [])
            parsed_data['themes'] = json.dumps(themes, ensure_ascii=False) if themes else 'NULL'
            
            # 金字塔主题信息
            pyramid_themes = alpha_data.get('pyramidThemes', {})
            parsed_data['pyramid_themes'] = json.dumps(pyramid_themes, ensure_ascii=False) if pyramid_themes else 'NULL'
            
            return parsed_data
            
        except Exception as e:
            logger.error(f"解析已提交Alpha数据异常: {e}")
            return None
    
    def save_submitted_alpha_to_database(self, alpha_data: Dict) -> bool:
        """保存已提交Alpha数据到数据库的submitted_alphas表"""
        try:
            cursor = self.db_connection.cursor()
            
            # 准备数据，处理None值
            data = {
                'id': alpha_data.get('id') or 'NULL',
                'type': alpha_data.get('type') or 'NULL',
                'author': alpha_data.get('author') or 'NULL',
                'date_created': self.parse_datetime(alpha_data.get('date_created')) if alpha_data.get('date_created') else 'NULL',
                'date_submitted': self.parse_datetime(alpha_data.get('date_submitted')) if alpha_data.get('date_submitted') else 'NULL',
                'date_modified': self.parse_datetime(alpha_data.get('date_modified')) if alpha_data.get('date_modified') else 'NULL',
                'name': alpha_data.get('name') or 'NULL',
                'favorite': alpha_data.get('favorite') or 'NULL',
                'hidden': alpha_data.get('hidden') or 'NULL',
                'color': alpha_data.get('color') or 'NULL',
                'category': alpha_data.get('category') or 'NULL',
                'stage': alpha_data.get('stage') or 'NULL',
                'status': alpha_data.get('status') or 'NULL',
                'grade': alpha_data.get('grade') or 'NULL',
                'instrument_type': alpha_data.get('instrument_type') or 'NULL',
                'region': alpha_data.get('region') or 'NULL',
                'universe': alpha_data.get('universe') or 'NULL',
                'delay': alpha_data.get('delay') if alpha_data.get('delay') is not None else 'NULL',
                'decay': alpha_data.get('decay') if alpha_data.get('decay') is not None else 'NULL',
                'neutralization': alpha_data.get('neutralization') or 'NULL',
                'truncation': alpha_data.get('truncation') if alpha_data.get('truncation') is not None else 'NULL',
                'pasteurization': alpha_data.get('pasteurization') or 'NULL',
                'unit_handling': alpha_data.get('unit_handling') or 'NULL',
                'nan_handling': alpha_data.get('nan_handling') or 'NULL',
                'selection_handling': alpha_data.get('selection_handling') if alpha_data.get('selection_handling') is not None else 'NULL',  # SUPER类型特有字段
                'selection_limit': alpha_data.get('selection_limit') if alpha_data.get('selection_limit') is not None else 'NULL',       # SUPER类型特有字段
                'max_trade': alpha_data.get('max_trade') or 'NULL',
                'language': alpha_data.get('language') or 'NULL',
                'visualization': alpha_data.get('visualization'),
                'start_date': self.parse_date(alpha_data.get('start_date')) if alpha_data.get('start_date') else 'NULL',
                'end_date': self.parse_date(alpha_data.get('end_date')) if alpha_data.get('end_date') else 'NULL',
                'component_activation': alpha_data.get('component_activation') if alpha_data.get('component_activation') is not None else 'NULL',  # SUPER类型特有字段
                'test_period': alpha_data.get('test_period') if alpha_data.get('test_period') is not None else 'NULL',                    # SUPER类型特有字段
                'code': alpha_data.get('code') or 'NULL',
                'description': alpha_data.get('description') or 'NULL',
                'operator_count': alpha_data.get('operator_count') or 'NULL',
                # 新增的combo和selection字段
                'combo_code': alpha_data.get('combo_code') or 'NULL',
                'combo_description': alpha_data.get('combo_description') or 'NULL',
                'combo_operator_count': alpha_data.get('combo_operator_count') if alpha_data.get('combo_operator_count') is not None else 'NULL',
                'selection_code': alpha_data.get('selection_code') or 'NULL',
                'selection_description': alpha_data.get('selection_description') or 'NULL',
                'selection_operator_count': alpha_data.get('selection_operator_count') if alpha_data.get('selection_operator_count') is not None else 'NULL',
                'tags': alpha_data.get('tags') or 'NULL',
                'classifications': alpha_data.get('classifications') or 'NULL',
                'pnl': alpha_data.get('pnl') if alpha_data.get('pnl') is not None else 'NULL',
                'book_size': alpha_data.get('book_size') if alpha_data.get('book_size') is not None else 'NULL',
                'long_count': alpha_data.get('long_count') if alpha_data.get('long_count') is not None else 'NULL',
                'short_count': alpha_data.get('short_count') if alpha_data.get('short_count') is not None else 'NULL',
                'turnover': alpha_data.get('turnover') if alpha_data.get('turnover') is not None else 'NULL',
                'returns': alpha_data.get('returns') if alpha_data.get('returns') is not None else 'NULL',
                'drawdown': alpha_data.get('drawdown') if alpha_data.get('drawdown') is not None else 'NULL',
                'margin': alpha_data.get('margin') if alpha_data.get('margin') is not None else 'NULL',
                'sharpe': alpha_data.get('sharpe') if alpha_data.get('sharpe') is not None else 'NULL',
                'fitness': alpha_data.get('fitness') if alpha_data.get('fitness') is not None else 'NULL',
                'is_start_date': self.parse_date(alpha_data.get('is_start_date')) if alpha_data.get('is_start_date') else 'NULL',
                'os_start_date': self.parse_date(alpha_data.get('os_start_date')) if alpha_data.get('os_start_date') else 'NULL',  # OS阶段特有字段
                'self_correlation': alpha_data.get('self_correlation') if alpha_data.get('self_correlation') is not None else 'NULL',
                'prod_correlation': alpha_data.get('prod_correlation') if alpha_data.get('prod_correlation') is not None else 'NULL',
                'os_is_sharpe_ratio': alpha_data.get('os_is_sharpe_ratio') if alpha_data.get('os_is_sharpe_ratio') is not None else 'NULL',
                'pre_close_sharpe_ratio': alpha_data.get('pre_close_sharpe_ratio') if alpha_data.get('pre_close_sharpe_ratio') is not None else 'NULL',
                'investability_constrained_pnl': alpha_data.get('investability_constrained_pnl') if alpha_data.get('investability_constrained_pnl') is not None else 'NULL',
                'investability_constrained_book_size': alpha_data.get('investability_constrained_book_size') if alpha_data.get('investability_constrained_book_size') is not None else 'NULL',
                'investability_constrained_long_count': alpha_data.get('investability_constrained_long_count') if alpha_data.get('investability_constrained_long_count') is not None else 'NULL',
                'investability_constrained_short_count': alpha_data.get('investability_constrained_short_count') if alpha_data.get('investability_constrained_short_count') is not None else 'NULL',
                'investability_constrained_turnover': alpha_data.get('investability_constrained_turnover') if alpha_data.get('investability_constrained_turnover') is not None else 'NULL',
                'investability_constrained_returns': alpha_data.get('investability_constrained_returns') if alpha_data.get('investability_constrained_returns') is not None else 'NULL',
                'investability_constrained_drawdown': alpha_data.get('investability_constrained_drawdown') if alpha_data.get('investability_constrained_drawdown') is not None else 'NULL',
                'investability_constrained_margin': alpha_data.get('investability_constrained_margin') if alpha_data.get('investability_constrained_margin') is not None else 'NULL',
                'investability_constrained_fitness': alpha_data.get('investability_constrained_fitness') if alpha_data.get('investability_constrained_fitness') is not None else 'NULL',
                'investability_constrained_sharpe': alpha_data.get('investability_constrained_sharpe') if alpha_data.get('investability_constrained_sharpe') is not None else 'NULL',
                'risk_neutralized_pnl': alpha_data.get('risk_neutralized_pnl') if alpha_data.get('risk_neutralized_pnl') is not None else 'NULL',
                'risk_neutralized_book_size': alpha_data.get('risk_neutralized_book_size') if alpha_data.get('risk_neutralized_book_size') is not None else 'NULL',
                'risk_neutralized_long_count': alpha_data.get('risk_neutralized_long_count') if alpha_data.get('risk_neutralized_long_count') is not None else 'NULL',
                'risk_neutralized_short_count': alpha_data.get('risk_neutralized_short_count') if alpha_data.get('risk_neutralized_short_count') is not None else 'NULL',
                'risk_neutralized_turnover': alpha_data.get('risk_neutralized_turnover') if alpha_data.get('risk_neutralized_turnover') is not None else 'NULL',
                'risk_neutralized_returns': alpha_data.get('risk_neutralized_returns') if alpha_data.get('risk_neutralized_returns') is not None else 'NULL',
                'risk_neutralized_drawdown': alpha_data.get('risk_neutralized_drawdown') if alpha_data.get('risk_neutralized_drawdown') is not None else 'NULL',
                'risk_neutralized_margin': alpha_data.get('risk_neutralized_margin') if alpha_data.get('risk_neutralized_margin') is not None else 'NULL',
                'risk_neutralized_fitness': alpha_data.get('risk_neutralized_fitness') if alpha_data.get('risk_neutralized_fitness') is not None else 'NULL',
                'risk_neutralized_sharpe': alpha_data.get('risk_neutralized_sharpe') if alpha_data.get('risk_neutralized_sharpe') is not None else 'NULL',
                'checks': alpha_data.get('checks') or 'NULL',
                'competitions': alpha_data.get('competitions') or 'NULL',
                'pyramids': alpha_data.get('pyramids') or 'NULL',
                'themes': alpha_data.get('themes') or 'NULL',
                'pyramid_themes': alpha_data.get('pyramid_themes') or 'NULL'
            }
            
            # 转义单引号并包装字符串值
            # JSON字段不需要额外的转义处理
            json_fields = {'checks', 'competitions', 'pyramids', 'themes', 'tags', 'classifications', 'pyramid_themes'}
            
            for key, value in data.items():
                if value == 'NULL':
                    data[key] = None  # 将'NULL'字符串改为None，让参数化查询正确处理NULL值
                elif key in ['date_created', 'date_submitted', 'date_modified']:
                    # 日期时间字段已经处理过，不需要再包装（使用参数化查询）
                    if value is not None:
                        data[key] = str(value)
                    else:
                        data[key] = None  # 将'NULL'字符串改为None，让参数化查询正确处理NULL值
                elif key in ['start_date', 'end_date', 'is_start_date', 'os_start_date']:
                    # 日期字段已经处理过，不需要再包装（使用参数化查询）
                    if value is not None:
                        data[key] = str(value)
                    else:
                        data[key] = None  # 将'NULL'字符串改为None，让参数化查询正确处理NULL值
                elif key in json_fields:
                    # JSON字段特殊处理，只需要确保是有效的JSON字符串
                    if isinstance(value, str) and value != 'NULL':
                        # JSON字段需要转义单引号，但不能转义双引号（JSON中的双引号是有效的）
                        # 使用参数化查询来正确处理JSON字段，避免手动转义
                        data[key] = value
                    elif value == 'NULL':
                        data[key] = None  # 将'NULL'字符串改为None，让参数化查询正确处理NULL值
                    else:
                        # 其他情况转换为字符串
                        data[key] = str(value)
                elif isinstance(value, str):
                    # 普通字符串字段，不进行单引号包装（由参数化查询处理）
                    # 只需要确保字符串本身是有效的，不需要手动转义单引号
                    pass  # 依赖参数化查询自动处理字符串转义和包装
                elif isinstance(value, (int, float)):
                    # 数值类型保持原样，让参数化查询处理
                    pass  # 不需要转换，保持原始数值类型
                elif isinstance(value, bool):
                    # 布尔值保持原样，让参数化查询处理
                    pass  # 不需要转换，保持原始布尔类型
                elif value is None:
                    data[key] = None  # 将'NULL'字符串改为None，让参数化查询正确处理NULL值
                else:
                    # 其他类型保持原样或转换为字符串
                    pass  # 对于其他类型，依赖参数化查询的自动处理
            
            # 构造SQL语句，使用format方式替换占位符，避免参数占位符与参数值对应问题
            sql_template = """
            INSERT INTO submitted_alphas (
                id, type, author, date_created, date_submitted, date_modified, name,
                favorite, hidden, color, category, stage, status, grade,
                instrument_type, region, universe, delay, decay, neutralization,
                truncation, pasteurization, unit_handling, nan_handling, selection_handling, selection_limit,
                max_trade, language, visualization, start_date, end_date, component_activation, test_period,
                code, description, operator_count,
                combo_code, combo_description, combo_operator_count,
                selection_code, selection_description, selection_operator_count,
                tags, classifications,
                pnl, book_size, long_count, short_count, turnover, returns, drawdown,
                margin, sharpe, fitness, is_start_date, os_start_date,
                self_correlation, prod_correlation, os_is_sharpe_ratio, pre_close_sharpe_ratio,
                investability_constrained_pnl, investability_constrained_book_size,
                investability_constrained_long_count, investability_constrained_short_count,
                investability_constrained_turnover, investability_constrained_returns,
                investability_constrained_drawdown, investability_constrained_margin,
                investability_constrained_fitness, investability_constrained_sharpe,
                risk_neutralized_pnl, risk_neutralized_book_size,
                risk_neutralized_long_count, risk_neutralized_short_count,
                risk_neutralized_turnover, risk_neutralized_returns,
                risk_neutralized_drawdown, risk_neutralized_margin,
                risk_neutralized_fitness, risk_neutralized_sharpe,
                checks, competitions, pyramids, themes, pyramid_themes
            ) VALUES (
                {id}, {type}, {author}, {date_created}, {date_submitted}, {date_modified}, {name},
                {favorite}, {hidden}, {color}, {category}, {stage}, {status}, {grade},
                {instrument_type}, {region}, {universe}, {delay}, {decay}, {neutralization},
                {truncation}, {pasteurization}, {unit_handling}, {nan_handling}, {selection_handling}, {selection_limit},
                {max_trade}, {language}, {visualization}, {start_date}, {end_date}, {component_activation}, {test_period},
                {code}, {description}, {operator_count},
                {combo_code}, {combo_description}, {combo_operator_count},
                {selection_code}, {selection_description}, {selection_operator_count},
                {tags}, {classifications},
                {pnl}, {book_size}, {long_count}, {short_count}, {turnover}, {returns}, {drawdown},
                {margin}, {sharpe}, {fitness}, {is_start_date}, {os_start_date},
                {self_correlation}, {prod_correlation}, {os_is_sharpe_ratio}, {pre_close_sharpe_ratio},
                {investability_constrained_pnl}, {investability_constrained_book_size},
                {investability_constrained_long_count}, {investability_constrained_short_count},
                {investability_constrained_turnover}, {investability_constrained_returns},
                {investability_constrained_drawdown}, {investability_constrained_margin},
                {investability_constrained_fitness}, {investability_constrained_sharpe},
                {risk_neutralized_pnl}, {risk_neutralized_book_size},
                {risk_neutralized_long_count}, {risk_neutralized_short_count},
                {risk_neutralized_turnover}, {risk_neutralized_returns},
                {risk_neutralized_drawdown}, {risk_neutralized_margin},
                {risk_neutralized_fitness}, {risk_neutralized_sharpe},
                {checks}, {competitions}, {pyramids}, {themes}, {pyramid_themes}
            ) ON DUPLICATE KEY UPDATE
                type = VALUES(type), author = VALUES(author), date_created = VALUES(date_created),
                date_submitted = VALUES(date_submitted), date_modified = VALUES(date_modified),
                name = VALUES(name), favorite = VALUES(favorite), hidden = VALUES(hidden),
                color = VALUES(color), category = VALUES(category), stage = VALUES(stage),
                status = VALUES(status), grade = VALUES(grade), instrument_type = VALUES(instrument_type),
                region = VALUES(region), universe = VALUES(universe), delay = VALUES(delay),
                decay = VALUES(decay), neutralization = VALUES(neutralization), truncation = VALUES(truncation),
                pasteurization = VALUES(pasteurization), unit_handling = VALUES(unit_handling),
                nan_handling = VALUES(nan_handling), max_trade = VALUES(max_trade), language = VALUES(language),
                visualization = VALUES(visualization), start_date = VALUES(start_date), end_date = VALUES(end_date),
                code = VALUES(code), description = VALUES(description), operator_count = VALUES(operator_count),
                combo_code = VALUES(combo_code), combo_description = VALUES(combo_description), combo_operator_count = VALUES(combo_operator_count),
                selection_code = VALUES(selection_code), selection_description = VALUES(selection_description), selection_operator_count = VALUES(selection_operator_count),
                tags = VALUES(tags), classifications = VALUES(classifications), pnl = VALUES(pnl),
                book_size = VALUES(book_size), long_count = VALUES(long_count), short_count = VALUES(short_count),
                turnover = VALUES(turnover), returns = VALUES(returns), drawdown = VALUES(drawdown),
                margin = VALUES(margin), sharpe = VALUES(sharpe), fitness = VALUES(fitness),
                is_start_date = VALUES(is_start_date), os_start_date = VALUES(os_start_date),
                self_correlation = VALUES(self_correlation), prod_correlation = VALUES(prod_correlation),
                os_is_sharpe_ratio = VALUES(os_is_sharpe_ratio), pre_close_sharpe_ratio = VALUES(pre_close_sharpe_ratio),
                investability_constrained_pnl = VALUES(investability_constrained_pnl),
                investability_constrained_book_size = VALUES(investability_constrained_book_size),
                investability_constrained_long_count = VALUES(investability_constrained_long_count),
                investability_constrained_short_count = VALUES(investability_constrained_short_count),
                investability_constrained_turnover = VALUES(investability_constrained_turnover),
                investability_constrained_returns = VALUES(investability_constrained_returns),
                investability_constrained_drawdown = VALUES(investability_constrained_drawdown),
                investability_constrained_margin = VALUES(investability_constrained_margin),
                investability_constrained_fitness = VALUES(investability_constrained_fitness),
                investability_constrained_sharpe = VALUES(investability_constrained_sharpe),
                risk_neutralized_pnl = VALUES(risk_neutralized_pnl),
                risk_neutralized_book_size = VALUES(risk_neutralized_book_size),
                risk_neutralized_long_count = VALUES(risk_neutralized_long_count),
                risk_neutralized_short_count = VALUES(risk_neutralized_short_count),
                risk_neutralized_turnover = VALUES(risk_neutralized_turnover),
                risk_neutralized_returns = VALUES(risk_neutralized_returns),
                risk_neutralized_drawdown = VALUES(risk_neutralized_drawdown),
                risk_neutralized_margin = VALUES(risk_neutralized_margin),
                risk_neutralized_fitness = VALUES(risk_neutralized_fitness),
                risk_neutralized_sharpe = VALUES(risk_neutralized_sharpe),
                checks = VALUES(checks), competitions = VALUES(competitions), pyramids = VALUES(pyramids),
                themes = VALUES(themes), pyramid_themes = VALUES(pyramid_themes), updated_at = CURRENT_TIMESTAMP
            """
            
            # 提取所有值作为参数元组，保持与SQL模板中占位符的顺序一致
            values = (
                data['id'], data['type'], data['author'], data['date_created'], data['date_submitted'], data['date_modified'], data['name'],
                data['favorite'], data['hidden'], data['color'], data['category'], data['stage'], data['status'], data['grade'],
                data['instrument_type'], data['region'], data['universe'], data['delay'], data['decay'], data['neutralization'],
                data['truncation'], data['pasteurization'], data['unit_handling'], data['nan_handling'], data['selection_handling'], data['selection_limit'],
                data['max_trade'], data['language'], data['visualization'], data['start_date'], data['end_date'], data['component_activation'], data['test_period'],
                data['code'], data['description'], data['operator_count'],
                data['combo_code'], data['combo_description'], data['combo_operator_count'],
                data['selection_code'], data['selection_description'], data['selection_operator_count'],
                data['tags'], data['classifications'],
                data['pnl'], data['book_size'], data['long_count'], data['short_count'], data['turnover'], data['returns'], data['drawdown'],
                data['margin'], data['sharpe'], data['fitness'], data['is_start_date'], data['os_start_date'],
                data['self_correlation'], data['prod_correlation'], data['os_is_sharpe_ratio'], data['pre_close_sharpe_ratio'],
                data['investability_constrained_pnl'], data['investability_constrained_book_size'],
                data['investability_constrained_long_count'], data['investability_constrained_short_count'],
                data['investability_constrained_turnover'], data['investability_constrained_returns'],
                data['investability_constrained_drawdown'], data['investability_constrained_margin'],
                data['investability_constrained_fitness'], data['investability_constrained_sharpe'],
                data['risk_neutralized_pnl'], data['risk_neutralized_book_size'],
                data['risk_neutralized_long_count'], data['risk_neutralized_short_count'],
                data['risk_neutralized_turnover'], data['risk_neutralized_returns'],
                data['risk_neutralized_drawdown'], data['risk_neutralized_margin'],
                data['risk_neutralized_fitness'], data['risk_neutralized_sharpe'],
                data['checks'], data['competitions'], data['pyramids'], data['themes'], data['pyramid_themes']
            )
            
            # 使用参数化查询执行SQL语句
            sql = sql_template.format(
                id='%s', type='%s', author='%s', date_created='%s', date_submitted='%s', date_modified='%s', name='%s',
                favorite='%s', hidden='%s', color='%s', category='%s', stage='%s', status='%s', grade='%s',
                instrument_type='%s', region='%s', universe='%s', delay='%s', decay='%s', neutralization='%s',
                truncation='%s', pasteurization='%s', unit_handling='%s', nan_handling='%s', selection_handling='%s', selection_limit='%s',
                max_trade='%s', language='%s', visualization='%s', start_date='%s', end_date='%s', component_activation='%s', test_period='%s',
                code='%s', description='%s', operator_count='%s',
                combo_code='%s', combo_description='%s', combo_operator_count='%s',
                selection_code='%s', selection_description='%s', selection_operator_count='%s',
                tags='%s', classifications='%s',
                pnl='%s', book_size='%s', long_count='%s', short_count='%s', turnover='%s', returns='%s', drawdown='%s',
                margin='%s', sharpe='%s', fitness='%s', is_start_date='%s', os_start_date='%s',
                self_correlation='%s', prod_correlation='%s', os_is_sharpe_ratio='%s', pre_close_sharpe_ratio='%s',
                investability_constrained_pnl='%s', investability_constrained_book_size='%s',
                investability_constrained_long_count='%s', investability_constrained_short_count='%s',
                investability_constrained_turnover='%s', investability_constrained_returns='%s',
                investability_constrained_drawdown='%s', investability_constrained_margin='%s',
                investability_constrained_fitness='%s', investability_constrained_sharpe='%s',
                risk_neutralized_pnl='%s', risk_neutralized_book_size='%s',
                risk_neutralized_long_count='%s', risk_neutralized_short_count='%s',
                risk_neutralized_turnover='%s', risk_neutralized_returns='%s',
                risk_neutralized_drawdown='%s', risk_neutralized_margin='%s',
                risk_neutralized_fitness='%s', risk_neutralized_sharpe='%s',
                checks='%s', competitions='%s', pyramids='%s', themes='%s', pyramid_themes='%s'
            )
            
            # 记录完整的SQL语句以便调试
            logger.debug(f"准备执行的完整SQL语句: {sql}")
            
            cursor.execute(sql, values)
            self.db_connection.commit()
            cursor.close()
            
            # logger.info(f"保存已提交Alpha数据成功: {alpha_data.get('id')}")
            return True
            
        except Error as e:
            alpha_id = alpha_data.get('id', '未知ID')
            # 打印完整SQL语句以便调试
            logger.error(f"保存已提交Alpha数据失败 (ID: {alpha_id}): {e}")
            # 在错误日志中显示实际执行的完整SQL语句
            logger.error(f"完整SQL语句: {sql}")
            return False

    def crawl_submitted_alphas(self, total_limit: Optional[int] = None, filters: Optional[Dict] = None, 
                              task_id: str = 'default', crawl_status_id: Optional[int] = None) -> bool:
        """爬取已提交Alpha数据 - 支持分批处理和断点续连
        
        Args:
            total_limit: 每批数据量限制
            filters: 过滤条件（已提交Alpha数据不需要过滤条件）
            task_id: 任务ID
            crawl_status_id: 批次记录ID，用于断点续连
            
        Returns:
            爬取是否成功
        """
        # 记录开始时间
        start_time = datetime.now()
        
        # 如果提供了批次记录ID，则更新状态为running并计算duration_seconds
        if crawl_status_id:
            cursor = self.db_connection.cursor()
            sql = "UPDATE crawl_status SET status = 'running', start_time = %s, duration_seconds = TIMESTAMPDIFF(SECOND, %s, NOW()) WHERE id = %s"
            cursor.execute(sql, (start_time, start_time, crawl_status_id))
            self.db_connection.commit()
            cursor.close()
            logger.info(f"使用现有批次记录 ID: {crawl_status_id}")
        else:
            # 创建新的爬虫状态记录（已提交Alpha数据不需要过滤条件）
            crawl_status_id = self.create_crawl_status(start_time, None, task_id, 'submitted_alphas_crawl')
            if not crawl_status_id:
                logger.error("创建爬虫状态记录失败")
                return False
        
        # 初始化统计
        total_count = 0
        success_count = 0
        error_count = 0
        limit = 100
        offset = 0
        
        try:
            while True:
                # 检查认证状态，如果过期则重新认证
                if not self.authenticate():
                    logger.error("认证失败，尝试重新认证")
                    # 重新认证失败则继续尝试，不停止爬取
                    continue
                
                # 获取一页已提交Alpha数据
                page_data = self.get_submitted_alphas_page(limit, offset)
                
                if not page_data:
                    logger.error(f"第 {offset//limit + 1} 页已提交Alpha数据获取失败")
                    error_count += 1
                    break
                
                results = page_data.get('results', [])
                if not results:
                    logger.info("没有更多已提交Alpha数据")
                    break
                
                # 处理本页数据
                page_success = 0
                page_error = 0
                
                for alpha_data in results:
                    try:
                        # 解析数据
                        parsed_data = self.parse_submitted_alpha_data(alpha_data)
                        
                        if not parsed_data:
                            logger.warning(f"已提交Alpha数据解析失败: {alpha_data.get('id')}")
                            page_error += 1
                            continue
                        
                        # 保存到数据库
                        if self.save_submitted_alpha_to_database(parsed_data):
                            page_success += 1
                        else:
                            page_error += 1
                            
                    except Exception as e:
                        logger.error(f"处理已提交Alpha数据异常: {e}")
                        page_error += 1
                
                # 更新统计
                success_count += page_success
                error_count += page_error
                total_count += len(results)
                
                logger.info(f"第 {offset//limit + 1} 页处理完成: 成功 {page_success}, 失败 {page_error}")
                
                # 更新爬虫状态记录
                self.update_crawl_status(crawl_status_id, total_count, success_count, error_count, offset)
                
                # 检查是否达到总数限制
                if total_limit and total_count >= total_limit:
                    logger.info(f"达到总数限制 {total_limit}，停止爬取")
                    break
                
                # 检查是否还有下一页
                next_url = page_data.get('next')
                if not next_url:
                    logger.info("已获取所有已提交Alpha数据")
                    break
                
                # 更新offset
                offset += limit
                
                # 添加延迟避免频繁请求
                time.sleep(2)
            
            # 记录结束时间
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # 更新爬虫状态记录为完成状态
            self.complete_crawl_status(crawl_status_id, total_count, success_count, error_count, offset, end_time, duration)
            
            logger.info(f"已提交Alpha数据爬取完成: 总数 {total_count}, 成功 {success_count}, 失败 {error_count}, 耗时 {duration} 秒")
            return success_count > 0
            
        except Exception as e:
            # 记录错误信息
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            error_message = str(e)
            
            # 更新爬虫状态记录为错误状态
            self.error_crawl_status(crawl_status_id, total_count, success_count, error_count, offset, end_time, duration, error_message)
            
            logger.error(f"爬取已提交Alpha数据过程中发生异常: {e}")
            return False

    def create_daily_batch_filters(self, start_date: str, end_date: str) -> List[Dict]:
        """创建每日分批过滤条件
        
        Args:
            start_date: 开始日期，格式: 2025-05-10
            end_date: 结束日期，格式: 2025-10-24
            
        Returns:
            每日过滤条件列表
        """
        from datetime import datetime, timedelta
        
        try:
            # 解析日期
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            # 计算天数差
            delta_days = (end_dt - start_dt).days + 1
            
            if delta_days <= 0:
                logger.error("结束日期必须晚于开始日期")
                return []
            
            batch_filters = []
            
            # 为每一天创建过滤条件
            for i in range(delta_days):
                current_date = start_dt + timedelta(days=i)
                date_str = current_date.strftime('%Y-%m-%d')
                
                # 创建当天的过滤条件
                filters = {
                    'dateSubmitted': date_str,
                    'description': f"已提交Alpha数据 - {date_str}"
                }
                
                batch_filters.append(filters)
            
            logger.info(f"创建了 {len(batch_filters)} 天的分批过滤条件")
            return batch_filters
            
        except Exception as e:
            logger.error(f"创建每日分批过滤条件失败: {e}")
            return []


def check_resume_point(crawler: SubmittedAlphaCrawler, resume: bool) -> Tuple[str, bool]:
    """检查断点续连点
    
    Args:
        crawler: 爬虫实例
        resume: 是否断点续连
        
    Returns:
        (实际任务ID, 是否继续处理现有任务)
    """
    # 生成新的任务ID
    actual_task_id = f"submitted_alphas_{int(time.time())}"
    
    # 如果不启用断点续连，直接返回新任务ID
    if not resume:
        logger.info("断点续连已禁用，使用新任务")
        return actual_task_id, False
    
    # 查找前一个主任务
    cursor = crawler.db_connection.cursor()
    sql = """
    SELECT DISTINCT task_id, created_at FROM crawl_status 
    WHERE task_type = 'submitted_alphas_crawl_batch' 
    ORDER BY created_at DESC 
    LIMIT 1
    """
    cursor.execute(sql)
    result = cursor.fetchone()
    cursor.close()
    
    if not result:
        logger.info("断点续连：未找到前一个主任务，使用新任务")
        return actual_task_id, False
    
    previous_task_id = result[0]
    # 检查前一个主任务的所有批次是否都已完成（只有completed状态才算完成，running状态需要重新处理）
    cursor = crawler.db_connection.cursor()
    sql = """
    SELECT COUNT(*) as total_batches,
           SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_batches,
           SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) as running_batches
    FROM crawl_status 
    WHERE task_id = %s AND task_type = 'submitted_alphas_crawl_batch'
    """
    cursor.execute(sql, (previous_task_id,))
    result = cursor.fetchone()
    cursor.close()
    
    if result and result[0] > 0 and result[1] < result[0]:
        # 前一个主任务有未完成的批次（只有completed状态才算完成，running状态需要重新处理），继续处理前一个任务
        actual_task_id = previous_task_id
        logger.info(f"断点续连：检测到前一个主任务 {previous_task_id} 有未完成批次，继续处理该任务")
        logger.info(f"批次完成情况：{result[1]}/{result[0]} 已完成，{result[2]}/{result[0]} 运行中")
        return actual_task_id, True
    else:
        logger.info(f"断点续连：前一个主任务 {previous_task_id} 所有批次已完成，使用新任务 {actual_task_id}")
        return actual_task_id, False


def process_batch_data(crawler: SubmittedAlphaCrawler, task_id: str, total_limit: Optional[int] = None) -> Tuple[int, int]:
    """处理分批数据
    
    Args:
        crawler: 爬虫实例
        task_id: 任务ID
        total_limit: 每批数据量限制，默认None（获取所有数据）
        
    Returns:
        (成功批次数量, 失败批次数量)
    """
    total_success = 0
    total_error = 0
    
    # 从数据库读取批次信息，优先处理running状态的批次，然后是pending状态的批次
    cursor = crawler.db_connection.cursor()
    sql = """
    SELECT id, batch_info, status FROM crawl_status 
    WHERE task_id = %s AND (status = 'running' OR status = 'pending') 
    ORDER BY 
        CASE WHEN status = 'running' THEN 1 ELSE 2 END,
        id
    """
    cursor.execute(sql, (task_id,))
    batch_records = cursor.fetchall()
    cursor.close()
    
    if not batch_records:
        logger.warning(f"未找到任务 {task_id} 的待处理批次记录")
        return 0, 0
    
    logger.info(f"从数据库读取到 {len(batch_records)} 个待处理批次")
    
    for i, record in enumerate(batch_records):
        
        batch_info = json.loads(record[1])  # batch_info字段
        
        logger.info(f"=== 开始处理第 {i+1} 批数据 ===")
        logger.info(f"批次描述: {batch_info.get('description', '未知批次')}")
        
        # 使用从数据库读取的批次信息进行爬取，直接传递批次记录ID
        success = crawler.crawl_submitted_alphas(total_limit=total_limit, filters=batch_info, task_id=task_id, crawl_status_id=record[0])
        
        if success:
            logger.info(f"第 {i+1} 批数据爬取成功")
            total_success += 1
        else:
            logger.error(f"第 {i+1} 批数据爬取失败")
            total_error += 1
        
        # 批次间延迟
        time.sleep(5)
    
    return total_success, total_error


def initialize_submitted_crawler() -> Optional[SubmittedAlphaCrawler]:
    """初始化已提交Alpha爬虫实例"""
    crawler = SubmittedAlphaCrawler()
    
    # 1. API认证
    logger.info("步骤1: API认证")
    if not crawler.authenticate():
        logger.error("API认证失败")
        return None
    
    # 2. 连接数据库
    logger.info("步骤2: 连接数据库")
    if not crawler.connect_database():
        logger.error("数据库连接失败")
        return None
    
    # 3. 创建数据库表
    logger.info("步骤3: 创建数据库表")
    if not crawler.create_tables():
        logger.error("数据库表创建失败")
        return None
    
    return crawler


def main(resume: bool = True, total_limit: Optional[int] = None):
    """主函数，支持断点续连和分批处理
    
    Args:
        resume: 是否断点续连，默认开启
        total_limit: 每批数据量限制，默认None（获取所有数据）
    """
    logger.info("=== WorldQuant已提交Alpha数据爬虫启动 ===")
    logger.info(f"断点续连模式: {'启用' if resume else '禁用'}")
    logger.info(f"每批数据量限制: {'无限制（获取所有数据）' if total_limit is None else total_limit}")
    
    # 初始化爬虫
    crawler = initialize_submitted_crawler()
    if not crawler:
        logger.error("已提交Alpha爬虫初始化失败，程序退出")
        return False
    
    try:
        # 检查断点续连点
        actual_task_id, should_resume = check_resume_point(crawler, resume)
        
        if should_resume:
            # 断点续连模式：处理现有任务的分批数据
            logger.info(f"=== 断点续连模式：继续处理任务 {actual_task_id} ===")
            success_batches, error_batches = process_batch_data(crawler, actual_task_id, total_limit)
            
            logger.info(f"断点续连处理完成：成功批次 {success_batches}，失败批次 {error_batches}")
            
            # 检查是否所有批次都已完成
            cursor = crawler.db_connection.cursor()
            sql = """
            SELECT COUNT(*) as total_batches,
                   SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_batches
            FROM crawl_status 
            WHERE task_id = %s AND task_type = 'submitted_alphas_crawl_batch'
            """
            cursor.execute(sql, (actual_task_id,))
            result = cursor.fetchone()
            cursor.close()
            
            if result and result[0] > 0 and result[1] == result[0]:
                logger.info(f"任务 {actual_task_id} 所有批次已完成")
                success = True
            else:
                logger.warning(f"任务 {actual_task_id} 仍有未完成批次")
                success = False
        
        else:
            # 新任务模式：直接处理所有已提交Alpha数据，不需要按日期分批
            logger.info(f"=== 新任务模式：创建任务 {actual_task_id} ===")
            
            # 创建单个批次记录，不按日期分批
            batch_info = json.dumps({
                'description': '已提交Alpha数据 - 全部'
            })
            
            cursor = crawler.db_connection.cursor()
            sql = """
            INSERT INTO crawl_status (crawl_date, task_id, task_type, status, batch_info, start_time, created_at, updated_at)
            VALUES (CURDATE(), %s, 'submitted_alphas_crawl_batch', 'pending', %s, NOW(), NOW(), NOW())
            """
            cursor.execute(sql, (actual_task_id, batch_info))
            crawler.db_connection.commit()
            cursor.close()
            
            logger.info("创建批次记录: 已提交Alpha数据 - 全部")
            
            # 处理数据
            success_batches, error_batches = process_batch_data(crawler, actual_task_id, total_limit)
            
            logger.info(f"新任务处理完成：成功批次 {success_batches}，失败批次 {error_batches}")
            success = success_batches > 0
        
        # 输出结果
        if success:
            logger.info("=== 已提交Alpha数据爬虫执行成功 ===")
        else:
            logger.error("=== 已提交Alpha数据爬虫执行失败 ===")
        
        return success
        
    except Exception as e:
        logger.error(f"已提交Alpha数据爬虫执行异常: {e}")
        return False
    
    finally:
        # 关闭连接
        crawler.close()
        logger.info("=== 已提交Alpha数据爬虫执行结束 ===")


if __name__ == "__main__":
    # 执行主函数
    success = main()
    
    # 根据执行结果退出
    sys.exit(0 if success else 1)
