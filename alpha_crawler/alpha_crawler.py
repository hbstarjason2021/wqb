#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WorldQuant Alpha数据爬虫 - 统一脚本
整合API客户端功能，实现一页一入库的功能
"""

import os
import sys
import json
import time
import logging
import base64
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import requests
import mysql.connector
from mysql.connector import Error
from urllib.parse import urlencode

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 尝试导入公共session管理器（可选，不影响现有功能）
try:
    from common.session_manager import get_shared_session
    SHARED_SESSION_AVAILABLE = True
except ImportError:
    SHARED_SESSION_AVAILABLE = False

# 配置日志
# 确保log目录存在
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'log')
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'alpha_crawler.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AlphaCrawler:
    """Alpha数据爬虫 - 统一脚本"""
    
    def __init__(self, use_shared_session=True):
        """
        初始化Alpha爬虫
        
        Args:
            use_shared_session: 是否使用公共session（默认True）
        """
        # 加载配置
        self.config = self.load_config()
        self.use_shared_session = use_shared_session
        
        # API配置 - 整合API客户端功能
        self.base_url = "https://api.worldquantbrain.com"
        
        # 【重要】先创建一个基础session，确保self.session始终可用
        self.session = requests.Session()
        self.session.timeout = 60
        
        self.token = None
        self.is_authenticated = False
        self.auth_time = None  # 认证时间记录
        
        # 基础过滤条件 - 统一的生产版本
        self.base_filters = {
            
            # 'status': 'UNSUBMITTED%1FIS_FAIL',
            # 'color': 'GREEN',
            # 'settings.region': 'EUR'
        }
        
        # 数据库连接
        self.db_connection = None
        
        # 设置浏览器头（必须在session创建后）
        self.setup_browser_headers()
        
        # 【可选】尝试使用公共session（如果失败，继续使用上面创建的session）
        self._try_shared_session()
    
    def load_config(self) -> Dict:
        """加载配置文件"""
        try:
            config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'credentials.json')
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logger.info("配置文件加载成功")
            return config
        except Exception as e:
            logger.error(f"配置文件加载失败: {e}")
            return {}
    
    def _try_shared_session(self):
        """
        尝试使用公共session（可选优化，不影响原有功能）
        
        特点：
        - 如果公共session可用，替换当前session（已认证）
        - 如果不可用，保持使用__init__中创建的session（需要后续认证）
        - 完全向后兼容，零破坏性
        """
        if not self.use_shared_session or not SHARED_SESSION_AVAILABLE:
            logger.info("🔧 使用传统session（公共session已禁用或不可用）")
            return
        
        try:
            shared_session = get_shared_session()
            if shared_session:
                logger.info("✅ 切换到公共session（已认证，可直接使用）")
                self.session = shared_session
                self.is_authenticated = True
                self.auth_time = time.time()
            else:
                logger.info("🔧 使用传统session（公共session获取失败）")
        except Exception as e:
            logger.warning(f"⚠️ 获取公共session异常: {e}，继续使用传统session")
    
    def authenticate(self, force_renew: bool = False) -> bool:
        """
        API认证 - 基于参考文件的认证机制（完全向后兼容）
        
        Args:
            force_renew: 是否强制重新认证
        
        Returns:
            是否认证成功
        """
        try:
            # 【可选】如果使用公共session且未强制刷新，尝试复用
            if self.use_shared_session and SHARED_SESSION_AVAILABLE and not force_renew:
                if self.is_authenticated:
                    logger.info("♻️  使用公共session，无需重新认证")
                    return True
                # 尝试获取公共session
                try:
                    session = get_shared_session()
                    if session:
                        self.session = session
                        self.is_authenticated = True
                        self.auth_time = time.time()
                        logger.info("✅ 获取公共session成功")
                        return True
                except Exception as e:
                    logger.warning(f"⚠️ 获取公共session失败: {e}，继续使用传统认证")
            
            # 【传统认证逻辑 - 完全保留】
            if not self.config:
                logger.error("配置信息未加载")
                return False
            
            email = self.config.get('email')
            password = self.config.get('password')
            
            if not email or not password:
                logger.error("邮箱或密码未配置")
                return False
            
            # 强制重新认证时创建全新的session
            if force_renew:
                logger.info("强制重新认证，创建全新session...")
                self.session = requests.Session()
                self.session.timeout = 60
                self.setup_browser_headers()
                self.auth_time = None
            
            # 检查认证是否过期（3小时过期）
            current_time = time.time()
            if not force_renew and self.auth_time and (current_time - self.auth_time) < 3 * 3600 - 300:  # 提前5分钟重新认证
                logger.info("认证仍在有效期内，无需重新认证")
                return True
            
            # 创建Basic认证头
            credentials = f"{email}:{password}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()
            
            headers = {
                'Authorization': f'Basic {encoded_credentials}'
            }
            
            # 添加认证重试机制，避免429错误
            max_auth_retries = 50
            auth_retry_delay = 30  # 秒
            
            for auth_attempt in range(max_auth_retries):
                try:
                    logger.info(f"尝试认证 (尝试 {auth_attempt + 1}/{max_auth_retries})")
                    response = self.session.post('https://api.worldquantbrain.com/authentication', headers=headers)
                    
                    if response.status_code == 201:
                        logger.info("认证成功")
                        self.auth_time = current_time  # 记录认证时间
                        
                        # 检查认证响应中是否包含Cookie
                        if 'set-cookie' in response.headers:
                            logger.info("认证响应包含Cookie信息")
                        
                        # 打印认证后的Cookie信息用于调试
                        if self.session.cookies:
                            logger.info("认证后的Cookie信息:")
                            for cookie in self.session.cookies:
                                logger.info(f"  {cookie.name}: {cookie.value[:50]}...")
                        
                        # 认证成功，session已自动管理Cookie
                        self.is_authenticated = True
                        logger.info("认证成功，session已自动管理Cookie")
                        return True
                    elif response.status_code == 401:
                        logger.error("认证失败: 用户名或密码错误")
                        return False
                    elif response.status_code == 429:
                        # 速率限制，等待后重试
                        retry_after = int(response.headers.get("Retry-After", auth_retry_delay))
                        logger.warning(f"认证请求过于频繁，等待 {retry_after} 秒后重试...")
                        time.sleep(retry_after)
                        continue
                    else:
                        logger.error(f"认证失败: 状态码 {response.status_code}")
                        if auth_attempt < max_auth_retries - 1:
                            logger.info(f"{auth_retry_delay} 秒后重试...")
                            time.sleep(auth_retry_delay)
                            continue
                        else:
                            return False
                            
                except Exception as e:
                    logger.error(f"认证请求异常 (尝试 {auth_attempt + 1}/{max_auth_retries}): {e}")
                    if auth_attempt < max_auth_retries - 1:
                        logger.info(f"{auth_retry_delay} 秒后重试...")
                        time.sleep(auth_retry_delay)
                        continue
                    else:
                        return False
            
            return False
                
        except Exception as e:
            logger.error(f"认证过程中出错: {e}")
            return False
    
    def setup_browser_headers(self):
        """设置浏览器请求头 - 基于参考文件的实现"""
        # 添加多种浏览器标识，包括Chrome、Firefox、Edge等
        browser_headers = [
            # Chrome浏览器标识
            {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'Content-Type': 'application/json',
                'Origin': 'https://platform.worldquantbrain.com',
                'Pragma': 'no-cache',
                'Referer': 'https://platform.worldquantbrain.com/',
                'Sec-Ch-Ua': '\"Not?A_Brand\";v=\"99\", \"Chromium\";v=\"130\"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '\"Windows\"',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-site'
            },
            # Firefox浏览器标识
            {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'Content-Type': 'application/json',
                'Origin': 'https://platform.worldquantbrain.com',
                'Pragma': 'no-cache',
                'Referer': 'https://platform.worldquantbrain.com/',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-site'
            },
            # Edge浏览器标识
            {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0',
                'Accept': 'application/json, text/plain, */*',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'Content-Type': 'application/json',
                'Origin': 'https://platform.worldquantbrain.com',
                'Pragma': 'no-cache',
                'Referer': 'https://platform.worldquantbrain.com/',
                'Sec-Ch-Ua': '\"Not?A_Brand\";v=\"99\", \"Microsoft Edge\";v=\"130\"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': '\"Windows\"',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-site'
            }
        ]
        
        # 随机选择一个浏览器标识
        selected_headers = random.choice(browser_headers)
        self.session.headers.update(selected_headers)
    
    def get_alphas(self, limit: int = 100, offset: int = 0, filters: Optional[Dict] = None) -> Optional[Dict]:
        """获取Alpha列表 - 整合API客户端功能"""
        if not self.is_authenticated:
            logger.error("未认证，请先调用authenticate方法")
            return None
        
        try:
            # 构建查询参数
            params = {
                'limit': limit,
                'offset': offset,
                'hidden': 'false',
                'order': '-dateCreated'
            }
            
            # 添加过滤条件
            if filters:
                params.update(filters)
            
            # 构建URL
            query_string = urlencode(params, doseq=True)
            url = f"{self.base_url}/users/self/alphas?{query_string}"
            
            logger.info(f"请求URL: {url}")
            
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"成功获取数据，count: {data.get('count', 0)}, results数量: {len(data.get('results', []))}")
                return data
            elif response.status_code == 429:
                logger.warning("API调用频率限制，等待后重试")
                time.sleep(60)  # 等待1分钟后重试
                return self.get_alphas(limit, offset, filters)
            else:
                logger.error(f"获取Alpha列表失败，状态码: {response.status_code}, 响应: {response.text}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"获取Alpha列表请求异常: {e}")
            return None
    
    def get_all_alphas(self, total_limit: Optional[int] = None, 
                      filters: Optional[Dict] = None) -> List[Dict]:
        """获取所有Alpha数据（分页获取） - 整合API客户端功能"""
        if not self.is_authenticated:
            logger.error("未认证，请先调用authenticate方法")
            return []
        
        all_alphas = []
        offset = 0
        limit = 100
        
        while True:
            logger.info(f"正在获取第 {offset//limit + 1} 页数据...")
            
            data = self.get_alphas(limit, offset, filters)
            
            if not data:
                logger.error("获取数据失败，停止获取")
                break
            
            results = data.get('results', [])
            if not results:
                logger.info("没有更多数据")
                break
            
            all_alphas.extend(results)
            
            # 检查是否达到总数限制
            if total_limit and len(all_alphas) >= total_limit:
                all_alphas = all_alphas[:total_limit]
                logger.info(f"达到总数限制 {total_limit}，停止获取")
                break
            
            # 检查是否还有下一页
            next_url = data.get('next')
            if not next_url:
                logger.info("已获取所有数据")
                break
            
            offset += limit
            
            # 添加延迟避免频繁请求
            time.sleep(1)
        
        logger.info(f"总共获取 {len(all_alphas)} 条Alpha数据")
        return all_alphas
    
    def test_connection(self) -> bool:
        """测试连接和认证 - 整合API客户端功能"""
        logger.info("测试API连接...")
        
        # 先认证
        if not self.authenticate():
            return False
        
        # 测试获取少量数据
        test_data = self.get_alphas(limit=1, offset=0)
        
        if test_data:
            logger.info("API连接测试成功")
            return True
        else:
            logger.error("API连接测试失败")
            return False
    
    def connect_database(self) -> bool:
        """连接数据库"""
        try:
            config = self.load_config()
            db_config = config.get('database', {})
            
            self.db_connection = mysql.connector.connect(
                host=db_config.get('host', 'localhost'),
                port=db_config.get('port', 3306),
                user=db_config.get('username', 'quant_user'),
                password=db_config.get('password', 'quant_password'),
                database=db_config.get('database', 'consultant_analytics')
            )
            
            logger.info("数据库连接成功")
            return True
            
        except Error as e:
            logger.error(f"数据库连接失败: {e}")
            return False
    
    def create_tables(self) -> bool:
        """创建数据库表"""
        try:
            cursor = self.db_connection.cursor()
            
            # 读取SQL文件
            sql_file = os.path.join(os.path.dirname(__file__), 'database_schema.sql')
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql_script = f.read()
            
            # 分割SQL语句并执行
            statements = sql_script.split(';')
            for statement in statements:
                statement = statement.strip()
                if statement:
                    cursor.execute(statement)
            
            self.db_connection.commit()
            cursor.close()
            logger.info("数据库表创建成功")
            return True
            
        except Error as e:
            logger.error(f"创建数据库表失败: {e}")
            return False
    
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
    
    def parse_alpha_data(self, alpha_data: Dict) -> Dict:
        """解析Alpha数据"""
        try:
            # 处理name字段为空的情况，直接填入"anonymous"
            alpha_name = alpha_data.get('name')
            if not alpha_name:
                alpha_name = "anonymous"
            
            # 基本信息
            parsed = {
                'id': alpha_data.get('id'),
                'type': alpha_data.get('type'),
                'author': alpha_data.get('author'),
                'date_created': self.parse_datetime(alpha_data.get('dateCreated')),
                'date_submitted': self.parse_datetime(alpha_data.get('dateSubmitted')),
                'date_modified': self.parse_datetime(alpha_data.get('dateModified')),
                'name': alpha_name,
                'favorite': alpha_data.get('favorite', False),
                'hidden': alpha_data.get('hidden', False),
                'color': alpha_data.get('color'),
                'category': alpha_data.get('category'),
                'stage': alpha_data.get('stage'),
                'status': alpha_data.get('status'),
                'grade': alpha_data.get('grade'),
            }
            
            # 设置信息
            settings = alpha_data.get('settings', {})
            parsed.update({
                'instrument_type': settings.get('instrumentType'),
                'region': settings.get('region'),
                'universe': settings.get('universe'),
                'delay': settings.get('delay'),
                'decay': settings.get('decay'),
                'neutralization': settings.get('neutralization'),
                'truncation': settings.get('truncation'),
                'pasteurization': settings.get('pasteurization'),
                'unit_handling': settings.get('unitHandling'),
                'nan_handling': settings.get('nanHandling'),
                'selection_handling': settings.get('selectionHandling'),  # SUPER类型特有字段
                'selection_limit': settings.get('selectionLimit'),       # SUPER类型特有字段
                'max_trade': settings.get('maxTrade'),
                'language': settings.get('language'),
                'visualization': settings.get('visualization'),
                'start_date': self.parse_date(settings.get('startDate')),
                'end_date': self.parse_date(settings.get('endDate')),
                'component_activation': settings.get('componentActivation'),  # SUPER类型特有字段
                'test_period': settings.get('testPeriod'),                    # SUPER类型特有字段
            })
            
            # 常规信息 - 支持REGULAR和SUPER类型
            # 对于SUPER类型，我们需要从combo和selection中提取信息
            # 优先级：regular.code > combo.code > selection.code
            alpha_type = alpha_data.get('type', 'REGULAR')
            
            if alpha_type == 'SUPER':
                # 处理SUPER类型
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
                    code = None
                
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
                    description = None
                
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
                    operator_count = None
                
                # 提取combo信息
                combo_code = combo.get('code')
                combo_description = combo.get('description')
                combo_operator_count = combo.get('operatorCount')
                
                # 提取selection信息
                selection_code = selection.get('code')
                selection_description = selection.get('description')
                selection_operator_count = selection.get('operatorCount')
                
                parsed.update({
                    'code': code,
                    'description': description,
                    'operator_count': operator_count,
                    # 新增combo和selection的完整信息
                    'combo_code': combo_code,
                    'combo_description': combo_description,
                    'combo_operator_count': combo_operator_count,
                    'selection_code': selection_code,
                    'selection_description': selection_description,
                    'selection_operator_count': selection_operator_count,
                })
            else:
                # 处理REGULAR类型
                regular = alpha_data.get('regular', {})
                parsed.update({
                    'code': regular.get('code'),
                    'description': regular.get('description'),
                    'operator_count': regular.get('operatorCount'),
                })
            
            # 标签和分类信息
            parsed.update({
                'tags': json.dumps(alpha_data.get('tags', [])),
                'classifications': json.dumps(alpha_data.get('classifications', [])),
            })
            
            # IS阶段性能指标
            is_data = alpha_data.get('is', {})
            parsed.update({
                'pnl': is_data.get('pnl'),
                'book_size': is_data.get('bookSize'),
                'long_count': is_data.get('longCount'),
                'short_count': is_data.get('shortCount'),
                'turnover': is_data.get('turnover'),
                'returns': is_data.get('returns'),
                'drawdown': is_data.get('drawdown'),
                'margin': is_data.get('margin'),
                'sharpe': is_data.get('sharpe'),
                'fitness': is_data.get('fitness'),
                'is_start_date': self.parse_date(is_data.get('startDate')),
            })
            
            # 投资约束性能指标
            invest_constrained = is_data.get('investabilityConstrained', {})
            parsed.update({
                'investability_constrained_pnl': invest_constrained.get('pnl'),
                'investability_constrained_book_size': invest_constrained.get('bookSize'),
                'investability_constrained_long_count': invest_constrained.get('longCount'),
                'investability_constrained_short_count': invest_constrained.get('shortCount'),
                'investability_constrained_turnover': invest_constrained.get('turnover'),
                'investability_constrained_returns': invest_constrained.get('returns'),
                'investability_constrained_drawdown': invest_constrained.get('drawdown'),
                'investability_constrained_margin': invest_constrained.get('margin'),
                'investability_constrained_fitness': invest_constrained.get('fitness'),
                'investability_constrained_sharpe': invest_constrained.get('sharpe'),
            })
            
            # 风险中性化性能指标
            risk_neutralized = is_data.get('riskNeutralized', {})
            parsed.update({
                'risk_neutralized_pnl': risk_neutralized.get('pnl'),
                'risk_neutralized_book_size': risk_neutralized.get('bookSize'),
                'risk_neutralized_long_count': risk_neutralized.get('longCount'),
                'risk_neutralized_short_count': risk_neutralized.get('shortCount'),
                'risk_neutralized_turnover': risk_neutralized.get('turnover'),
                'risk_neutralized_returns': risk_neutralized.get('returns'),
                'risk_neutralized_drawdown': risk_neutralized.get('drawdown'),
                'risk_neutralized_margin': risk_neutralized.get('margin'),
                'risk_neutralized_fitness': risk_neutralized.get('fitness'),
                'risk_neutralized_sharpe': risk_neutralized.get('sharpe'),
            })
            
            # 其他信息
            parsed.update({
                'checks': json.dumps(is_data.get('checks', [])),
                'competitions': json.dumps(alpha_data.get('competitions', [])),
                'pyramids': json.dumps(alpha_data.get('pyramids', [])),
                'themes': json.dumps(alpha_data.get('themes', [])),
            })
            
            return parsed
            
        except Exception as e:
            logger.error(f"解析Alpha数据失败: {e}")
            return {}
    
    def save_alpha_to_database(self, alpha_data: Dict) -> bool:
        """保存Alpha数据到数据库"""
        try:
            cursor = self.db_connection.cursor()
            
            # 准备数据，处理None值
            data = {
                'id': alpha_data.get('id') or 'NULL',
                'type': alpha_data.get('type') or 'NULL',
                'author': alpha_data.get('author') or 'NULL',
                'date_created': alpha_data.get('date_created') or 'NULL',
                'date_submitted': alpha_data.get('date_submitted') or 'NULL',
                'date_modified': alpha_data.get('date_modified') or 'NULL',
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
                'visualization': alpha_data.get('visualization') or 'NULL',
                'start_date': alpha_data.get('start_date') or 'NULL',
                'end_date': alpha_data.get('end_date') or 'NULL',
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
                'is_start_date': alpha_data.get('is_start_date') or 'NULL',
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
                'themes': alpha_data.get('themes') or 'NULL'
            }
            
            # 转义单引号并包装字符串值
            # JSON字段不需要额外的转义处理
            json_fields = {'checks', 'competitions', 'pyramids', 'themes', 'tags', 'classifications'}
            
            for key, value in data.items():
                if value == 'NULL':
                    data[key] = None  # 将'NULL'字符串改为None，让参数化查询正确处理NULL值
                elif key in ['date_created', 'date_submitted', 'date_modified']:
                    # 日期时间字段已经处理过，不需要再包装（使用参数化查询）
                    if value is not None:
                        data[key] = str(value)
                    else:
                        data[key] = None  # 将'NULL'字符串改为None，让参数化查询正确处理NULL值
                elif key in ['start_date', 'end_date', 'is_start_date']:
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
            
            sql_template = """
            INSERT INTO alphas (
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
                margin, sharpe, fitness, is_start_date,
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
                checks, competitions, pyramids, themes
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
                {margin}, {sharpe}, {fitness}, {is_start_date},
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
                {checks}, {competitions}, {pyramids}, {themes}
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
                is_start_date = VALUES(is_start_date), investability_constrained_pnl = VALUES(investability_constrained_pnl),
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
                themes = VALUES(themes), updated_at = CURRENT_TIMESTAMP
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
                data['margin'], data['sharpe'], data['fitness'], data['is_start_date'],
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
                data['checks'], data['competitions'], data['pyramids'], data['themes']
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
                margin='%s', sharpe='%s', fitness='%s', is_start_date='%s',
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
                checks='%s', competitions='%s', pyramids='%s', themes='%s'
            )
            
            # 记录完整的SQL语句以便调试
            logger.debug(f"准备执行的完整SQL语句: {sql}")
            
            cursor.execute(sql, values)
            self.db_connection.commit()
            cursor.close()
            
            # logger.info(f"保存Alpha数据成功: {alpha_data.get('id')}")
            return True
            
        except Error as e:
            alpha_id = alpha_data.get('id', '未知ID')
            # 打印完整SQL语句以便调试
            logger.error(f"保存Alpha数据失败 (ID: {alpha_id}): {e}")
            # 在错误日志中显示实际执行的完整SQL语句
            logger.error(f"完整SQL语句: {sql}")
            return False
    
    def get_alphas_page(self, limit: int = 100, offset: int = 0, filters: Optional[Dict] = None) -> Optional[Dict]:
        """获取一页Alpha数据"""
        if not self.is_authenticated:
            logger.error("未认证，请先调用authenticate方法")
            return None
        
        try:
            # 构建基础URL
            api_url = f"{self.base_url}/users/self/alphas?limit={limit}&offset={offset}&hidden=false&order=-dateCreated"
            
            # 添加过滤条件
            if filters:
                for key, value in filters.items():
                    # 正确处理参数键编码：只对>和<进行编码
                    # 需要特别处理>=和<=的情况，只编码>和<字符，保留=作为键值分隔符
                    if '>=' in key:
                        encoded_key = key.replace('>=', '%3E')
                    elif '<=' in key:
                        encoded_key = key.replace('<=', '%3C')
                    else:
                        encoded_key = key.replace('>', '%3E').replace('<', '%3C')
                    # 只对值中的逗号进行编码
                    encoded_value = str(value).replace(',', '%2C')
                    api_url += f"&{encoded_key}={encoded_value}"
            
            logger.info(f"请求URL: {api_url}")
            
            logger.info(f"请求第 {offset//limit + 1} 页数据，offset: {offset}")
            
            # 添加重试机制，参考文件中的实现
            max_retries = 100
            retry_delay = 10  # 秒
            
            for attempt in range(max_retries):
                try:
                    # 构建基本的请求头
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
                        logger.info(f"获取成功，总数: {count}, 本页数量: {results_count}")
                        return data
                    elif response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", retry_delay))
                        logger.warning(f"API调用频率限制，{retry_after}秒后重试...")
                        time.sleep(retry_after)
                        continue
                    else:
                        logger.error(f"获取数据失败，状态码: {response.status_code}")
                        # 打印响应内容用于调试
                        logger.error(f"响应内容: {response.text[:500]}")
                        if attempt < max_retries - 1:
                            logger.info(f"{retry_delay}秒后重试...")
                            time.sleep(retry_delay)
                            continue
                        else:
                            return None
                            
                except Exception as e:
                    logger.error(f"获取数据请求异常 (尝试 {attempt + 1}/{max_retries}): {e}")
                    if attempt < max_retries - 1:
                        logger.info(f"{retry_delay}秒后重试...")
                        time.sleep(retry_delay)
                        continue
                    else:
                        return None
            
            return None
                
        except Exception as e:
            logger.error(f"获取数据请求异常: {e}")
            return None
    
    def crawl_alphas(self, total_limit: Optional[int] = None, 
                    filters: Optional[Dict] = None, resume_from: int = None, 
                    task_id: str = 'default', crawl_status_id: Optional[int] = None) -> bool:
        """爬取Alpha数据 - 一页一入库，支持断点续连
        
        Args:
            total_limit: 总数限制
            filters: 过滤条件
            resume_from: 断点续连起始位置
            task_id: 任务ID，用于状态记录
            crawl_status_id: 批次记录ID，如果提供则直接使用该记录
        """
        # 记录开始时间
        start_time = datetime.now()
        
        # 断点续连逻辑
        if resume_from is not None:
            offset = resume_from
            logger.info(f"断点续连: 从offset {resume_from}开始爬取... 开始时间: {start_time}")
        else:
            offset = 0
            logger.info(f"开始爬取Alpha数据... 开始时间: {start_time}")
        
        # 使用提供的批次记录ID或查找现有记录
        if crawl_status_id is not None:
            # 直接使用提供的批次记录ID
            logger.info(f"使用提供的批次记录 ID: {crawl_status_id}")
            # 更新记录状态为running，并计算duration_seconds（从start_time到当前时间）
            cursor = self.db_connection.cursor()
            sql = "UPDATE crawl_status SET status = 'running', start_time = %s, duration_seconds = TIMESTAMPDIFF(SECOND, %s, NOW()) WHERE id = %s"
            cursor.execute(sql, (start_time, start_time, crawl_status_id))
            self.db_connection.commit()
            cursor.close()
        else:
            # 如果没有提供批次记录ID，则查找现有记录
            cursor = self.db_connection.cursor()
            sql = "SELECT id FROM crawl_status WHERE task_id = %s AND batch_info = %s AND status = 'pending'"
            batch_info_json = json.dumps(filters, ensure_ascii=False) if filters else None
            cursor.execute(sql, (task_id, batch_info_json))
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                crawl_status_id = result[0]
                # 更新记录状态为running，并计算duration_seconds（从start_time到当前时间）
                cursor = self.db_connection.cursor()
                sql = "UPDATE crawl_status SET status = 'running', start_time = %s, duration_seconds = TIMESTAMPDIFF(SECOND, %s, NOW()) WHERE id = %s"
                cursor.execute(sql, (start_time, start_time, crawl_status_id))
                self.db_connection.commit()
                cursor.close()
                logger.info(f"使用现有批次记录 ID: {crawl_status_id}")
            else:
                logger.warning(f"未找到匹配的批次记录，将创建新记录")
                crawl_status_id = self.create_crawl_status(start_time, filters, task_id)
        
        # 初始化统计
        total_count = 0
        success_count = 0
        error_count = 0
        limit = 100
        
        try:
            while True:
                # 检查认证状态，如果过期则重新认证
                if not self.authenticate():
                    logger.error("认证失败，尝试重新认证")
                    # 重新认证失败则继续尝试，不停止爬取
                    continue
                
                # 获取一页数据
                page_data = self.get_alphas_page(limit, offset, filters)
                
                if not page_data:
                    logger.error(f"第 {offset//limit + 1} 页数据获取失败")
                    error_count += 1
                    break
                
                results = page_data.get('results', [])
                if not results:
                    logger.info("没有更多数据")
                    break
                
                # 处理本页数据
                page_success = 0
                page_error = 0
                
                for alpha_data in results:
                    try:
                        # 解析数据
                        parsed_data = self.parse_alpha_data(alpha_data)
                        
                        if not parsed_data:
                            logger.warning(f"数据解析失败: {alpha_data.get('id')}")
                            page_error += 1
                            continue
                        
                        # 保存到数据库
                        if self.save_alpha_to_database(parsed_data):
                            page_success += 1
                        else:
                            page_error += 1
                            
                    except Exception as e:
                        logger.error(f"处理Alpha数据异常: {e}")
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
                    logger.info("已获取所有数据")
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
            
            logger.info(f"爬取完成: 总数 {total_count}, 成功 {success_count}, 失败 {error_count}, 耗时 {duration} 秒")
            return success_count > 0
            
        except Exception as e:
            # 记录错误信息
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            error_message = str(e)
            
            # 更新爬虫状态记录为错误状态
            self.error_crawl_status(crawl_status_id, total_count, success_count, error_count, offset, end_time, duration, error_message)
            
            logger.error(f"爬取过程中发生异常: {e}")
            return False
    
    def save_crawl_status(self, total_count: int, success_count: int, error_count: int, 
                         last_offset: int, task_id: str = 'default', task_type: str = 'alpha_crawl',
                         task_params: Optional[Dict] = None) -> bool:
        """保存爬虫状态"""
        try:
            cursor = self.db_connection.cursor()
            
            # 将任务参数转换为JSON字符串
            task_params_json = json.dumps(task_params, ensure_ascii=False) if task_params else None
            
            sql = """
            INSERT INTO crawl_status (crawl_date, total_count, success_count, error_count, last_offset, status,
                                     task_id, task_type, task_params)
            VALUES (CURDATE(), %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(sql, (total_count, success_count, error_count, last_offset, 'completed',
                               task_id, task_type, task_params_json))
            self.db_connection.commit()
            cursor.close()
            
            logger.info("爬虫状态保存成功")
            return True
            
        except Error as e:
            logger.error(f"保存爬虫状态失败: {e}")
            return False
    
    def create_crawl_status(self, start_time: datetime, filters: Optional[Dict] = None,
                           task_id: str = 'default', task_type: str = 'alpha_crawl',
                           task_params: Optional[Dict] = None) -> Optional[int]:
        """创建新的爬虫状态记录"""
        try:
            cursor = self.db_connection.cursor()
            
            # 将过滤条件和任务参数转换为JSON字符串
            batch_info = json.dumps(filters, ensure_ascii=False) if filters else None
            task_params_json = json.dumps(task_params, ensure_ascii=False) if task_params else None
            
            sql = """
            INSERT INTO crawl_status (crawl_date, total_count, success_count, error_count, last_offset, status, 
                                     start_time, batch_info, task_id, task_type, task_params)
            VALUES (CURDATE(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(sql, (0, 0, 0, 0, 'running', start_time, batch_info, task_id, task_type, task_params_json))
            self.db_connection.commit()
            
            # 获取插入记录的ID
            crawl_status_id = cursor.lastrowid
            cursor.close()
            
            logger.info(f"创建爬虫状态记录成功，ID: {crawl_status_id}, 任务号: {task_id}")
            return crawl_status_id
            
        except Error as e:
            logger.error(f"创建爬虫状态记录失败: {e}")
            return None
    
    def update_crawl_status(self, crawl_status_id: int, total_count: int, success_count: int, 
                           error_count: int, last_offset: int) -> bool:
        """更新爬虫状态记录"""
        try:
            cursor = self.db_connection.cursor()
            
            sql = """
            UPDATE crawl_status 
            SET total_count = %s, success_count = %s, error_count = %s, last_offset = %s, 
                duration_seconds = TIMESTAMPDIFF(SECOND, start_time, NOW()), updated_at = NOW()
            WHERE id = %s
            """
            
            cursor.execute(sql, (total_count, success_count, error_count, last_offset, crawl_status_id))
            self.db_connection.commit()
            cursor.close()
            
            return True
            
        except Error as e:
            logger.error(f"更新爬虫状态记录失败: {e}")
            return False
    
    def complete_crawl_status(self, crawl_status_id: int, total_count: int, success_count: int, 
                             error_count: int, last_offset: int, end_time: datetime, 
                             duration: float) -> bool:
        """完成爬虫状态记录"""
        try:
            cursor = self.db_connection.cursor()
            
            sql = """
            UPDATE crawl_status 
            SET total_count = %s, success_count = %s, error_count = %s, last_offset = %s, 
                status = %s, end_time = %s, duration_seconds = %s, updated_at = NOW()
            WHERE id = %s
            """
            
            cursor.execute(sql, (total_count, success_count, error_count, last_offset, 
                               'completed', end_time, duration, crawl_status_id))
            self.db_connection.commit()
            cursor.close()
            
            logger.info(f"爬虫状态记录更新为完成状态，ID: {crawl_status_id}")
            return True
            
        except Error as e:
            logger.error(f"完成爬虫状态记录失败: {e}")
            return False
    
    def error_crawl_status(self, crawl_status_id: int, total_count: int, success_count: int, 
                          error_count: int, last_offset: int, end_time: datetime, 
                          duration: float, error_message: str) -> bool:
        """标记爬虫状态记录为错误状态"""
        try:
            cursor = self.db_connection.cursor()
            
            sql = """
            UPDATE crawl_status 
            SET total_count = %s, success_count = %s, error_count = %s, last_offset = %s, 
                status = %s, end_time = %s, duration_seconds = %s, error_message = %s, updated_at = NOW()
            WHERE id = %s
            """
            
            cursor.execute(sql, (total_count, success_count, error_count, last_offset, 
                               'error', end_time, duration, error_message, crawl_status_id))
            self.db_connection.commit()
            cursor.close()
            
            logger.info(f"爬虫状态记录更新为错误状态，ID: {crawl_status_id}")
            return True
            
        except Error as e:
            logger.error(f"错误爬虫状态记录失败: {e}")
            return False
    
    def get_latest_successful_crawl(self, task_type: str = 'alpha_crawl') -> Optional[Dict]:
        """获取最近一次成功的爬虫状态（error_count为0）"""
        try:
            cursor = self.db_connection.cursor()
            
            sql = """
            SELECT id, crawl_date, start_time, end_time, total_count, success_count, 
                   error_count, last_offset, status, task_id, task_type, task_params
            FROM crawl_status 
            WHERE error_count = 0 AND status = 'completed' AND task_type = %s
            ORDER BY end_time DESC 
            LIMIT 1
            """
            
            cursor.execute(sql, (task_type,))
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                return {
                    'id': result[0],
                    'crawl_date': result[1],
                    'start_time': result[2],
                    'end_time': result[3],
                    'total_count': result[4],
                    'success_count': result[5],
                    'error_count': result[6],
                    'last_offset': result[7],
                    'status': result[8],
                    'task_id': result[9],
                    'task_type': result[10],
                    'task_params': json.loads(result[11]) if result[11] else None
                }
            return None
            
        except Error as e:
            logger.error(f"获取最近成功爬虫状态失败: {e}")
            return None
    
    def generate_task_id(self, task_type: str = 'alpha_crawl', 
                        start_date: str = None, 
                        end_date: str = None,
                        fitness_ranges: List[str] = None) -> str:
        """生成任务号，包含任务属性及编码"""
        from datetime import datetime
        import hashlib
        
        # 基础信息
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        
        # 构建任务属性字符串
        task_attrs = f"{task_type}_{start_date or 'all'}_{end_date or 'all'}"
        if fitness_ranges:
            fitness_str = '_'.join([str(r) for r in fitness_ranges[:3]])  # 取前3个范围
            task_attrs += f"_{fitness_str}"
        
        # 生成唯一编码
        unique_str = f"{task_attrs}_{timestamp}"
        hash_code = hashlib.md5(unique_str.encode()).hexdigest()[:8]
        
        # 组合任务号
        task_id = f"{task_type}_{timestamp}_{hash_code}"
        
        logger.info(f"生成任务号: {task_id}")
        return task_id

    def close(self):
        """关闭连接"""
        if self.db_connection and self.db_connection.is_connected():
            self.db_connection.close()
            logger.info("数据库连接已关闭")

    def create_daily_batch_filters(self, start_date: str, end_date: str, 
                                 fitness_ranges: Optional[List[Tuple[str, str, str]]] = None,
                                 additional_filters: Optional[Dict] = None) -> List[Dict]:
        """
        创建每日分批过滤条件，支持fitness范围和其他条件
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            fitness_ranges: fitness范围配置，格式为[(range_name, min_value, max_value), ...]
            additional_filters: 额外的过滤条件
            
        Returns:
            每日分批过滤条件列表
        """
        try:
            # 解析日期范围
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            # 计算天数
            delta = end_dt - start_dt
            total_days = delta.days + 1
            
            if total_days <= 0:
                logger.error("日期范围无效")
                return []
            
            # 默认fitness范围配置
            if fitness_ranges is None:
                fitness_ranges = [
                    ('<-1.0', None, '-1.0'),
                    ('-1.0-0.5', '-1.0', '-0.5'),
                    ('-0.5-0.3', '-0.5', '0.3'),
                    ('-0.3-0', '-0.3', '0'),
                    ('0-0.3', '0', '0.3'),
                    ('0.3-0.5', '0.3', '0.5'),
                    ('0.5-0.7', '0.5', '0.7'),
                    ('0.7-1.0', '0.7', '1.0'),
                    ('1.0-1.5', '1.0', '1.5'),
                    ('1.5-2.0', '1.5', '2.0'),
                    ('2.0-3.0', '2.0', '3.0'),
                    ('3.0-4.0', '3.0', '4.0'),
                    ('>=4.0', '4.0', None)
                ]
            
            # 使用类级别的基础过滤条件
            base_filters = self.base_filters
            
            # 合并额外的过滤条件
            if additional_filters:
                base_filters.update(additional_filters)
            
            batch_filters = []
            
            # 为每一天创建fitness范围批次
            for day_offset in range(total_days):
                current_date = start_dt + timedelta(days=day_offset)
                date_str = current_date.strftime('%Y-%m-%d')
                
                # 为每个fitness范围创建过滤条件
                for range_name, min_val, max_val in fitness_ranges:
                    filters = base_filters.copy()
                    
                    # 添加日期过滤条件（使用正确的时区格式）
                    filters[f'dateCreated>='] = f'{date_str}T00:00:00-04:00'
                    # 结束时间应该是下一天的00:00，而不是当天的23:59
                    next_date = current_date + timedelta(days=1)
                    next_date_str = next_date.strftime('%Y-%m-%d')
                    filters[f'dateCreated<'] = f'{next_date_str}T00:00:00-04:00'
                    
                    # 添加fitness过滤条件
                    if min_val is not None and max_val is not None:
                        filters['is.fitness>='] = min_val
                        filters['is.fitness<'] = max_val
                        fitness_desc = f"fitness>={min_val} AND fitness<{max_val}"
                    elif min_val is not None:
                        filters['is.fitness>='] = min_val
                        fitness_desc = f"fitness>={min_val}"
                    elif max_val is not None:
                        filters['is.fitness<'] = max_val
                        fitness_desc = f"fitness<{max_val}"
                    else:
                        fitness_desc = "无fitness限制"
                    
                    batch_filters.append({
                        'filters': filters,
                        'date': date_str,
                        'fitness_range': range_name,
                        'fitness_min': min_val,
                        'fitness_max': max_val,
                        'description': f"{date_str} - {range_name} ({fitness_desc})"
                    })
            
            logger.info(f"创建了 {len(batch_filters)} 个分批过滤条件，覆盖 {total_days} 天，{len(fitness_ranges)} 个fitness范围")
            return batch_filters
            
        except Exception as e:
            logger.error(f"创建分批过滤条件失败: {e}")
            return []
    


def initialize_crawler() -> Optional[AlphaCrawler]:
    """初始化爬虫实例"""
    crawler = AlphaCrawler()
    
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

def check_resume_point(crawler: AlphaCrawler, resume: bool) -> Tuple[str, bool]:
    """检查断点续连点
    
    Args:
        crawler: 爬虫实例
        resume: 是否开启断点续连
        
    Returns:
        (实际任务ID, 是否继续处理现有任务)
    """
    actual_task_id = f"alpha_crawl_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    if not resume:
        logger.info("断点续连模式已禁用，使用新任务")
        return actual_task_id, False
    
    # 查找前一个主任务ID（不排除当前任务ID，因为此时还没有记录）
    cursor = crawler.db_connection.cursor()
    sql = """
    SELECT DISTINCT task_id, created_at FROM crawl_status 
    WHERE task_type = 'alpha_crawl_batch' 
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
    WHERE task_id = %s AND task_type = 'alpha_crawl_batch'
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

def process_batch_data(crawler: AlphaCrawler, task_id: str, total_limit: int) -> Tuple[int, int]:
    """处理分批数据
    
    Args:
        crawler: 爬虫实例
        task_id: 任务ID
        total_limit: 每批数据量限制
        
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
        logger.info(f"过滤条件: {batch_info.get('filters', {})}")
        
        # 使用从数据库读取的批次信息进行爬取，直接传递批次记录ID
        success = crawler.crawl_alphas(total_limit=total_limit, filters=batch_info.get('filters', {}), task_id=task_id, crawl_status_id=record[0])
        
        if success:
            logger.info(f"第 {i+1} 批数据爬取成功")
            total_success += 1
        else:
            logger.error(f"第 {i+1} 批数据爬取失败")
            total_error += 1
        
        # 批次间延迟
        time.sleep(5)
    
    return total_success, total_error

def main(start_date: str = "2025-08-28", 
          end_date: str = None,
          total_limit: int = 10000,
          resume: bool = True):
    """主函数，支持断点续连
    
    Args:
        start_date: 开始日期，格式: 2025-05-10
        end_date: 结束日期，格式: 2025-10-24，如果为None则动态获取明天日期
        total_limit: 每批数据量限制
        resume: 是否断点续连，默认开启
    """
    # 如果end_date为None，则动态计算明天的日期
    if end_date is None:
        from datetime import datetime, timedelta
        tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        end_date = tomorrow
        logger.info(f"使用动态计算的结束日期: {end_date}")
    
    logger.info("=== WorldQuant Alpha数据爬虫启动 ===")
    logger.info(f"时间范围: {start_date} 到 {end_date}")
    logger.info(f"断点续连模式: {'启用' if resume else '禁用'}")
    
    # 初始化爬虫
    crawler = initialize_crawler()
    if not crawler:
        logger.error("爬虫初始化失败，程序退出")
        return False
    
    try:
        # 断点续连检查
        actual_task_id, should_resume = check_resume_point(crawler, resume)
        
        if should_resume:
            # 直接进入步骤6，跳过步骤4和5（断点续连模式）
            logger.info("步骤6: 开始分批爬取数据（断点续连模式）")
            total_success, total_error = process_batch_data(crawler, actual_task_id, total_limit)
            
            # 输出结果
            if total_success > 0:
                logger.info(f"=== 爬虫执行成功，成功批次: {total_success}, 失败批次: {total_error} ===")
            else:
                logger.error(f"=== 爬虫执行失败，成功批次: {total_success}, 失败批次: {total_error} ===")
            
            return total_success > 0
        
        # 如果没有断点续连或前一个任务已完成，则正常执行步骤4和5
        # 创建每日分批过滤条件
        logger.info("步骤4: 创建每日分批过滤条件")
        start_date_only = start_date.split('T')[0]
        end_date_only = end_date.split('T')[0]
        batch_filters = crawler.create_daily_batch_filters(start_date_only, end_date_only)
        
        if not batch_filters:
            logger.error("创建分批过滤条件失败")
            return False
        
        # 将所有批次过滤条件写入爬虫记录表
        logger.info("步骤5: 将批次过滤条件写入爬虫记录表")
        
        for i, batch_info in enumerate(batch_filters):
            batch_info_json = json.dumps(batch_info, ensure_ascii=False)
            
            # 创建批次记录到crawl_status表
            cursor = crawler.db_connection.cursor()
            sql = """
            INSERT INTO crawl_status (crawl_date, total_count, success_count, error_count, last_offset, status, 
                                     batch_info, task_id, task_type, task_params)
            VALUES (CURDATE(), %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (0, 0, 0, 0, 'pending', batch_info_json, actual_task_id, 'alpha_crawl_batch', None))
            crawler.db_connection.commit()
            cursor.close()
            
            logger.info(f"批次 {i+1} 已记录到爬虫记录表: {batch_info.get('description', '未知批次')}")
        
        logger.info(f"共 {len(batch_filters)} 个批次过滤条件已写入爬虫记录表，主任务ID: {actual_task_id}")
        
        # 分批爬取数据
        logger.info("步骤6: 开始分批爬取数据")
        total_success, total_error = process_batch_data(crawler, actual_task_id, total_limit)
        
        # 输出结果
        if total_success > 0:
            logger.info(f"=== 爬虫执行成功，成功批次: {total_success}, 失败批次: {total_error} ===")
        else:
            logger.error(f"=== 爬虫执行失败，成功批次: {total_success}, 失败批次: {total_error} ===")
        
        return total_success > 0
        
    except Exception as e:
        logger.error(f"爬虫执行异常: {e}")
        return False
    
    finally:
        # 关闭连接
        crawler.close()
        logger.info("=== 爬虫执行结束 ===")

if __name__ == "__main__":
    # 执行主函数
    success = main()
    
    # 根据执行结果退出
    sys.exit(0 if success else 1)
