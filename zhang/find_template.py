#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BRAIN Alpha 模板发现工具 (独立版)
========================================
完全独立的模板发现脚本，无需任何外部依赖模块

功能:
- 搜索 WorldQuant BRAIN 论坛中的 Alpha 表达式模板
- 提取和解析模板变量
- 展开模板生成表达式组合
- 保存模板到本地 JSON 文件

依赖 (pip install):
- selenium
- beautifulsoup4
- requests
- Chrome 或 Edge 浏览器

使用方法:
    python template_discovery_standalone.py

作者: GC13416
版本: 1.0.0 (独立版)
"""

import asyncio
import json
import os
import re
import sys
import time
from datetime import datetime
from itertools import product
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote

# ============================================================
# 颜色输出
# ============================================================
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


def print_header(text: str):
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'='*60}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{text.center(60)}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'='*60}{Colors.ENDC}\n")


def print_success(text: str):
    print(f"{Colors.GREEN}✅ {text}{Colors.ENDC}")


def print_error(text: str):
    print(f"{Colors.RED}❌ {text}{Colors.ENDC}")


def print_warning(text: str):
    print(f"{Colors.YELLOW}⚠️  {text}{Colors.ENDC}")


def print_info(text: str):
    print(f"{Colors.CYAN}ℹ️  {text}{Colors.ENDC}")


def print_step(text: str):
    print(f"{Colors.BLUE}🔹 {text}{Colors.ENDC}")


def log(message: str, level: str = "INFO"):
    """日志输出"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")


# ============================================================
# 配置
# ============================================================
FORUM_BASE_URL = "https://support.worldquantbrain.com"
DEFAULT_OUTPUT_FILE = "discovered_templates.json"

# 常用搜索关键词
SEARCH_KEYWORDS = [
    "alpha expression template",
    "alpha formula",
    "rank expression",
    "ts_delta alpha",
    "group_rank",
    "decay_linear",
    "neutralization",
    "sharpe ratio",
]

# 常用变量值
DEFAULT_VARIABLE_VALUES = {
    "days": [5, 10, 20, 60, 120, 252],
    "lookback": [5, 10, 20, 60],
    "decay": [0, 3, 5, 10],
    "field": ["close", "open", "high", "low", "volume", "vwap", "returns"],
    "operator": ["rank", "zscore", "scale"],
    "ts_op": ["ts_mean", "ts_std", "ts_sum", "ts_max", "ts_min", "ts_delta"],
    "group": ["sector", "industry", "subindustry", "market"],
}


# ============================================================
# 论坛客户端 (内置完整实现)
# ============================================================
class ForumClient:
    """论坛客户端 - 完整独立实现"""
   
    def __init__(self):
        self.base_url = FORUM_BASE_URL
        self.driver = None
       
    def detect_browser(self) -> str:
        """检测可用的浏览器"""
        try:
            from selenium import webdriver
           
            # 尝试 Chrome
            try:
                from selenium.webdriver.chrome.options import Options
                options = Options()
                options.add_argument('--headless=new')
                options.add_argument('--log-level=3')
                driver = webdriver.Chrome(options=options)
                driver.quit()
                return "chrome"
            except Exception:
                pass
           
            # 尝试 Edge
            try:
                from selenium.webdriver.edge.options import Options as EdgeOptions
                options = EdgeOptions()
                options.add_argument('--headless=new')
                options.add_argument('--log-level=3')
                driver = webdriver.Edge(options=options)
                driver.quit()
                return "edge"
            except Exception:
                pass
           
            return "chrome"
        except Exception:
            return "chrome"
   
    def create_driver(self, headless: bool = True):
        """创建浏览器驱动"""
        from selenium import webdriver
       
        browser = self.detect_browser()
        log(f"使用浏览器: {browser}", "INFO")
       
        try:
            if browser == "chrome":
                from selenium.webdriver.chrome.options import Options
                options = Options()
                if headless:
                    options.add_argument('--headless=new')
                options.add_argument('--disable-blink-features=AutomationControlled')
                options.add_argument('--no-sandbox')
                options.add_argument('--disable-dev-shm-usage')
                options.add_argument('--disable-gpu')
                options.add_argument('--log-level=3')
                options.add_argument('--window-size=1920,1080')
                options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
                self.driver = webdriver.Chrome(options=options)
            else:
                from selenium.webdriver.edge.options import Options as EdgeOptions
                options = EdgeOptions()
                if headless:
                    options.add_argument('--headless=new')
                options.add_argument('--disable-blink-features=AutomationControlled')
                options.add_argument('--no-sandbox')
                options.add_argument('--disable-dev-shm-usage')
                options.add_argument('--disable-gpu')
                options.add_argument('--log-level=3')
                options.add_argument('--window-size=1920,1080')
                options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
                self.driver = webdriver.Edge(options=options)
           
            self.driver.set_page_load_timeout(30)
            self.driver.implicitly_wait(10)
            return self.driver
           
        except Exception as e:
            log(f"浏览器启动失败: {e}", "ERROR")
            raise
   
    async def login(self, email: str, password: str) -> bool:
        """登录论坛"""
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
       
        try:
            log("正在登录论坛...", "WORK")
           
            # 访问登录页面
            self.driver.get(f"{self.base_url}/hc/en-us/signin")
            await asyncio.sleep(3)
           
            # 尝试多种输入框选择器
            email_selectors = [
                (By.NAME, "email"),
                (By.ID, "email"),
                (By.CSS_SELECTOR, "input[type='email']"),
            ]
           
            email_input = None
            for selector in email_selectors:
                try:
                    email_input = WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located(selector)
                    )
                    if email_input:
                        break
                except:
                    continue
           
            if not email_input:
                log("找不到邮箱输入框", "ERROR")
                return False
           
            # 密码输入框
            password_selectors = [
                (By.NAME, "currentPassword"),
                (By.NAME, "password"),
                (By.ID, "password"),
                (By.CSS_SELECTOR, "input[type='password']"),
            ]
           
            password_input = None
            for selector in password_selectors:
                try:
                    password_input = self.driver.find_element(*selector)
                    if password_input:
                        break
                except:
                    continue
           
            if not password_input:
                log("找不到密码输入框", "ERROR")
                return False
           
            # 输入凭据
            email_input.clear()
            email_input.send_keys(email)
            await asyncio.sleep(0.5)
           
            password_input.clear()
            password_input.send_keys(password)
            await asyncio.sleep(0.5)
           
            # 提交
            submit_selectors = [
                (By.XPATH, '//button[@type="submit"]'),
                (By.CSS_SELECTOR, 'button[type="submit"]'),
                (By.CSS_SELECTOR, 'input[type="submit"]'),
            ]
           
            for selector in submit_selectors:
                try:
                    submit_btn = self.driver.find_element(*selector)
                    if submit_btn:
                        submit_btn.click()
                        break
                except:
                    continue
           
            await asyncio.sleep(3)
           
            # 检查登录状态
            current_url = self.driver.current_url
            if 'signin' not in current_url.lower():
                log("登录成功!", "SUCCESS")
                return True
           
            log("登录可能失败，继续尝试...", "WARNING")
            return True  # 继续尝试，有些页面可能不需要登录
           
        except Exception as e:
            log(f"登录过程出错: {e}", "ERROR")
            return False
   
    async def search(self, query: str, email: str, password: str, max_results: int = 20, headless: bool = True) -> List[Dict[str, Any]]:
        """搜索论坛"""
        from bs4 import BeautifulSoup
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
       
        results = []
       
        try:
            # 创建浏览器
            self.create_driver(headless)
           
            # 登录
            await self.login(email, password)
           
            # 搜索
            encoded_query = quote(query)
            search_url = f"{self.base_url}/hc/zh-cn/search?utf8=%E2%9C%93&query={encoded_query}"
            log(f"搜索: {search_url}", "WORK")
           
            self.driver.get(search_url)
            await asyncio.sleep(2)
           
            # 等待搜索结果
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '.search-results-list, .search-result-list-item'))
                )
            except:
                log("未找到搜索结果", "WARNING")
           
            # 提取结果
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
           
            # 尝试多种选择器
            container_selectors = [
                '.search-result-list-item',
                '.search-results-list .search-result',
                '.striped-list-item',
            ]
           
            items = []
            for selector in container_selectors:
                items = soup.select(selector)
                if items:
                    log(f"使用选择器 {selector} 找到 {len(items)} 个结果", "INFO")
                    break
           
            for idx, item in enumerate(items[:max_results]):
                try:
                    # 标题和链接
                    title_selectors = ['.search-result-title a', 'h3 a', '.title a', 'a']
                    title_elem = None
                    for sel in title_selectors:
                        title_elem = item.select_one(sel)
                        if title_elem and title_elem.get_text(strip=True):
                            break
                   
                    title = title_elem.get_text(strip=True) if title_elem else 'No title'
                    link = title_elem.get('href', '') if title_elem else ''
                    if link and not link.startswith('http'):
                        link = f"{self.base_url}{link}"
                   
                    # 描述
                    desc_elem = item.select_one('.search-results-description, .description, p')
                    description = desc_elem.get_text(strip=True) if desc_elem else ''
                   
                    results.append({
                        'title': title,
                        'link': link,
                        'description': description,
                        'index': idx
                    })
                except Exception:
                    continue
           
            log(f"搜索完成，找到 {len(results)} 个结果", "SUCCESS")
            return results
           
        except Exception as e:
            log(f"搜索失败: {e}", "ERROR")
            return results
        finally:
            self.close()
   
    async def read_post(self, url: str, email: str, password: str, headless: bool = True) -> Dict[str, Any]:
        """读取帖子内容"""
        from bs4 import BeautifulSoup
       
        try:
            # 创建浏览器
            self.create_driver(headless)
           
            # 登录
            await self.login(email, password)
           
            # 读取帖子
            log(f"读取帖子: {url}", "WORK")
            self.driver.get(url)
            await asyncio.sleep(2)
           
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
           
            # 提取标题
            title_elem = soup.select_one('.post-title, h1, .article-title, title')
            title = title_elem.get_text(strip=True) if title_elem else 'Unknown'
           
            # 提取内容
            content_elem = soup.select_one('.post-body, .article-body, .content, article, main')
            content_html = str(content_elem) if content_elem else ''
            content_text = content_elem.get_text(strip=True) if content_elem else ''
           
            log(f"帖子读取完成: {title[:30]}...", "SUCCESS")
           
            return {
                'title': title,
                'url': url,
                'content_html': content_html,
                'content_text': content_text
            }
           
        except Exception as e:
            log(f"读取帖子失败: {e}", "ERROR")
            return {'error': str(e)}
        finally:
            self.close()
   
    def close(self):
        """关闭浏览器"""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None


# ============================================================
# 模板解析器
# ============================================================
class TemplateParser:
    """模板解析器 - 提取和处理表达式模板"""
   
    # Alpha 表达式模式
    EXPRESSION_PATTERNS = [
        # 函数调用模式: func(...)
        r'\b(rank|ts_rank|group_rank|zscore|scale|decay_linear|ts_delta|ts_mean|ts_std|ts_sum|ts_max|ts_min|ts_argmax|ts_argmin|ts_corr|ts_covariance|ts_regression|ts_skewness|ts_kurtosis|group_mean|group_sum|group_max|group_min|group_zscore|group_neutralize|signed_power|abs|log|sqrt|power|min|max|sum|mean|std|correlation|covariance|delta|delay|product|vec_sum|vec_avg|vec_max|vec_min|vec_stddev|vec_norm|vec_count|vec_ir|vec_skewness|vec_kurtosis|vec_choose|vec_range|trade_when|if_else|clamp|winsorize|truncate|pasteurize|filter|bucket|step|sigmoid|tanh|sign|ceil|floor|round|fraction|nan_mask|nan_out|keep|densify|hump|jump|tail|ts_product|ts_ir|ts_moment|ts_entropy|ts_hump|ts_decay_exp_window|ts_decay_linear|ts_av_diff|ts_percentage|ts_returns|ts_zscore|ts_scale|ts_count_nans|ts_weighted_delay|ts_arg_max|ts_arg_min|ts_triple_corr|ts_quantile|ts_step|ts_operation|ts_theilsen|ts_median|ts_co_kurtosis|ts_co_skewness|ts_backfill|ts_rank_gmean_amean_diff|ts_rank_gmean_amean_diff|inst_tvr|market_cap|adv|close|open|high|low|volume|vwap|returns|sharesout|cap|sector|industry|subindustry)\s*\(',
        # 带模板变量的表达式: <variable/>
        r'[a-zA-Z_][a-zA-Z0-9_]*\s*\(',
        # 简单算术表达式
        r'(?:rank|ts_|group_)[a-zA-Z_]+\s*\(\s*[-+*/]\s*(?:rank|ts_|group_)[a-zA-Z_]+\s*\(',
    ]
   
    # 模板变量模式
    VARIABLE_PATTERN = r'<([a-zA-Z_][a-zA-Z0-9_]*)/>'
   
    def extract_expressions(self, text: str) -> List[str]:
        """从文本中提取 Alpha 表达式"""
        expressions = []
       
        for pattern in self.EXPRESSION_PATTERNS:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    match = match[0]
                expr = match.strip()
                if expr and len(expr) > 5 and expr not in expressions:
                    # 验证表达式
                    if self._is_valid_expression(expr):
                        expressions.append(expr)
       
        # 查找代码块中的表达式
        code_blocks = re.findall(r'```[^`]*```|`[^`]+`', text)
        for block in code_blocks:
            block_text = block.strip('`').strip()
            for pattern in self.EXPRESSION_PATTERNS:
                matches = re.findall(pattern, block_text, re.IGNORECASE)
                for match in matches:
                    if isinstance(match, tuple):
                        match = match[0]
                    expr = match.strip()
                    if expr and len(expr) > 5 and expr not in expressions:
                        if self._is_valid_expression(expr):
                            expressions.append(expr)
       
        return expressions
   
    def _is_valid_expression(self, expr: str) -> bool:
        """验证表达式是否有效"""
        # 检查括号匹配
        open_count = expr.count('(')
        close_count = expr.count(')')
        if open_count != close_count:
            return False
       
        # 检查是否包含常见函数
        common_funcs = ['rank', 'ts_', 'group_', 'zscore', 'scale', 'decay', 'delta', 'mean', 'std', 'sum', 'max', 'min']
        has_func = any(func in expr.lower() for func in common_funcs)
       
        return has_func
   
    def extract_variables(self, expression: str) -> List[str]:
        """提取模板变量"""
        return re.findall(self.VARIABLE_PATTERN, expression)
   
    def expand_template(self, expression: str, variable_values: Dict[str, List[Any]]) -> List[str]:
        """展开模板生成所有组合"""
        variables = self.extract_variables(expression)
       
        if not variables:
            return [expression]
       
        # 获取每个变量的值列表
        value_lists = []
        for var in variables:
            if var in variable_values:
                value_lists.append(variable_values[var])
            elif var in DEFAULT_VARIABLE_VALUES:
                value_lists.append(DEFAULT_VARIABLE_VALUES[var])
            else:
                value_lists.append([var])  # 保持原样
       
        # 生成所有组合
        expanded = []
        for combo in product(*value_lists):
            result = expression
            for var, val in zip(variables, combo):
                result = result.replace(f'<{var}/>', str(val))
            expanded.append(result)
       
        return expanded
   
    def analyze_expression(self, expression: str) -> Dict[str, Any]:
        """分析表达式结构"""
        variables = self.extract_variables(expression)
       
        # 提取使用的函数
        func_pattern = r'\b([a-zA-Z_][a-zA-Z0-9_]*)\s*\('
        functions = list(set(re.findall(func_pattern, expression)))
       
        # 计算嵌套深度
        max_depth = 0
        current_depth = 0
        for char in expression:
            if char == '(':
                current_depth += 1
                max_depth = max(max_depth, current_depth)
            elif char == ')':
                current_depth -= 1
       
        return {
            'expression': expression,
            'variables': variables,
            'functions': functions,
            'nesting_depth': max_depth,
            'length': len(expression),
            'is_template': len(variables) > 0
        }


# ============================================================
# 模板管理器
# ============================================================
class TemplateManager:
    """模板管理器 - 保存和加载模板"""
   
    def __init__(self, output_file: str = DEFAULT_OUTPUT_FILE):
        self.output_file = output_file
        self.templates: List[Dict[str, Any]] = []
        self.load()
   
    def load(self):
        """加载已保存的模板"""
        if os.path.exists(self.output_file):
            try:
                with open(self.output_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.templates = data.get('templates', [])
                    print_info(f"已加载 {len(self.templates)} 个模板")
            except Exception as e:
                print_warning(f"加载模板失败: {e}")
                self.templates = []
   
    def save(self):
        """保存模板到文件"""
        try:
            data = {
                'templates': self.templates,
                'updated_at': datetime.now().isoformat(),
                'count': len(self.templates)
            }
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print_success(f"已保存 {len(self.templates)} 个模板到 {self.output_file}")
        except Exception as e:
            print_error(f"保存模板失败: {e}")
   
    def add_template(self, template: Dict[str, Any]) -> bool:
        """添加模板"""
        # 检查重复
        expr = template.get('expression', '')
        for t in self.templates:
            if t.get('expression') == expr:
                print_warning("模板已存在")
                return False
       
        template['added_at'] = datetime.now().isoformat()
        self.templates.append(template)
        return True
   
    def list_templates(self):
        """列出所有模板"""
        if not self.templates:
            print_info("暂无保存的模板")
            return
       
        print(f"\n{Colors.BOLD}已保存的模板 ({len(self.templates)} 个):{Colors.ENDC}\n")
       
        parser = TemplateParser()
        for idx, t in enumerate(self.templates, 1):
            expr = t.get('expression', '')
            analysis = parser.analyze_expression(expr)
           
            print(f"  {idx}. {expr[:60]}{'...' if len(expr) > 60 else ''}")
            if analysis['variables']:
                print(f"     变量: {', '.join(analysis['variables'])}")
            print()


# ============================================================
# 主程序
# ============================================================
class TemplateDiscovery:
    """模板发现主程序"""
   
    def __init__(self):
        self.forum = ForumClient()
        self.parser = TemplateParser()
        self.manager = TemplateManager()
        self.email = ""
        self.password = ""
   
    def get_credentials(self) -> bool:
        """获取用户凭据"""
        print_header("BRAIN 论坛登录")
       
        # 尝试从配置文件读取
        config_paths = [
            Path(__file__).parent / 'user_config.json',
            Path(__file__).parent / 'brain_credentials.json',
            Path.home() / '.brain_credentials',
        ]
       
        for config_path in config_paths:
            if config_path.exists():
                try:
                    with open(config_path, 'r', encoding='utf-8') as f:
                        config = json.load(f)
                        email = config.get('email') or config.get('credentials', {}).get('email')
                        password = config.get('password') or config.get('credentials', {}).get('password')
                        if email and password:
                            print_info(f"从 {config_path.name} 读取凭据")
                            use_saved = input(f"使用已保存的账号 ({email})? [Y/n]: ").strip().lower()
                            if use_saved != 'n':
                                self.email = email
                                self.password = password
                                return True
                except Exception:
                    continue
       
        # 手动输入
        print_info("请输入 BRAIN 账号信息:")
        self.email = input("邮箱: ").strip()
        self.password = input("密码: ").strip()
       
        if not self.email or not self.password:
            print_error("邮箱和密码不能为空")
            return False
       
        return True
   
    async def search_and_extract(self, query: str) -> List[Dict[str, Any]]:
        """搜索并提取模板"""
        templates = []
       
        # 询问是否使用无头模式
        use_headless = input("使用无头模式 (不显示浏览器窗口)? [Y/n]: ").strip().lower()
        headless = use_headless != 'n'
       
        # 搜索
        results = await self.forum.search(query, self.email, self.password, max_results=10, headless=headless)
       
        if not results:
            return templates
       
        # 显示搜索结果
        print(f"\n{Colors.BOLD}搜索结果:{Colors.ENDC}\n")
        for idx, r in enumerate(results, 1):
            print(f"  {idx}. {r['title'][:50]}...")
       
        # 选择要读取的帖子
        print()
        selection = input("输入要读取的帖子编号 (多个用逗号分隔, 回车跳过): ").strip()
       
        if not selection:
            return templates
       
        indices = [int(i.strip()) - 1 for i in selection.split(',') if i.strip().isdigit()]
       
        for idx in indices:
            if 0 <= idx < len(results):
                result = results[idx]
                print()
               
                # 读取帖子
                post = await self.forum.read_post(result['link'], self.email, self.password, headless=headless)
               
                if 'error' in post:
                    continue
               
                # 提取表达式
                expressions = self.parser.extract_expressions(post['content_text'])
               
                if expressions:
                    print_success(f"从 \"{post['title'][:30]}...\" 提取到 {len(expressions)} 个表达式")
                   
                    for expr in expressions:
                        print(f"    • {expr[:60]}...")
                       
                        # 询问是否保存
                        save = input("      保存此模板? [y/N]: ").strip().lower()
                        if save == 'y':
                            template = {
                                'expression': expr,
                                'source': result['link'],
                                'source_title': post['title'],
                            }
                            if self.manager.add_template(template):
                                print_success("      已添加!")
                else:
                    print_warning(f"未在帖子中找到表达式")
       
        return templates
   
    async def manual_add(self):
        """手动添加模板"""
        print_header("手动添加模板")
       
        print_info("输入表达式 (支持 <variable/> 格式的模板变量)")
        print_info("示例: rank(ts_delta(close, <days/>))")
        print()
       
        expression = input("表达式: ").strip()
       
        if not expression:
            return
       
        # 分析表达式
        analysis = self.parser.analyze_expression(expression)
       
        print()
        print_info(f"函数: {', '.join(analysis['functions'])}")
        print_info(f"嵌套深度: {analysis['nesting_depth']}")
       
        if analysis['variables']:
            print_info(f"模板变量: {', '.join(analysis['variables'])}")
       
        # 确认保存
        save = input("\n保存此模板? [Y/n]: ").strip().lower()
        if save != 'n':
            template = {
                'expression': expression,
                'source': 'manual',
                'analysis': analysis
            }
            if self.manager.add_template(template):
                print_success("模板已添加!")
   
    async def run(self):
        """运行主程序"""
        print_header("BRAIN Alpha 模板发现工具")
       
        while True:
            print("\n请选择操作:\n")
            print("  1. 🔍 搜索论坛模板")
            print("  2. ➕ 手动添加模板")
            print("  3. 📋 查看已保存模板")
            print("  4. 💾 保存并退出")
            print("  0. ❌ 退出 (不保存)")
            print()
           
            choice = input("选择 [0-4]: ").strip()
           
            if choice == '1':
                # 搜索论坛
                if not self.email:
                    if not self.get_credentials():
                        continue
               
                # 选择搜索关键词
                print()
                print_info("常用搜索关键词:")
                for idx, kw in enumerate(SEARCH_KEYWORDS, 1):
                    print(f"  {idx}. {kw}")
                print()
               
                kw_choice = input("选择关键词编号或输入自定义关键词: ").strip()
               
                if kw_choice.isdigit() and 1 <= int(kw_choice) <= len(SEARCH_KEYWORDS):
                    query = SEARCH_KEYWORDS[int(kw_choice) - 1]
                else:
                    query = kw_choice
               
                if query:
                    await self.search_and_extract(query)
                   
            elif choice == '2':
                await self.manual_add()
               
            elif choice == '3':
                self.manager.list_templates()
               
            elif choice == '4':
                self.manager.save()
                print_success("再见!")
                break
               
            elif choice == '0':
                print_info("退出 (未保存)")
                break
           
            else:
                print_warning("无效选择")


def main():
    """主入口"""
    try:
        discovery = TemplateDiscovery()
        asyncio.run(discovery.run())
    except KeyboardInterrupt:
        print_info("\n用户中断")
    except Exception as e:
        print_error(f"程序错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
