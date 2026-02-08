import pandas as pd
import json
import time
import os
from datetime import datetime
from machine_lib import login

class AlphaDetailFetcher:
    """Alpha详细信息获取器 - 修复相关性API"""
   
    _session = None
    _session_time = None
   
    @classmethod
    def get_session(cls):
        """获取或创建会话"""
        if cls._session is None or cls._session_time is None:
            cls._session = login()
            cls._session_time = time.time()
            print("🔄 创建新会话")
        else:
            elapsed = time.time() - cls._session_time
            if elapsed > 4 * 3600:
                print("🔄 会话过期，创建新会话")
                cls._session = login()
                cls._session_time = time.time()
            else:
                print(f"♻️ 复用会话 (已使用 {elapsed/3600:.1f} 小时)")
       
        return cls._session
   
    def __init__(self):
        """初始化获取器"""
        self.brain_api_url = "https://api.worldquantbrain.com"
        self.session = self.get_session()
   
    def get_alpha_details(self, alpha_id):
        """获取Alpha详细信息"""
        print(f"📡 获取Alpha: {alpha_id}")
       
        # 获取基本数据
        base_details = self._get_base_alpha_data(alpha_id)
        if not base_details:
            return None
       
        # 获取相关性数据 - 使用稳健的方法
        correlation_data = self._get_correlation_data_robust(alpha_id)
       
        # 合并数据
        if correlation_data:
            base_details.update(correlation_data)
       
        return base_details
   
    def _get_base_alpha_data(self, alpha_id):
        """获取基本Alpha数据"""
        try:
            response = self.session.get(f"{self.brain_api_url}/alphas/{alpha_id}")
           
            if response.status_code == 200:
                alpha_data = response.json()
                print("✅ 基础数据获取成功")
                return self._parse_base_data(alpha_data)
            elif response.status_code == 404:
                print(f"❌ Alpha {alpha_id} 不存在")
                return None
            else:
                print(f"⚠️ 基础数据请求失败 ({response.status_code})")
                return None
               
        except Exception as e:
            print(f"❌ 基础数据请求出错: {str(e)}")
            return None
   
    def _get_correlation_data_robust(self, alpha_id):
        """稳健获取相关性数据"""
        correlation_data = {}
       
        # 逐个获取相关性数据，添加延迟避免请求过快
        correlation_data['Self_Correlation'] = self._get_correlation_with_retry(alpha_id, "self")
        time.sleep(0.5)  # 添加延迟
       
        correlation_data['Power_Pool_Correlation'] = self._get_correlation_with_retry(alpha_id, "power-pool")
        time.sleep(0.5)  # 添加延迟
       
        correlation_data['Prod_Correlation'] = self._get_correlation_with_retry(alpha_id, "prod")
       
        return correlation_data
   
    def _get_correlation_with_retry(self, alpha_id, corr_type):
        """带重试的获取相关性值"""
        url = f"{self.brain_api_url}/alphas/{alpha_id}/correlations/{corr_type}"
       
        for retry in range(3):
            try:
                # 添加User-Agent和其他头信息
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'application/json',
                    'Accept-Language': 'en-US,en;q=0.9',
                }
               
                response = self.session.get(url, headers=headers, timeout=30)
               
                print(f"🔍 {corr_type.upper()} API状态: {response.status_code}")
               
                if response.status_code == 200:
                    content = response.text.strip()
                   
                    if not content or content == "null":
                        print(f"⚠️  {corr_type.upper()}: 空响应")
                        continue  # 继续重试
                   
                    # 尝试多种方式解析
                    value = self._extract_correlation_value(content, corr_type)
                    if value is not None:
                        print(f"✅  {corr_type.upper()}: {value}")
                        return value
                    else:
                        print(f"⚠️  {corr_type.upper()}: 无法提取值, 内容: {content[:100]}")
               
                elif response.status_code == 404:
                    print(f"⚠️  {corr_type.upper()} API不存在 (404)")
                    break  # 不需要重试
               
                elif response.status_code == 429:
                    retry_after = response.headers.get('Retry-After', 10)
                    wait_time = int(retry_after) + 5
                    print(f"⏳  {corr_type.upper()} 速率限制，等待 {wait_time} 秒")
                    time.sleep(wait_time)
                    continue
               
                else:
                    print(f"⚠️  {corr_type.upper()} 请求失败 ({response.status_code})")
               
                # 重试前等待
                time.sleep(2 * (retry + 1))
               
            except Exception as e:
                print(f"❌  {corr_type.upper()} 请求出错: {str(e)}")
                time.sleep(3)
       
        print(f"❌  获取{corr_type.upper()}失败")
        return 'N/A'
   
    def _extract_correlation_value(self, content, corr_type):
        """从响应内容中提取相关性值"""
        # 方法1: 尝试解析JSON
        try:
            data = json.loads(content)
           
            # 常见的数据结构
            if isinstance(data, dict):
                # 尝试多个可能的键
                for key in ['value', 'max', 'correlation', 'corr', 'result']:
                    if key in data and data[key] is not None:
                        value = data[key]
                        if isinstance(value, (int, float)):
                            return float(value)
               
                # 尝试查找任何数值字段
                for key, val in data.items():
                    if isinstance(val, (int, float)):
                        return float(val)
           
            elif isinstance(data, (int, float)):
                return float(data)
           
            elif isinstance(data, list) and len(data) > 0:
                # 如果是列表，取第一个元素
                first_item = data[0]
                if isinstance(first_item, dict):
                    for key in ['value', 'max', 'correlation']:
                        if key in first_item and first_item[key] is not None:
                            value = first_item[key]
                            if isinstance(value, (int, float)):
                                return float(value)
                elif isinstance(first_item, (int, float)):
                    return float(first_item)
       
        except json.JSONDecodeError:
            pass  # 不是JSON，尝试其他方法
       
        # 方法2: 尝试直接提取数字
        try:
            # 如果是纯数字
            return float(content)
        except:
            pass
       
        # 方法3: 使用正则表达式提取数字
        import re
       
        # 查找小数或整数
        matches = re.findall(r'-?\d+\.?\d*', content)
        if matches:
            try:
                # 取第一个匹配的数字
                return float(matches[0])
            except:
                pass
       
        # 方法4: 查找百分比
        percent_match = re.search(r'(\d+\.?\d*)%', content)
        if percent_match:
            try:
                # 百分比转换为小数
                return float(percent_match.group(1)) / 100
            except:
                pass
       
        return None
   
    def _parse_base_data(self, alpha_data):
        """解析基础Alpha数据"""
        details = {}
       
        try:
            # 1. 基本信息
            details['Alpha_ID'] = alpha_data.get('id', 'N/A')
            details['Code'] = alpha_data.get('regular', {}).get('code', 'N/A')
            details['Color'] = alpha_data.get('color', 'None')
            details['Status'] = alpha_data.get('status', 'N/A')
            details['Date_Created'] = alpha_data.get('dateCreated', 'N/A')
           
            # 2. 模拟设置
            settings = alpha_data.get('settings', {})
            details['Region'] = settings.get('region', 'N/A')
            details['Universe'] = settings.get('universe', 'N/A')
            details['Delay'] = settings.get('delay', 'N/A')
            details['Decay'] = settings.get('decay', 'N/A')
            details['Neutralization'] = settings.get('neutralization', 'N/A')
            details['Truncation'] = settings.get('truncation', 'N/A')
            details['Start_Date'] = settings.get('startDate', 'N/A')
            details['End_Date'] = settings.get('endDate', 'N/A')
           
            # 3. IS Summary数据
            is_data = alpha_data.get('is', {})
            details['IS_Sharpe'] = is_data.get('sharpe', 0)
            details['IS_Turnover'] = is_data.get('turnover', 0)
            details['IS_Fitness'] = is_data.get('fitness', 0)
            details['IS_Returns'] = is_data.get('returns', 0)
            details['IS_Drawdown'] = is_data.get('drawdown', 0)
            details['IS_Margin'] = is_data.get('margin', 0)
            details['IS_Pnl'] = is_data.get('pnl', 0)
           
            # 4. Investability Constrained数据
            inv_data = is_data.get('investabilityConstrained', {})
            details['INV_Sharpe'] = inv_data.get('sharpe', 0)
            details['INV_Turnover'] = inv_data.get('turnover', 0)
            details['INV_Fitness'] = inv_data.get('fitness', 0)
            details['INV_Returns'] = inv_data.get('returns', 0)
            details['INV_Drawdown'] = inv_data.get('drawdown', 0)
            details['INV_Margin'] = inv_data.get('margin', 0)
            details['INV_Pnl'] = inv_data.get('pnl', 0)
           
            # 5. 其他重要数据
            checks = is_data.get('checks', [])
            check_values = {}
            for check in checks:
                name = check.get('name', '')
                value = check.get('value', None)
                if value is not None:
                    check_values[name] = value
           
            details['Low_Robust_Sharpe'] = check_values.get('LOW_ROBUST_UNIVERSE_SHARPE', 0)
            details['Concentrated_Weight'] = 'PASS' if 'CONCENTRATED_WEIGHT' in check_values else 'N/A'
            details['Sub_Universe_Sharpe'] = check_values.get('LOW_SUB_UNIVERSE_SHARPE', 0)
            details['Two_Year_Sharpe'] = check_values.get('LOW_2Y_SHARPE', 0)
           
            # 6. 标签
            details['Tags'] = ', '.join(alpha_data.get('tags', []))
           
        except Exception as e:
            print(f"❌ 解析基础数据时出错: {str(e)}")
            return None
       
        return details
   
    def display_results(self, details):
        """显示结果"""
        if not details:
            print("❌ 无数据可显示")
            return
       
        print("\n" + "═" * 60)
        print(f"📊 ALPHA详情报告: {details['Alpha_ID']}")
        print("═" * 60)
       
        # 基本信息
        print(f"🆔 ID: {details['Alpha_ID']}")
        print(f"🎨 颜色: {details['Color']} | 📝 状态: {details['Status']}")
        print(f"📍 地区: {details['Region']} | 🌐 股票池: {details['Universe']}")
        print(f"📅 期间: {details['Start_Date']} 至 {details['End_Date']}")
        print("─" * 40)
       
        # IS Summary数据表格
        print("📈 IS Summary Aggregate Data")
        print("─" * 25)
       
        is_metrics = [
            ('Sharpe', details.get('IS_Sharpe', 0), 'float'),
            ('Turnover', details.get('IS_Turnover', 0), 'percent'),
            ('Fitness', details.get('IS_Fitness', 0), 'float'),
            ('Returns', details.get('IS_Returns', 0), 'percent'),
            ('Drawdown', details.get('IS_Drawdown', 0), 'percent'),
            ('Margin', details.get('IS_Margin', 0), 'basis')
        ]
       
        for name, value, fmt in is_metrics:
            formatted = self._format_value(value, fmt)
            print(f"{name:12} {formatted:>15}")
       
        print("─" * 40)
       
        # Investability Constrained数据表格
        print("📉 Investability Constrained Aggregate Data")
        print("─" * 25)
       
        inv_metrics = [
            ('Sharpe', details.get('INV_Sharpe', 0), 'float'),
            ('Turnover', details.get('INV_Turnover', 0), 'percent'),
            ('Fitness', details.get('INV_Fitness', 0), 'float'),
            ('Returns', details.get('INV_Returns', 0), 'percent'),
            ('Drawdown', details.get('INV_Drawdown', 0), 'percent'),
            ('Margin', details.get('INV_Margin', 0), 'basis')
        ]
       
        for name, value, fmt in inv_metrics:
            formatted = self._format_value(value, fmt)
            print(f"{name:12} {formatted:>15}")
       
        print("─" * 40)
       
        # 相关性数据
        print("🔗 相关性数据")
        print("─" * 25)
       
        corr_metrics = [
            ('Self Correlation', details.get('Self_Correlation', 'N/A')),
            ('Power Pool Correlation', details.get('Power_Pool_Correlation', 'N/A')),
            ('Prod Correlation', details.get('Prod_Correlation', 'N/A'))
        ]
       
        for name, value in corr_metrics:
            formatted = self._format_correlation(value)
            print(f"{name:25} {formatted:>10}")
       
        print("─" * 40)
       
        # 其他重要指标
        print("📊 其他重要指标")
        print("─" * 25)
       
        other_metrics = [
            ('Low Robust Sharpe', details.get('Low_Robust_Sharpe', 0), 'float'),
            ('Sub Universe Sharpe', details.get('Sub_Universe_Sharpe', 0), 'float'),
            ('Two Year Sharpe', details.get('Two_Year_Sharpe', 0), 'float'),
            ('Concentrated Weight', details.get('Concentrated_Weight', 'N/A'), 'str'),
            ('Decay', details.get('Decay', 'N/A'), 'str'),
            ('Neutralization', details.get('Neutralization', 'N/A'), 'str'),
            ('Truncation', details.get('Truncation', 'N/A'), 'percent')
        ]
       
        for name, value, fmt in other_metrics:
            formatted = self._format_value(value, fmt)
            print(f"{name:25} {formatted:>10}")
       
        print("═" * 60)
   
    def _format_correlation(self, value):
        """格式化相关性值"""
        if value in ['N/A', None, '', 'PENDING']:
            return 'N/A'
       
        try:
            if isinstance(value, str):
                value = float(value)
           
            # 转换为百分比显示
            return f"{value * 100:.2f}%"
        except:
            return str(value)
   
    def _format_value(self, value, fmt_type):
        """格式化数值"""
        if value in ['N/A', None, '', 'PENDING', 'PASS']:
            return str(value)
       
        try:
            if fmt_type == 'percent':
                num_value = float(value)
                return f"{num_value * 100:.2f}%"
            elif fmt_type == 'basis':
                num_value = float(value)
                return f"{num_value * 10000:.2f}‱"
            elif fmt_type == 'float':
                num_value = float(value)
                return f"{num_value:.2f}"
            elif fmt_type == 'str':
                return str(value)
            else:
                return str(value)
        except:
            return str(value)
   
    def save_to_csv(self, details, filename=None):
        """保存到CSV文件"""
        if not details:
            return None
       
        os.makedirs("alpha_details", exist_ok=True)
       
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"alpha_{details['Alpha_ID']}_{timestamp}.csv"
       
        filepath = os.path.join("alpha_details", filename)
       
        try:
            df = pd.DataFrame([details])
           
            column_order = [
                'Alpha_ID', 'Status', 'Color', 'Date_Created', 'Tags',
                'Region', 'Universe', 'Delay', 'Decay', 'Neutralization', 'Truncation',
                'Start_Date', 'End_Date',
                'IS_Sharpe', 'IS_Turnover', 'IS_Fitness', 'IS_Returns',
                'IS_Drawdown', 'IS_Margin', 'IS_Pnl',
                'INV_Sharpe', 'INV_Turnover', 'INV_Fitness', 'INV_Returns',
                'INV_Drawdown', 'INV_Margin', 'INV_Pnl',
                'Self_Correlation', 'Power_Pool_Correlation', 'Prod_Correlation',
                'Low_Robust_Sharpe', 'Sub_Universe_Sharpe', 'Two_Year_Sharpe',
                'Concentrated_Weight', 'Code'
            ]
           
            existing_cols = [col for col in column_order if col in df.columns]
            df = df[existing_cols]
           
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            print(f"💾 数据已保存: {filepath}")
           
            return filepath
           
        except Exception as e:
            print(f"❌ 保存CSV失败: {str(e)}")
            return None


def fetch_alpha_details(alpha_id):
    """获取Alpha详情（主函数）"""
    print("=" * 60)
    print("🧠 Brain平台Alpha详情查询工具")
    print("=" * 60)
   
    print(f"🎯 目标Alpha: {alpha_id}")
    print("=" * 60)
   
    # 创建获取器
    fetcher = AlphaDetailFetcher()
   
    # 获取数据
    details = fetcher.get_alpha_details(alpha_id)
   
    if details:
        # 显示结果
        fetcher.display_results(details)
       
        # 保存数据
        csv_file = fetcher.save_to_csv(details)
       
        if csv_file:
            print(f"\n📁 完整数据已保存到CSV文件: {csv_file}")
   
    print("=" * 60)
    return details


def main():
    """主函数"""
    # ============================================
    # ⭐⭐⭐ 在这里设置Alpha ID ⭐⭐⭐
    # ============================================
    ALPHA_ID = "alpha_id"  # 替换为你的Alpha ID
    # ============================================
   
    # 单Alpha模式
    details = fetch_alpha_details(ALPHA_ID)
   
    return details


if __name__ == "__main__":
    # 直接运行
    main()
