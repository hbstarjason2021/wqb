#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import json
import time
from datetime import datetime, timedelta
import pytz
from typing import Dict, List, Tuple
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from time import sleep
import sys
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib import rcParams
import io
import base64
import glob

# 设置中文字体
rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
rcParams['axes.unicode_minus'] = False

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('alpha_hourly_stats.log'),
        logging.StreamHandler()
    ]
)

def load_config(file_path):
    """从指定路径f加载JSON配置文件，并处理可能的异常"""
    try:
        with open(file_path, 'r') as file:
            config = json.load(file)
        return config
    except FileNotFoundError:
        print(f"Error: Config file '{file_path}' not found.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Failed to parse JSON from '{file_path}': {e}")
        sys.exit(1)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
print(BASE_DIR)
config_file = os.path.join(BASE_DIR, 'config.json')
print(config_file)
# config_file = 'config.json'
config = load_config(config_file)
user=config["user"]
passwd=config["password"]
def login():
    username = user
    password =  passwd
    retry_strategy = Retry(
        total=3,  # 总共重试次数
        backoff_factor=1,  # 每次重试之间的延迟时间（秒）
        status_forcelist=[500, 502, 503, 504]  # 遇到这些HTTP状态码时重试
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.auth = (username, password)
    max_try = 3
    retry=0
    while True:
        try:
            response = s.post('https://api.worldquantbrain.com/authentication')
            if response.status_code  in [200,201]:
                print(f"login success")
                return s
        except Exception as e:
            print(f"login err :{e}")
            print(f"login failed ,sleep 5 ,try again")
            sleep(5)
        retry +=1
        if retry > max_try:
            break
    return None
s = login()

def get_alpha_count_by_time_range(start_time: str, end_time: str) -> int:
    """
    获取指定时间范围内的alpha表达式数量

    Args:
        start_time: 开始时间 (格式: 2025-07-11T00:00:00-04:00)
        end_time: 结束时间 (格式: 2025-07-11T01:00:00-04:00)

    Returns:
        int: alpha表达式数量
    """
    baseurl="https://api.worldquantbrain.com/users/self/alphas?limit=100&offset=0&status=UNSUBMITTED%1FIS_FAIL"
    url=f"{baseurl}&dateCreated%3E={start_time}&dateCreated%3C={end_time}&order=-dateCreated"
    print(url)
    try:
        # params = {
        #     'limit': 1,  # 只需要获取数量，不需要实际数据
        #     'offset': 0,
        #     'status': 'UNSUBMITTED,IS_FAIL',
        #     'dateCreated>=': start_time,
        #     'dateCreated<': end_time,
        #     'order': '-dateCreated',
        #     'hidden': 'false'
        # }

        response = s.get(url)
        response.raise_for_status()

        data = response.json()
        total_count = data.get('count', 0)

        logging.info(f"时间范围 {start_time} 到 {end_time}: {total_count} 个表达式")
        return total_count

    except requests.exceptions.RequestException as e:
        logging.error(f"请求失败: {e}")
        return 0
    except json.JSONDecodeError as e:
        logging.error(f"JSON解析失败: {e}")
        return 0
    except Exception as e:
        logging.error(f"未知错误: {e}")
        return 0

def get_server_timezone() -> str:
    """
    获取服务器时区偏移量
    根据你提供的信息，服务器在美国东部时间 (EDT: -04:00)

    Returns:
        str: 时区偏移量，如 "-04:00"
    """
    # 可以根据实际情况调整
    return "-04:00"

def format_time_for_api( dt: datetime, timezone_offset: str) -> str:
    """
    格式化时间为API所需的格式

    Args:
        dt: datetime对象
        timezone_offset: 时区偏移量，如 "-04:00"

    Returns:
        str: 格式化后的时间字符串
    """
    return dt.strftime(f"%Y-%m-%dT%H:%M:%S{timezone_offset}")

def get_hourly_stats_last_24h() -> List[Dict]:
    """
    获取最近24小时（以服务器当前时间为基准）每个小时的alpha表达式数量统计
    Returns:
        List[Dict]: 每小时统计结果列表
    """
    # 获取服务器时区
    timezone_offset = get_server_timezone()

    # 直接获取美国东部时间的当前时刻
    server_tz = pytz.timezone('America/New_York')
    server_now = datetime.now(server_tz)

    # 计算24小时前的时间
    start_time = server_now - timedelta(hours=24)

    hourly_stats = []

    # 遍历每个小时
    for i in range(24):
        hour_start = start_time + timedelta(hours=i)
        hour_end = hour_start + timedelta(hours=1)

        # 格式化为API需要的字符串（带时区）
        def format_with_colon(dt):
            s = dt.strftime("%Y-%m-%dT%H:%M:%S%z")
            return s[:-2] + ":" + s[-2:]
        start_str = format_with_colon(hour_start)
        end_str = format_with_colon(hour_end)

        # 获取该小时的表达式数量
        count = get_alpha_count_by_time_range(start_str, end_str)

        # 记录统计结果
        hour_stat = {
            'hour': hour_start.strftime('%Y-%m-%d %H:00'),
            'start_time': start_str,
            'end_time': end_str,
            'count': count
        }
        hourly_stats.append(hour_stat)

        # 添加延迟避免请求过于频繁
        time.sleep(0.5)

    return hourly_stats

def save_stats_to_file( stats: List[Dict], filename: str = None):
    """
    保存统计结果到文件

    Args:
        stats: 统计结果列表
        filename: 文件名，如果为None则使用时间戳
    """
    if filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'alpha_hourly_stats_{timestamp}.json'

    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        logging.info(f"统计结果已保存到: {filename}")
    except Exception as e:
        logging.error(f"保存文件失败: {e}")

def print_summary(stats: List[Dict]):
    """
    打印统计摘要

    Args:
        stats: 统计结果列表
    """
    total_count = sum(stat['count'] for stat in stats)
    max_hour = max(stats, key=lambda x: x['count'])
    min_hour = min(stats, key=lambda x: x['count'])

    print("\n" + "="*60)
    print("Alpha表达式24小时统计摘要")
    print("="*60)
    print(f"总表达式数量: {total_count}")
    print(f"最高产小时: {max_hour['hour']} ({max_hour['count']} 个)")
    print(f"最低产小时: {min_hour['hour']} ({min_hour['count']} 个)")
    print(f"平均每小时: {total_count/24:.1f} 个")
    print("="*60)

    # 打印每小时详细统计
    print("\n每小时详细统计:")
    print("-" * 50)
    for stat in stats:
        print(f"{stat['hour']}: {stat['count']:3d} 个")
    print("-" * 50)

def send_feishu_notification(stats: List[Dict], webhook_url: str = None):
    """
    发送飞书机器人通知

    Args:
        stats: 统计结果列表
        webhook_url: 飞书机器人webhook地址
    """
    if not webhook_url:
        # 如果没有提供webhook_url，尝试从配置文件读取
        try:
            webhook_url = config.get("feishu_webhook_url")
            if not webhook_url:
                logging.warning("未配置飞书webhook地址，跳过通知发送")
                return
        except:
            logging.warning("未配置飞书webhook地址，跳过通知发送")
            return

    try:
        # 计算统计摘要
        total_count = sum(stat['count'] for stat in stats)
        max_hour = max(stats, key=lambda x: x['count'])
        min_hour = min(stats, key=lambda x: x['count'])
        avg_count = total_count / 24

        # 构建表格内容
        table_rows = []
        for stat in stats:
            table_rows.append([
                stat['hour'],
                str(stat['count'])
            ])

        # 构建飞书消息
        message = {
            "msg_type": "post",
            "content": {
                "post": {
                    "zh_cn": {
                        "title": f"Alpha表达式24小时统计报告 - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                        "content": [
                            [
                                {
                                    "tag": "text",
                                    "text": f"📊 Alpha表达式24小时统计摘要\n"
                                }
                            ],
                            [
                                {
                                    "tag": "text",
                                    "text": f"总表达式数量: {total_count} 个\n"
                                }
                            ],
                            [
                                {
                                    "tag": "text",
                                    "text": f"最高产小时: {max_hour['hour']} ({max_hour['count']} 个)\n"
                                }
                            ],
                            [
                                {
                                    "tag": "text",
                                    "text": f"最低产小时: {min_hour['hour']} ({min_hour['count']} 个)\n"
                                }
                            ],
                            [
                                {
                                    "tag": "text",
                                    "text": f"平均每小时: {avg_count:.1f} 个\n\n"
                                }
                            ],
                            [
                                {
                                    "tag": "text",
                                    "text": "📈 每小时详细统计:\n"
                                }
                            ]
                        ]
                    }
                }
            }
        }

        # 添加完整的24小时数据（使用紧凑格式）
        for i, row in enumerate(table_rows):
            # 每行显示2个时间段，节省空间
            if i % 2 == 0:
                if i + 1 < len(table_rows):
                    # 显示两个时间段
                    next_row = table_rows[i + 1]
                    message["content"]["post"]["zh_cn"]["content"].append([
                        {
                            "tag": "text",
                            "text": f"{row[0]}: {row[1]:>4} | {next_row[0]}: {next_row[1]:>4}\n"
                        }
                    ])
                else:
                    # 最后一个时间段
                    message["content"]["post"]["zh_cn"]["content"].append([
                        {
                            "tag": "text",
                            "text": f"{row[0]}: {row[1]:>4}\n"
                        }
                    ])

        # 发送消息
        response = requests.post(webhook_url, json=message, timeout=10)
        response.raise_for_status()

        # 如果消息太长，发送第二条消息包含剩余数据
        if len(table_rows) > 12:
            # 构建第二条消息（包含剩余12小时数据）
            message2 = {
                "msg_type": "post",
                "content": {
                    "post": {
                        "zh_cn": {
                            "title": f"Alpha表达式统计报告（续）- {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                            "content": [
                                [
                                    {
                                        "tag": "text",
                                        "text": "📈 每小时详细统计（续）:\n"
                                    }
                                ]
                            ]
                        }
                    }
                }
            }

            # 添加剩余12小时数据
            for i in range(12, len(table_rows)):
                row = table_rows[i]
                message2["content"]["post"]["zh_cn"]["content"].append([
                    {
                        "tag": "text",
                        "text": f"{row[0]}: {row[1]:>4} 个\n"
                    }
                ])

            # 发送第二条消息
            response2 = requests.post(webhook_url, json=message2, timeout=10)
            response2.raise_for_status()
            logging.info("飞书通知（续）发送成功")

        logging.info("飞书通知发送成功")

    except Exception as e:
        logging.error(f"发送飞书通知失败: {e}")

def generate_hourly_chart(stats: List[Dict]) -> str:
    """
    生成每小时统计数据的曲线图

    Args:
        stats: 统计结果列表

    Returns:
        str: 图片文件路径
    """
    try:
        # 准备数据
        hours = []
        counts = []

        for stat in stats:
            # 提取小时信息，格式化为更简洁的显示
            hour_str = stat['hour']
            # 只显示小时，格式为 HH:00
            display_hour = hour_str.split(' ')[1]  # 只取小时部分

            hours.append(display_hour)
            counts.append(stat['count'])

        # 创建图表
        plt.figure(figsize=(16, 10))

        # 绘制曲线图
        plt.plot(range(len(hours)), counts, marker='o', linewidth=3, markersize=8, color='#1890ff', alpha=0.8)

        # 填充曲线下方区域
        plt.fill_between(range(len(hours)), counts, alpha=0.3, color='#1890ff')

        # 设置标题和标签
        plt.title('Alpha 24-Hour Expression Statistics ', fontsize=18, fontweight='bold', pad=20, color='#333')
        plt.xlabel("Time (Hour)", fontsize=14, color='#333')
        plt.ylabel('simulate count', fontsize=14, color='#333')

        # 设置x轴刻度
        plt.xticks(range(len(hours)), hours, rotation=0, fontsize=10)
        plt.yticks(fontsize=10)

        # 添加网格
        plt.grid(True, alpha=0.2, linestyle='--')

        # 添加所有数据点的数字标签
        for i, count in enumerate(counts):
            plt.annotate(str(count), (i, count),
                        textcoords="offset points", xytext=(0,10),
                        ha='center', fontsize=9, fontweight='bold',
                        bbox=dict(boxstyle='round,pad=0.2', facecolor='white', alpha=0.8, edgecolor='#1890ff'))

        # 计算统计信息
        total_count = sum(counts)
        max_count = max(counts)
        min_count = min(counts)
        avg_count = total_count / len(counts)

        # 添加统计信息文本框
        stats_text = f'total : {total_count:,}\nMax: {max_count:,}\nMin: {min_count:,}\navg: {avg_count:.0f}'
        plt.text(0.02, 0.98, stats_text, transform=plt.gca().transAxes,
                verticalalignment='top', fontsize=12, fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.5', facecolor='lightblue', alpha=0.8))

        # 设置背景色
        plt.gca().set_facecolor('#f8f9fa')
        plt.gcf().set_facecolor('white')

        # 调整布局
        plt.tight_layout()

        # 保存图片到文件
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        image_path = f"alpha_hourly_chart_{timestamp}.png"
        plt.savefig(image_path, format='png', dpi=150, bbox_inches='tight',
                   facecolor='white', edgecolor='none')

        # 关闭图表释放内存
        plt.close()

        logging.info(f"图表已保存到: {image_path}")
        return image_path

    except Exception as e:
        logging.error(f"生成图表失败: {e}")
        return None

def upload_image_to_feishu_simple(image_path: str, webhook_url: str) -> str:
    """
    使用飞书机器人API上传图片（简化版）

    Args:
        image_path: 图片文件路径
        webhook_url: 飞书机器人webhook地址

    Returns:
        str: image_key，如果失败返回None
    """
    try:
        # 飞书机器人图片上传API
        upload_url = webhook_url.replace("/hook/", "/upload/")

        with open(image_path, 'rb') as f:
            files = {'file': f}
            response = requests.post(upload_url, files=files, timeout=30)

            if response.status_code == 200:
                data = response.json()
                image_key = data.get('image_key')
                if image_key:
                    logging.info("图片上传到飞书成功（简化版）")
                    return image_key
                else:
                    logging.error(f"图片上传响应中未找到image_key: {data}")
            else:
                logging.error(f"图片上传失败，状态码: {response.status_code}, 响应: {response.text}")

        return None

    except Exception as e:
        logging.error(f"上传图片到飞书失败（简化版）: {e}")
        return None

def upload_image_to_feishu(image_path: str, app_token: str = None) -> str:
    """
    上传图片到飞书并获取image_key

    Args:
        image_path: 图片文件路径
        app_token: 飞书应用token

    Returns:
        str: image_key，如果失败返回None
    """
    if not app_token:
        app_token = config.get('feishu_app_token')
        if not app_token:
            logging.warning("未配置飞书应用token，无法上传图片")
            return None

    try:
        # 飞书图片上传API
        upload_url = "https://open.feishu.cn/open-apis/im/v1/files"
        headers = {
            "Authorization": f"Bearer {app_token}",
            "Content-Type": "multipart/form-data"
        }

        with open(image_path, 'rb') as f:
            files = {'file': (os.path.basename(image_path), f, 'image/png')}
            response = requests.post(upload_url, headers=headers, files=files, timeout=30)

            if response.status_code == 200:
                data = response.json()
                image_key = data.get('data', {}).get('image_key')
                if image_key:
                    logging.info("图片上传到飞书成功")
                    return image_key
                else:
                    logging.error(f"图片上传响应中未找到image_key: {data}")
            else:
                logging.error(f"图片上传失败，状态码: {response.status_code}, 响应: {response.text}")

        return None

    except Exception as e:
        logging.error(f"上传图片到飞书失败: {e}")
        return None

def send_feishu_notification_with_chart(stats: List[Dict], webhook_url: str = None):
    """
    发送带图表的飞书机器人通知

    Args:
        stats: 统计结果列表
        webhook_url: 飞书机器人webhook地址
    """
    if not webhook_url:
        # 如果没有提供webhook_url，尝试从配置文件读取
        try:
            webhook_url = config.get("feishu_webhook_url")
            if not webhook_url:
                logging.warning("未配置飞书webhook地址，跳过通知发送")
                return
        except:
            logging.warning("未配置飞书webhook地址，跳过通知发送")
            return

    try:
        # 生成图表
        chart_path = generate_hourly_chart(stats)

        # 计算统计摘要
        total_count = sum(stat['count'] for stat in stats)
        max_hour = max(stats, key=lambda x: x['count'])
        min_hour = min(stats, key=lambda x: x['count'])
        avg_count = total_count / 24

        # 构建表格内容
        table_rows = []
        for stat in stats:
            table_rows.append([
                stat['hour'],
                str(stat['count'])
            ])

        # 构建飞书消息
        message = {
            "msg_type": "post",
            "content": {
                "post": {
                    "zh_cn": {
                        "title": f"Alpha表达式24小时统计报告 - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                        "content": [
                            [
                                {
                                    "tag": "text",
                                    "text": f"📊 Alpha表达式24小时统计摘要\n"
                                }
                            ],
                            [
                                {
                                    "tag": "text",
                                    "text": f"总表达式数量: {total_count:,} 个\n"
                                }
                            ],
                            [
                                {
                                    "tag": "text",
                                    "text": f"最高产小时: {max_hour['hour']} ({max_hour['count']:,} 个)\n"
                                }
                            ],
                            [
                                {
                                    "tag": "text",
                                    "text": f"最低产小时: {min_hour['hour']} ({min_hour['count']:,} 个)\n"
                                }
                            ],
                            [
                                {
                                    "tag": "text",
                                    "text": f"平均每小时: {avg_count:.1f} 个\n\n"
                                }
                            ],
                            [
                                {
                                    "tag": "text",
                                    "text": "📈 每小时详细统计:\n"
                                }
                            ]
                        ]
                    }
                }
            }
        }

        # 添加完整的24小时数据（使用紧凑格式）
        for i, row in enumerate(table_rows):
            # 每行显示2个时间段，节省空间
            if i % 2 == 0:
                if i + 1 < len(table_rows):
                    # 显示两个时间段
                    next_row = table_rows[i + 1]
                    message["content"]["post"]["zh_cn"]["content"].append([
                        {
                            "tag": "text",
                            "text": f"{row[0]}: {row[1]:>4} | {next_row[0]}: {next_row[1]:>4}\n"
                        }
                    ])
                else:
                    # 最后一个时间段
                    message["content"]["post"]["zh_cn"]["content"].append([
                        {
                            "tag": "text",
                            "text": f"{row[0]}: {row[1]:>4}\n"
                        }
                    ])

        # 发送文本消息
        response = requests.post(webhook_url, json=message, timeout=10)
        response.raise_for_status()

        # 如果生成了图表，发送图表文件信息
        if chart_path:
            try:
                # 读取图片文件并转换为base64
                with open(chart_path, 'rb') as f:
                    image_data = f.read()
                    image_base64 = base64.b64encode(image_data).decode('utf-8')

                # 发送包含图片的富文本消息
                image_message = {
                    "msg_type": "post",
                    "content": {
                        "post": {
                            "zh_cn": {
                                "title": f"Alpha表达式统计曲线图 - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                                "content": [
                                    [
                                        {
                                            "tag": "text",
                                            "text": "📊 24小时统计曲线图\n"
                                        }
                                    ],
                                    [
                                        {
                                            "tag": "img",
                                            "image_key": image_base64
                                        }
                                    ],
                                    [
                                        {
                                            "tag": "text",
                                            "text": f"\n图表文件: {chart_path}\n"
                                        }
                                    ]
                                ]
                            }
                        }
                    }
                }

                # 发送图片消息
                img_response = requests.post(webhook_url, json=image_message, timeout=10)
                img_response.raise_for_status()
                logging.info("图表图片发送成功")

            except Exception as e:
                logging.error(f"发送图表失败: {e}")
                # 如果图片发送失败，发送文件路径信息
                file_info_msg = {
                    "msg_type": "post",
                    "content": {
                        "post": {
                            "zh_cn": {
                                "title": f"图表文件信息 - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                                "content": [
                                    [
                                        {
                                            "tag": "text",
                                            "text": f"📊 曲线图已生成，文件路径: {chart_path}\n"
                                        }
                                    ],
                                    [
                                        {
                                            "tag": "text",
                                            "text": "如需查看图表，请访问服务器获取图片文件。\n"
                                        }
                                    ]
                                ]
                            }
                        }
                    }
                }
                file_response = requests.post(webhook_url, json=file_info_msg, timeout=10)
                file_response.raise_for_status()
                logging.info("图表文件信息发送成功")

        # 如果消息太长，发送第二条消息包含剩余数据
        if len(table_rows) > 12:
            # 构建第二条消息（包含剩余12小时数据）
            message2 = {
                "msg_type": "post",
                "content": {
                    "post": {
                        "zh_cn": {
                            "title": f"Alpha表达式统计报告（续）- {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                            "content": [
                                [
                                    {
                                        "tag": "text",
                                        "text": "📈 每小时详细统计（续）:\n"
                                    }
                                ]
                            ]
                        }
                    }
                }
            }

            # 添加剩余12小时数据
            for i in range(12, len(table_rows)):
                row = table_rows[i]
                message2["content"]["post"]["zh_cn"]["content"].append([
                    {
                        "tag": "text",
                        "text": f"{row[0]}: {row[1]:>4} 个\n"
                    }
                ])

            # 发送第二条消息
            response2 = requests.post(webhook_url, json=message2, timeout=10)
            response2.raise_for_status()
            logging.info("飞书通知（续）发送成功")

        logging.info("飞书通知发送成功")

    except Exception as e:
        logging.error(f"发送飞书通知失败: {e}")

def format_time_for_display(time_str: str) -> str:
    """
    格式化时间字符串用于显示

    Args:
        time_str: 原始时间字符串 (格式: 2025-07-11T00:00:00-04:00)

    Returns:
        str: 格式化后的时间字符串
    """
    # 移除T并替换时区标识
    formatted = time_str.replace('T', ' ')
    formatted = formatted.replace('-04:00', ' EDT')
    formatted = formatted.replace('-05:00', ' EST')
    return formatted

def send_email_notification(stats: List[Dict], email_config: Dict = None):
    """
    发送邮件通知（包含图表附件）

    Args:
        stats: 统计结果列表
        email_config: 邮件配置
    """
    if not email_config:
        try:
            email_config = config.get("email_config")
            if not email_config:
                logging.warning("未配置邮件设置，跳过邮件通知")
                return
        except:
            logging.warning("未配置邮件设置，跳过邮件通知")
            return

    try:
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        from email.mime.image import MIMEImage
        import traceback

        # 生成图表
        chart_path = generate_hourly_chart(stats)

        # 计算统计摘要
        total_count = sum(stat['count'] for stat in stats)
        max_hour = max(stats, key=lambda x: x['count'])
        min_hour = min(stats, key=lambda x: x['count'])
        avg_count = total_count / 24

        # 获取查询的总体时间范围
        overall_start = format_time_for_display(stats[0]['start_time'])
        overall_end = format_time_for_display(stats[-1]['end_time'])

        # 构建HTML表格
        html_content = f"""
        <html>
        <head>
            <style>
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
                .summary {{ background-color: #e7f3ff; padding: 10px; margin: 10px 0; }}
                .chart-info {{ background-color: #f9f9f9; padding: 10px; margin: 10px 0; border-left: 4px solid #1890ff; }}
            </style>
        </head>
        <body>
            <h2>Alpha表达式24小时统计报告</h2>
            <p>生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

            <div class="summary">
                <h3>📊 统计摘要</h3>
                <p><strong>查询时间范围:</strong> {overall_start} 至 {overall_end}</p>
                <p><strong>总表达式数量:</strong> {total_count:,} 个</p>
                <p><strong>最高产小时:</strong> {max_hour['hour']} ({max_hour['count']:,} 个)</p>
                <p><strong>最低产小时:</strong> {min_hour['hour']} ({min_hour['count']:,} 个)</p>
                <p><strong>平均每小时:</strong> {avg_count:.1f} 个</p>
            </div>

            <div class="chart-info">
                <h3>📈 统计曲线图</h3>
                <p>本邮件已附上24小时统计曲线图，每个数据点都标注了具体的表达式数量。</p>
                <p>图表文件: {chart_path if chart_path else '生成失败'}</p>
            </div>

            <h3>📋 每小时详细统计</h3>
            <table>
                <tr>
                    <th>查询时间范围</th>
                    <th>表达式数量</th>
                </tr>
        """

        for stat in stats:
            # 格式化查询时间范围，显示开始和结束时间
            start_time_display = format_time_for_display(stat['start_time'])
            end_time_display = format_time_for_display(stat['end_time'])
            time_range = f"{start_time_display} - {end_time_display}"

            html_content += f"""
                <tr>
                    <td>{time_range}</td>
                    <td>{stat['count']:,}</td>
                </tr>
            """

        html_content += """
            </table>

            <hr>
            <p style="color: #666; font-size: 12px;">
                此邮件由Alpha表达式统计系统自动生成，如有问题请联系管理员。
            </p>
        </body>
        </html>
        """

        # 创建邮件
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"Alpha表达式24小时统计报告 - {datetime.now().strftime('%Y-%m-%d')}"
        msg['From'] = email_config['sender_email']
        msg['To'] = email_config['recipient_email']

        # 添加HTML内容
        msg.attach(MIMEText(html_content, 'html'))

        # 如果生成了图表，添加为附件
        if chart_path and os.path.exists(chart_path):
            try:
                with open(chart_path, 'rb') as f:
                    img_data = f.read()

                # 创建图片附件
                image_attachment = MIMEImage(img_data)
                image_attachment.add_header('Content-Disposition', 'attachment',
                                          filename=f"alpha_hourly_chart_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                msg.attach(image_attachment)

                logging.info(f"图表附件已添加: {chart_path}")

            except Exception as e:
                logging.error(f"添加图表附件失败: {e}")

        # 打印邮件配置信息（隐藏密码）
        logging.info(f"邮件配置: SMTP服务器={email_config['smtp_server']}, 端口={email_config['smtp_port']}, 发件人={email_config['sender_email']}")

        # 根据端口选择连接方式
        if email_config['smtp_port'] == 465:
            # SSL连接
            logging.info("使用SSL连接发送邮件...")
            server = smtplib.SMTP_SSL(email_config['smtp_server'], email_config['smtp_port'])
            server.login(email_config['sender_email'], email_config['password'])
        else:
            # STARTTLS连接
            logging.info("使用STARTTLS连接发送邮件...")
            server = smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port'])
            server.starttls()
            server.login(email_config['sender_email'], email_config['password'])

        server.send_message(msg)
        server.quit()

        logging.info("邮件通知发送成功（包含图表附件）")

    except Exception as e:
        logging.error(f"发送邮件通知失败: {e}")
        logging.error(f"详细错误信息: {traceback.format_exc()}")

def clean_old_files(hours=3):
    now = time.time()
    cutoff = now - hours * 3600
    patterns = ['alpha_hourly_chart_*.png', 'alpha_hourly_stats_*.json']
    deleted = []
    for pattern in patterns:
        for file in glob.glob(pattern):
            try:
                mtime = os.path.getmtime(file)
                if mtime < cutoff:
                    os.remove(file)
                    deleted.append(file)
            except Exception as e:
                print(f"Failed to delete {file}: {e}")
    if deleted:
        print(f"Deleted old files: {deleted}")
    else:
        print("No old files to delete.")

def main():
    clean_old_files(hours=3)
    """
    主函数
    """
    # 这里需要先获取已认证的session
    # 假设你已经有了登录函数

    # 创建统计对象
    # stats_collector = AlphaHourlyStats(session)

    try:
        # 获取24小时统计
        logging.info("开始获取前24小时alpha表达式统计...")
        hourly_stats = get_hourly_stats_last_24h()

        # 保存结果
        save_stats_to_file(hourly_stats)

        # 打印摘要
        print_summary(hourly_stats)

        # 发送通知
        # send_feishu_notification_with_chart(hourly_stats)  # 注释掉飞书通知
        send_email_notification(hourly_stats)

        logging.info("统计完成")

    except Exception as e:
        logging.error(f"执行失败: {e}")

if __name__ == "__main__":
    main()
