# 导入必要的依赖模块
import pandas as pd
import requests
import time
from datetime import datetime  # 新增：用于生成时间戳文件名

def login():
    """
    实际的 WQB 平台登录函数，返回带认证的 Session 对象
    注意：请填写你的 username 和 password
    """
    # 请在这里填写你的 WQB 平台账号和密码
    username = ""
    password = ""
 
    # 创建会话对象（持久化存储认证信息）
    s = requests.Session()
 
    # 将凭证存入会话
    s.auth = (username, password)
 
    # 发送认证请求
    response = s.post('https://api.worldquantbrain.com/authentication')
    print("登录响应内容：", response.content)  # 打印响应，方便调试登录是否成功
    return s  

def view_alphas(gold_bag, save_csv_path=None):
    """
    批量查询 Alpha 信息，计算得分并格式化输出，支持保存为 CSV
    :param gold_bag: 列表，元素可为字符串（Alpha ID）或元组/列表 (Alpha ID, 自相关系数)
    :param save_csv_path: 可选，CSV 文件保存路径（如 "alpha_result.csv"），None 则不保存
    :return: 格式化后的 DataFrame
    """
    s = login()
    data = []

    # 统一处理输入格式：转为 (alpha_id, 自相关系数) 的元组列表
    processed_bag = []
    for item in gold_bag:
        if isinstance(item, (tuple, list)):
            # 确保至少有 Alpha ID，自相关系数默认 0
            alpha_id = item[0] if len(item) > 0 else ""
            pc = item[1] if len(item) > 1 else 0
            processed_bag.append((alpha_id, pc))
        else:
            processed_bag.append((item, 0))

    # 遍历查询每个 Alpha
    for alpha_id, pc in processed_bag:
        if not alpha_id:  # 过滤空的 Alpha ID
            continue
        info = locate_alpha(s, alpha_id)
        if info:
            info['self_corr'] = pc
            info['fail_count'] = 0  # 默认为 0
            info['score'] = calculate_score(info)
            data.append(info)

    # 无数据时的提示
    if not data:
        print("No alphas found.")
        return pd.DataFrame()  # 返回空 DataFrame 而非 None，保证返回类型统一

    # 转为 DataFrame 并调整列顺序
    df = pd.DataFrame(data)
    # 核心列优先展示
    cols = ['id', 'score', 'sharpe', 'fitness', 'margin', 'turnover', 'returns', 'drawdown',
            'long_count', 'short_count', 'fail_count', 'sub_universe_sharpe', 'sharpe_2y', 'universe']
    # 筛选存在的列
    available_cols = [c for c in cols if c in df.columns]
    # 剩余列（排除 code 避免冗余）
    remaining_cols = [c for c in df.columns if c not in available_cols and c != 'code']
    final_cols = available_cols + remaining_cols
    df = df[final_cols]

    # 按得分降序排序
    df.sort_values('score', ascending=False, inplace=True)
    # 重置索引（方便查看）
    df.reset_index(drop=True, inplace=True)

    # 配置 pandas 显示格式
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.float_format', '{:.4f}'.format)

    # 打印结果
    print(df.to_string())

    # 新增：保存为 CSV 文件
    if save_csv_path:
        try:
            # 使用 utf-8-sig 编码，避免中文乱码（兼容 Excel 打开）
            df.to_csv(save_csv_path, index=False, encoding='utf-8-sig')
            print(f"\n结果已成功保存到 CSV 文件：{save_csv_path}")
        except Exception as e:
            print(f"\n保存 CSV 失败：{str(e)}")

    return df

def locate_alpha(s, alpha_id):
    """
    单次查询单个 Alpha 的详细信息（带重试机制）
    :param s: 登录后的 requests.Session 对象
    :param alpha_id: Alpha 唯一标识
    :return: Alpha 信息字典 / None
    """
    # 最多重试 5 次
    for retry in range(5):
        try:
            url = f"https://api.worldquantbrain.com/alphas/{alpha_id}"
            r = s.get(url, timeout=10)  # 增加超时限制，避免卡死

            # 处理不同响应状态码
            if r.status_code == 200:
                data = r.json()
                reg = data.get('regular', {})
                iss = data.get('is', {})
                sett = data.get('settings', {})
                checks = iss.get('checks', [])

                # 提取核心信息（所有值做空值兜底）
                info = {
                    'id': alpha_id,
                    'code': reg.get('code', ''),
                    'sharpe': iss.get('sharpe', 0.0),
                    'fitness': iss.get('fitness', 0.0),
                    'turnover': iss.get('turnover', 0.0),
                    'margin': iss.get('margin', 0.0),
                    'returns': iss.get('returns', 0.0),
                    'drawdown': iss.get('drawdown', 0.0),
                    'long_count': iss.get('longCount', 0),
                    'short_count': iss.get('shortCount', 0),
                    'dateCreated': data.get('dateCreated', ''),
                    'decay': sett.get('decay', 0.0),
                    'universe': sett.get('universe', 'TOP3000'),
                    'region': sett.get('region', 'USA')
                }

                # 提取检查项（子池夏普率、2年夏普率）
                for check in checks:
                    check_name = check.get('name', '')
                    if check_name == 'LOW_SUB_UNIVERSE_SHARPE':
                        info['sub_universe_sharpe'] = check.get('value', 0.0)
                    elif check_name == 'LOW_2Y_SHARPE':
                        info['sharpe_2y'] = check.get('value', 0.0)

                return info

            elif r.status_code == 429:
                # 处理限流：按响应头的 Retry-After 等待
                wait_time = float(r.headers.get('Retry-After', 5))
                print(f"请求限流，等待 {wait_time} 秒后重试（Alpha ID: {alpha_id}，重试次数: {retry+1}）")
                time.sleep(wait_time)

            else:
                # 其他错误状态码，等待 1 秒重试
                print(f"查询失败（状态码: {r.status_code}），Alpha ID: {alpha_id}，重试次数: {retry+1}")
                time.sleep(1)

        except Exception as e:
            # 捕获所有异常，避免单个 Alpha 查询失败中断整体流程
            print(f"查询异常（Alpha ID: {alpha_id}，重试次数: {retry+1}）：{str(e)}")
            time.sleep(1)

    # 5 次重试失败后返回 None
    print(f"Alpha ID: {alpha_id} 多次查询失败，跳过")
    return None

def calculate_score(row):
    """
    根据 效用-惩罚 逻辑计算 Alpha 最终得分
    :param row: Alpha 信息字典
    :return: 最终得分（保留 6 位小数），异常返回 -999
    """
    try:
        # 1. 提取指标并做类型转换 + 空值兜底（避免计算错误）
        fitness = float(row.get('fitness', 0.0))
        sharpe = float(row.get('sharpe', 0.0))
        margin = float(row.get('margin', 0.0))
        turnover = float(row.get('turnover', 0.0))
        returns = float(row.get('returns', 0.0))
        drawdown = float(row.get('drawdown', 1.0))  # 兜底为 1，避免除 0
        long_count = int(row.get('long_count', 0))
        short_count = int(row.get('short_count', 0))
        universe_name = str(row.get('universe', 'TOP3000')).upper()
        fail_count = int(row.get('fail_count', 0))

        # 2. 标的池规模映射（根据名称匹配规模）
        uni_map = {
            'TOP3000': 3000, 'TOP2500': 2500, 'TOP2000': 2000, 'TOP2000U': 2000,
            'TOP1200': 1200, 'TOP1000': 1000, 'TOP800': 800,
            'TOP500': 500, 'TOPSP500': 500, 'TOP400': 400, 'TOP200': 200, 'TOP100': 100,
            'MINVOL1M': 1000, 'ILLIQUID_MINVOL1M': 2000, 'TOPIDV3000': 3000
        }
        universe_size = 3000  # 默认规模
        for k, v in uni_map.items():
            if k in universe_name:
                universe_size = v
                break

        # 3. 异常 Alpha 直接返回 -100（标记为无效）
        # 条件1：夏普率和换手率均为 0（无有效因子）
        if sharpe == 0.0 and turnover == 0.0:
            return round(-100.0, 6)
        # 条件2：多头或空头持仓为 0（单边持仓）
        if long_count == 0 or short_count == 0:
            return round(-100.0, 6)
        # 条件3：换手率 ≥ 80%（过高换手）
        if turnover >= 0.80:
            return round(-100.0, 6)
        # 条件4：低持仓 + 高夏普/高拟合度（异常）
        holding_ratio = (long_count + short_count) / universe_size if universe_size > 0 else 0.0
        if holding_ratio < 0.30 and (sharpe > 1.58 or fitness > 1.0):
            return round(-100.0, 6)
        # 条件5：极低回撤 + 极高收益（不切实际）
        if drawdown < 0.05 and returns > 2.0:
            return round(-100.0, 6)

        # 4. 计算效用（正向得分）
        # 拟合度（35% 权重）
        u_fitness = 0.35 * fitness
        # 夏普率（30% 权重）
        u_sharpe = 0.30 * sharpe
        # 保证金（18% 权重，以 5bps 为基准归一化）
        u_margin = 0.18 * (margin / 0.0005) if margin >= 0 else 0.0
        # 换手率（10% 权重，仅理想区间得分）
        u_turnover = 0.0
        if 0.05 <= turnover <= 0.15:
            u_turnover = 0.10
        elif 0.15 < turnover <= 0.30:
            u_turnover = 0.05
        # 收益/回撤（7% 权重）
        ret_dd = returns / drawdown if drawdown > 0.0001 else 0.0
        u_ret_dd = 0.07 * ret_dd
        # 无失败奖励（额外 15%）
        bonus = 0.15 if fail_count == 0 else 0.0

        # 总效用
        utility = u_fitness + u_sharpe + u_margin + u_turnover + u_ret_dd + bonus

        # 5. 计算惩罚（反向扣分）
        penalty = 0.0

        # 换手率惩罚
        if 0.15 < turnover <= 0.30:
            penalty += (turnover - 0.15) / (0.30 - 0.15) * 0.3
        elif 0.30 < turnover <= 0.70:
            penalty += 0.3 + (turnover - 0.30) / (0.70 - 0.30) * 1.2
        elif turnover > 0.70:
            penalty += 1.5 + (turnover - 0.70) * 10.0
        elif turnover < 0.02:
            penalty += 10.0

        # 持仓比例惩罚
        if holding_ratio >= 0.50 and holding_ratio < 0.70:
            penalty += 0.3
        elif holding_ratio >= 0.30 and holding_ratio < 0.50:
            penalty += 0.8
        elif holding_ratio < 0.30:
            penalty += 1.5

        # 多空平衡惩罚
        ls_ratio = 0.0
        if max(long_count, short_count) > 0:
            ls_ratio = min(long_count, short_count) / max(long_count, short_count)
        if 0.60 <= ls_ratio < 0.80:
            penalty += 0.05
        elif 0.40 <= ls_ratio < 0.60:
            penalty += 0.15
        elif 0.20 <= ls_ratio < 0.40:
            penalty += 0.30
        elif ls_ratio < 0.20:
            penalty += 0.50

        # 最终得分 = 效用 - 惩罚
        final_score = utility - penalty
        return round(final_score, 6)

    except Exception as e:
        print(f"得分计算异常：{str(e)}")
        return round(-999.0, 6)

# ------------------- 测试示例（直接运行代码请取消注释） -------------------
if __name__ == "__main__":
    # 第一步：先在 login() 函数中填写你的 WQB 账号和密码
    # 第二步：修改测试用的 Alpha ID 为实际需要查询的 ID
    test_gold_bag = [
        "your_alpha_id_1",  # 纯 Alpha ID
        ("your_alpha_id_2", 0.123),  # Alpha ID + 自相关系数
    ]
    
    # 生成带时间戳的 CSV 文件名（避免覆盖），示例：alpha_result_20260120_1530.csv
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    csv_file_path = f"alpha_result_{timestamp}.csv"
    
    # 调用主函数：查询 + 打印 + 保存 CSV
    result_df = view_alphas(test_gold_bag, save_csv_path=csv_file_path)
