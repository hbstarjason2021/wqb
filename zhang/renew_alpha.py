import logging
import os
import pickle
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import requests
from tqdm import tqdm

from machine_lib import *

# ===================== 全局配置类 =====================
class cfg:
    """全局配置类"""
    # 账号密码配置
    username = "XXX"
    password = "XXX"
    
    # 路径配置
    data_path = Path('./ppac')
    cache_path = Path('./cache')  # 新增缓存路径

# ===================== 工具函数 =====================
def save_obj(obj: object, name: str) -> None:
    """
    保存对象到文件中，以 pickle 格式序列化。
    
    Args:
        obj (object): 需要保存的对象。
        name (str): 文件名（不包含扩展名），保存的文件将以 '.pickle' 为扩展名。
    
    Returns:
        None: 此函数无返回值。
    
    Raises:
        pickle.PickleError: 如果序列化过程中发生错误。
        IOError: 如果文件写入过程中发生 I/O 错误。
    """
    with open(name + '.pickle', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def load_obj(name: str) -> object:
    """
    加载指定名称的 pickle 文件并返回其内容。
    
    此函数会打开一个以 `.pickle` 为扩展名的文件，并使用 `pickle` 模块加载其内容。
    
    Args:
        name (str): 不带扩展名的文件名称。
    
    Returns:
        object: 从 pickle 文件中加载的 Python 对象。
    
    Raises:
        FileNotFoundError: 如果指定的文件不存在。
        pickle.UnpicklingError: 如果文件内容无法被正确反序列化。
    """
    with open(name + '.pickle', 'rb') as f:
        return pickle.load(f)


def save_cached_data(filename: str, data: dict):
    """
    将字典数据保存到 pickle 文件。
    """
    cfg.cache_path.mkdir(parents=True, exist_ok=True)
    filepath = cfg.cache_path / f"{filename}.pickle"
    with open(filepath, 'wb') as f:
        pickle.dump(data, f)


def load_cached_data(filename: str) -> dict:
    """
    从 pickle 文件加载字典数据。
    """
    filepath = cfg.cache_path / f"{filename}.pickle"
    if not filepath.exists():
        return {}
    
    with open(filepath, 'rb') as f:
        try:
            return pickle.load(f)
        except EOFError:
            print(f"Warning: Cached file {filepath} is empty or corrupted. Returning empty dictionary.")
            return {}


def wait_get(sess, url: str, max_retries: int = 10) -> "Response":
    """
    发送带有重试机制的 GET 请求，直到成功或达到最大重试次数。
    
    此函数会根据服务器返回的 `Retry-After` 头信息进行等待，并在遇到 401 状态码时重新初始化配置。
    
    Args:
        url (str): 目标 URL。
        max_retries (int, optional): 最大重试次数，默认为 10。
    
    Returns:
        Response: 请求的响应对象。
    """
    retries = 0
    while retries < max_retries:
        while True:
            simulation_progress = sess.get(url)
            if simulation_progress.headers.get("Retry-After", 0) == 0:
                break
            time.sleep(float(simulation_progress.headers["Retry-After"]))
        
        if simulation_progress.status_code < 400:
            break
        else:
            time.sleep(2 ** retries)
            retries += 1
    
    return simulation_progress

# ===================== 登录与API交互函数 =====================
def sign_in(username, password):
    """登录接口，创建带认证的Session"""
    s = requests.Session()
    s.auth = (username, password)
    
    try:
        response = s.post('https://api.worldquantbrain.com/authentication')
        response.raise_for_status()
        logging.info("Successfully signed in")
        return s  # 修复：原代码为returns（拼写错误）
    except requests.exceptions.RequestException as e:  # 修复：原代码为exceptrequests（连写错误）
        logging.error(f"Login failed: {e}")
        return None


# 初始化登录会话
sess = sign_in(cfg.username, cfg.password)


def _get_alpha_pnl(alpha_id: str) -> pd.DataFrame:
    """
    获取指定 alpha 的 PnL数据，并返回一个包含日期和 PnL 的 DataFrame。
    
    此函数通过调用 WorldQuant Brain API 获取指定 alpha 的 PnL 数据，
    并将其转换为 pandas DataFrame 格式，方便后续数据处理。
    
    Args:
        alpha_id (str): Alpha 的唯一标识符。
    
    Returns:
        pd.DataFrame: 包含日期和对应 PnL 数据的 DataFrame，列名为 'Date' 和 alpha_id。
    """
    pnl = wait_get(sess, "https://api.worldquantbrain.com/alphas/" + alpha_id + "/recordsets/pnl").json()
    df = pd.DataFrame(pnl['records'], columns=[item['name'] for item in pnl['schema']['properties']])
    df = df.rename(columns={'date': 'Date', 'pnl': alpha_id})
    df = df[['Date', alpha_id]]
    
    return df


def _safe_get_alpha_pnl(alpha_id: str, max_retries: int = 3) -> Optional[pd.DataFrame]:
    """ 
    安全地获取指定 alpha 的 PnL 数据，带重试机制。 
    """
    for attempt in range(max_retries):
        try:
            return _get_alpha_pnl(alpha_id)
        except Exception as e:
            print(f"获取 alpha {alpha_id} PnL 失败（第 {attempt+1}/{max_retries} 次重试）: {e}")
            time.sleep(2 ** attempt)
    
    return None


def locate_alpha_detail(s, alpha_id, fetch_exp=True):
    """获取Alpha详细信息，包含glb_low计算"""
    while True:
        alpha = s.get("https://api.worldquantbrain.com/alphas/" + alpha_id)
        if "retry-after" in alpha.headers:
            time.sleep(float(alpha.headers["Retry-After"]))
        else:
            break
    
    string = alpha.content.decode('utf-8')
    metrics = json.loads(string)
    
    # 新增：计算glb_low值并存入metrics
    sharpe_checks = ['LOW_GLB_AMER_SHARPE', 'LOW_GLB_EMEA_SHARPE', 'LOW_GLB_APAC_SHARPE']
    glb_low_values = []
    
    for check in metrics.get('is', {}).get('checks', []):
        if check.get('name') in sharpe_checks:
            glb_low_values.append(str(check.get('value', 0)))
    
    metrics['glb_low'] = '|'.join(glb_low_values) if glb_low_values else '0'
    
    if not fetch_exp:
        return metrics
    
    dateCreated = metrics["dateCreated"]
    sharpe = metrics["is"]["sharpe"]
    fitness = metrics["is"]["fitness"]
    turnover = metrics["is"]["turnover"]
    margin = metrics["is"]["margin"]
    decay = metrics["settings"]["decay"]
    exp = metrics['regular']['code']
    
    # 新增：计算glb_low值
    # 定义需要检查的三个项目
    sharpe_checks = ['LOW_GLB_AMER_SHARPE', 'LOW_GLB_EMEA_SHARPE', 'LOW_GLB_APAC_SHARPE']
    glb_low_values = []
    
    # 遍历所有检查项
    for check in metrics.get('is', {}).get('checks', []):
        if check.get('name') in sharpe_checks:
            # 获取value值并添加到列表
            glb_low_values.append(str(check.get('value', 0)))
    
    # 用|连接所有value值，如果没有值则设为'0'
    glb_low = '|'.join(glb_low_values) if glb_low_values else '0'
    
    triple = [alpha_id, exp, sharpe, turnover, fitness, margin, dateCreated, decay, glb_low]
    return triple


def _safe_get_alpha_details(alpha_id: str, max_retries: int = 3) -> Optional[dict]:
    """ 
    安全地获取指定 alpha 的详细信息，带重试机制。 
    """
    for attempt in range(max_retries):
        try:
            return locate_alpha_detail(sess, alpha_id, fetch_exp=False)
        except Exception as e:
            print(f"获取 alpha {alpha_id} 详情失败（第 {attempt+1}/{max_retries} 次重试）: {e}")
            time.sleep(2 ** attempt)
    
    return None


def base_check_alpha_in_memory(alpha_detail: dict) -> str:
    """
    Checks alpha checks from a pre-loaded detail object. No network calls.
    """
    if not alpha_detail or 'is' not in alpha_detail or 'checks' not in alpha_detail['is']:
        return "Error"

    try:
        checks_df = pd.DataFrame(alpha_detail['is']['checks'])
        if (checks_df["result"] == "FAIL").any() or (checks_df["result"] == "ERROR").any():
            return 'Fail'
        return 'Pass'
    except Exception:
        return "Error"

# ===================== 数据处理核心函数 =====================
def _preprocess_alphas(items_to_check, pnl_results, details_results, correlation_threshold=0.9):
    """
    对大量alpha进行“代表筛选”预处理。
    此函数现在接收已下载的数据，只进行计算。
    新增功能：当Sharpe为负时，不参与对比，直接舍弃
    """
    # 首先检查输入数据是否为空
    if not items_to_check:
        print("预处理: 待处理Alpha列表为空，直接返回")
        return []

    # 检查PNL数据是否为空
    if pnl_results is None or (
        isinstance(pnl_results, (pd.DataFrame, pd.Series)) and pnl_results.empty) or (
        isinstance(pnl_results, list) and len(pnl_results) == 0):
        print("预处理错误: PNL数据为空")
        return []

    # 检查Details数据是否为空
    if details_results is None or (
        isinstance(details_results, (pd.DataFrame, pd.Series)) and details_results.empty) or (
        isinstance(details_results, list) and len(details_results) == 0):
        print("预处理错误: Details数据为空")
        return []

    print(f"--- 开始对 {len(items_to_check)} 个 Alphas 进行“代表筛选”预处理 (阈值: {correlation_threshold}) ---")

    all_alphas_data = []
    negative_sharpe_alphas = []  # 存储Sharpe为负的alpha

    for item in items_to_check:
        alpha_id = item[0]
        detail = details_results.get(alpha_id)
        pnl_df = pnl_results.get(alpha_id)

        if detail and pnl_df is not None:
            # 防御性编程：确保pnl_df有'Date'列或已经是索引
            if 'Date' in pnl_df.columns:
                # 先将Date列转换为datetime并标准化
                pnl_df['Date'] = pd.to_datetime(pnl_df['Date']).dt.normalize()
                pnl_df = pnl_df.set_index('Date')
            else:
                # 标准化日期格式
                pnl_df.index = pd.to_datetime(pnl_df.index).normalize()
            
            # 筛选最近四年数据
            max_date = pnl_df.index.max()
            four_years_ago = max_date - pd.DateOffset(years=4)
            pnl_df = pnl_df[pnl_df.index > four_years_ago]
            
            # 移除替换零值的操作，将其移到 calc_self_corr 函数中处理收益率之后
            # pnl_df = pnl_df.replace(0, 1e-10)
            
            # 将处理后的 pnl_df 重新存回 pnl_results 字典
            pnl_results[alpha_id] = pnl_df
            
            # 获取Sharpe比率并检查是否为负
            sharpe = detail.get('is', {}).get('sharpe', 0) if detail.get('is') else 0

            # 如果Sharpe为负，直接加入negative_sharpe_alphas列表
            if sharpe <= 0:
                print(f" Alpha {alpha_id}: Sharpe为负 ({sharpe:.4f})，将进行相关性剪枝")
                negative_sharpe_alphas.append({
                    'item': item,
                    'pnl': pnl_df,
                    'sharpe': sharpe
                })
                continue
            
            base_check_status = base_check_alpha_in_memory(detail)
            print(f" Alpha {alpha_id}: Base Check Status: {base_check_status}")

            if base_check_status == 'Pass':
                base_check_passed = True
            else:
                base_check_passed = False
            
            all_alphas_data.append({
                'item': item,
                'pnl': pnl_df,
                'passed': base_check_passed,
                'fitness': abs(detail.get('is', {}).get('fitness', 0))
            })

    passed_alphas = sorted([d for d in all_alphas_data if d['passed']], key=lambda x: x['fitness'], reverse=True)
    remaining_alphas = [d for d in all_alphas_data if not d['passed']]
    
    print(f" 通过 Base Check 的 Alpha 数量: {len(passed_alphas)}")
    print(f" 未通过 Base Check 的 Alpha 数量: {len(remaining_alphas)}")
    print(f" Sharpe 为负的 Alpha 数量: {len(negative_sharpe_alphas)}")

    survivors = []
    while passed_alphas:
        representative = passed_alphas.pop(0)
        rep_id = representative['item'][0]
        # 修正：将代表的PnL转换为日收益率
        rep_rets_series = (representative['pnl'][rep_id] - representative['pnl'][rep_id].ffill().shift(1)).dropna()

        print(f"\n选出代表: {rep_id} (Fitness: {representative['fitness']:.4f})")
        survivors.append(representative['item'])

        # 批量筛选 passed_alphas
        new_passed_alphas = []
        for d in passed_alphas:
            alpha_id = d['item'][0]
            # 修正：将被比较的Alpha PnL也转换为日收益率
            alpha_rets_series = (d['pnl'][alpha_id] - d['pnl'][alpha_id].ffill().shift(1)).dropna()

            # 对齐并计算相关性
            aligned_rep, aligned_alpha = rep_rets_series.align(alpha_rets_series, join='inner')
            if not aligned_alpha.empty:
                corr = aligned_alpha.corr(aligned_rep)
            else:
                corr = np.nan

            if np.isnan(corr) or abs(corr) < correlation_threshold:
                new_passed_alphas.append(d)
            else:
                print(f" Alpha {alpha_id} (Passed): 相关性 {corr:.4f} - 丢弃 (与代表 {rep_id} 高相关)")
        
        passed_alphas = new_passed_alphas

        # 批量筛选 remaining_alphas
        new_remaining_alphas = []
        for d in remaining_alphas:
            alpha_id = d['item'][0]
            # 修正：同样转换为日收益率
            alpha_rets_series = (d['pnl'][alpha_id] - d['pnl'][alpha_id].ffill().shift(1)).dropna()
            
            aligned_rep, aligned_alpha = rep_rets_series.align(alpha_rets_series, join='inner')
            if not aligned_alpha.empty:
                corr = aligned_alpha.corr(aligned_rep)
            else:
                corr = np.nan

            if np.isnan(corr) or abs(corr) < correlation_threshold:
                new_remaining_alphas.append(d)
            else:
                print(f" Alpha {alpha_id} (Failed): 相关性 {corr:.4f} - 丢弃 (与代表 {rep_id} 高相关)")
        
        remaining_alphas = new_remaining_alphas

    survivors.extend([d['item'] for d in remaining_alphas])
    return survivors


def load_region_data(region):
    """加载指定区域的Alpha ID和PNL数据"""
    if region is None:
        print("region is None, please input a region")
        return None, None
    
    alpha_ids = []
    try:
        alpha_ids = load_obj(str(cfg.data_path / f'{region}_alpha_ids'))
        alpha_pnls = load_obj(str(cfg.data_path / f'{region}_alpha_pnls'))
    except FileNotFoundError:
        print(f"Files for {region} not found. Returning empty lists.")
        return [], None
    
    return alpha_ids, alpha_pnls


def get_alpha_pnls(
    alphas: list[dict],
    alpha_pnls: Optional[pd.DataFrame] = None,
    alpha_ids: Optional[dict[str, list]] = None
) -> Tuple[dict[str, list], pd.DataFrame]:
    """
    获取 alpha 的 PnL 数据，并按区域分类 alpha 的 ID。
    
    Args:
        alphas (list[dict]): 包含 alpha 信息的列表，每个元素是一个字典，包含 alpha 的 ID 和设置等信息。
        alpha_pnls (Optional[pd.DataFrame], 可选): 已有的 alpha PnL 数据，默认为空的 DataFrame。
        alpha_ids (Optional[dict[str, list]], 可选): 按区域分类的 alpha ID 字典，默认为空字典。
    
    Returns:
        Tuple[dict[str, list], pd.DataFrame]:
        - 按区域分类的 alpha ID 字典。
        - 包含所有 alpha 的 PnL 数据的 DataFrame。
    """
    if alpha_ids is None:
        alpha_ids = defaultdict(list)
    if alpha_pnls is None:
        alpha_pnls = pd.DataFrame()

    new_alphas = [item for item in alphas if item['id'] not in alpha_pnls.columns]
    if not new_alphas:
        return alpha_ids, alpha_pnls

    for item_alpha in new_alphas:
        alpha_ids[item_alpha['settings']['region']].append(item_alpha['id'])
    
    fetch_pnl_func = lambda alpha_id: _get_alpha_pnl(alpha_id).set_index('Date')
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(fetch_pnl_func, [item['id'] for item in new_alphas])
        alpha_pnls = pd.concat([alpha_pnls] + list(results), axis=1)
    
    alpha_pnls.sort_index(inplace=True)
    return alpha_ids, alpha_pnls


def get_os_alphas(limit: int = 100, get_first: bool = False) -> List[Dict]:
    """
    获取OS阶段的alpha列表。
    
    此函数通过调用WorldQuant Brain API获取用户的alpha列表，支持分页获取，并可以选择只获取第一个结果。
    
    Args:
        limit (int, optional): 每次请求获取的alpha数量限制。默认为100。
        get_first (bool, optional): 是否只获取第一次请求的alpha结果。如果为True，则只请求一次。默认为False。
    
    Returns:
        List[Dict]: 包含alpha信息的字典列表，每个字典表示一个alpha。
    """
    fetched_alphas = []
    offset = 0
    retries = 0
    total_alphas = 100
    
    while len(fetched_alphas) < total_alphas:
        print(f"Fetching alphas from offset {offset} to {offset+limit}")
        url = f"https://api.worldquantbrain.com/users/self/alphas?stage=OS&limit={limit}&offset={offset}&order=-dateSubmitted"
        res = wait_get(sess, url).json()
        
        if offset == 0:
            total_alphas = res['count']
        
        alphas = res["results"]
        fetched_alphas.extend(alphas)
        
        if len(alphas) < limit:
            break
        
        offset += limit
        if get_first:
            break
    
    return fetched_alphas[:total_alphas]


def calc_self_corr(
    alpha_id: str,
    os_alpha_rets: pd.DataFrame,
    os_alpha_ids: dict[str, list],
    alpha_result: dict,
    alpha_pnl_series: pd.Series,
) -> float:
    """
    计算指定 alpha 与其他 alpha 的最大自相关性。
    
    Args:
        alpha_id (str): 目标 alpha 的唯一标识符。
        os_alpha_rets (pd.DataFrame): 其他 alpha 的收益率数据。
        os_alpha_ids (dict[str, str]): 其他 alpha 的标识符映射。
        alpha_result (dict): 目标 alpha 的详细信息 (已预加载)。
        alpha_pnl_series (pd.Series): 目标 alpha 的 PnL 序列数据 (已预加载和预处理)。
    
    Returns:
        float: 最大自相关性值。
    """
    if alpha_pnl_series is None or alpha_pnl_series.empty:
        print(f"Warning: Alpha {alpha_id} PnL data (Series) is empty or not provided.")
        return 0.0
    
    # 计算日收益率
    alpha_rets = (alpha_pnl_series - alpha_pnl_series.ffill().shift(1)).dropna()
    # 替换零值以避免相关性计算问题
    alpha_rets = alpha_rets.replace(0, 1e-10)
    
    # alpha_rets 已经由 _preprocess_alphas 进行了筛选和 NaN 移除，此处不再需要重复筛选
    # 计算相关性
    region = alpha_result['settings']['region']
    if region not in os_alpha_ids:
        print(f"Warning: Region {region} not found in os_alpha_ids. Skipping...")
        return 0.0
    
    region_alpha_ids = os_alpha_ids[region]
    # 过滤掉 os_alpha_rets 中不存在的 alpha_id，并确保至少有一个共同的列
    valid_region_alpha_ids = [aid for aid in region_alpha_ids if aid in os_alpha_rets.columns]

    if not valid_region_alpha_ids:
        print(f"Warning: No valid alphas found in os_alpha_rets for region {region}. Skipping correlation calculation.")
        return 0.0
    
    region_alpha_rets = os_alpha_rets[valid_region_alpha_ids]

    # 确保 alpha_rets 和 region_alpha_rets 索引对齐，只取共同的日期
    aligned_alpha_rets, aligned_region_alpha_rets = alpha_rets.align(region_alpha_rets, join='inner', axis=0)
    if aligned_alpha_rets.empty or aligned_region_alpha_rets.empty:
        print(f"Warning: No overlapping dates for correlation calculation for alpha {alpha_id}.")
        return 0.0
    
    # 添加波动性过滤和零方差检测，参考 user_self_corr.py
    os_alpha_rets_clean = aligned_region_alpha_rets.dropna(how='all', axis=1)
    std_devs = os_alpha_rets_clean.std()
    valid_columns_for_corr = std_devs[std_devs > 1e-20].index
    os_alpha_rets_clean = os_alpha_rets_clean[valid_columns_for_corr]
    
    if os_alpha_rets_clean.empty:
        print(f"Warning: No valid queue alphas after volatility filter for alpha {alpha_id}.")
        return 0.0
    
    # 检查收益率序列方差是否为零
    alpha_var = aligned_alpha_rets.var()
    queue_var = os_alpha_rets_clean.var().max()

    if alpha_var < 1e-30 or queue_var < 1e-30:
        print(f"警告: 零方差检测 - 新Alpha方差: {alpha_var:.2e}, 队列最大方差: {queue_var:.2e} for alpha {alpha_id}")
        return 0.0
    
    corr_series = os_alpha_rets_clean.corrwith(aligned_alpha_rets)
    self_corr = corr_series.max() if not corr_series.empty else 0.0
    
    if np.isnan(self_corr):
        self_corr = 0.0
    
    return self_corr


def download_data(flag_increment=True):
    """
    下载数据并保存到指定路径。
    
    此函数会检查数据是否已经存在，如果不存在，则从 API 下载数据并保存到指定路径。
    
    Args:
        flag_increment (bool): 是否使用增量下载，默认为 True。
    """
    if flag_increment:
        try:
            os_alpha_ids = load_obj(str(cfg.data_path / 'os_alpha_ids'))
            os_alpha_pnls = load_obj(str(cfg.data_path / 'os_alpha_pnls'))
            ppac_alpha_ids = load_obj(str(cfg.data_path / 'ppac_alpha_ids'))
            exist_alpha = [alpha for ids in os_alpha_ids.values() for alpha in ids]
        except Exception as e:
            logging.error(f"Failed to load existing data: {e}")
            os_alpha_ids = None
            os_alpha_pnls = None
            exist_alpha = []
            ppac_alpha_ids = []
    else:
        os_alpha_ids = None
        os_alpha_pnls = None
        exist_alpha = []
        ppac_alpha_ids = []

    if os_alpha_ids is None:
        alphas = get_os_alphas(limit=100, get_first=False)
    else:
        alphas = get_os_alphas(limit=30, get_first=True)
    
    alphas = [item for item in alphas if item['id'] not in exist_alpha]
    ppac_alpha_ids += [item['id'] for item in alphas for item_match in item['classifications'] if item_match['name'] == 'Power Pool Alpha']
    
    os_alpha_ids, os_alpha_pnls = get_alpha_pnls(alphas, alpha_pnls=os_alpha_pnls, alpha_ids=os_alpha_ids)
    
    save_obj(os_alpha_ids, str(cfg.data_path / 'os_alpha_ids'))
    save_obj(os_alpha_pnls, str(cfg.data_path / 'os_alpha_pnls'))
    save_obj(ppac_alpha_ids, str(cfg.data_path / 'ppac_alpha_ids'))
    
    print(f'新下载的alpha数量: {len(alphas)}, 目前总共alpha数量: {os_alpha_pnls.shape[1]}')


def load_data(tag=None):
    """
    加载数据。
    
    此函数会检查数据是否已经存在，如果不存在，则从 API 下载数据并保存到指定路径。
    
    Args:
        tag (str): 数据标记，默认为 None。
    """
    os_alpha_ids = load_obj(str(cfg.data_path / 'os_alpha_ids'))
    os_alpha_pnls = load_obj(str(cfg.data_path / 'os_alpha_pnls'))
    ppac_alpha_ids = load_obj(str(cfg.data_path / 'ppac_alpha_ids'))
    
    if tag == 'PPAC':
        for item in os_alpha_ids:
            os_alpha_ids[item] = [alpha for alpha in os_alpha_ids[item] if alpha in ppac_alpha_ids]
    elif tag == 'SelfCorr':
        for item in os_alpha_ids:
            os_alpha_ids[item] = [alpha for alpha in os_alpha_ids[item] if alpha not in ppac_alpha_ids]
    else:
        os_alpha_ids = os_alpha_ids
    
    exist_alpha = [alpha for ids in os_alpha_ids.values() for alpha in ids]
    os_alpha_pnls = os_alpha_pnls[exist_alpha]
    
    os_alpha_rets = os_alpha_pnls - os_alpha_pnls.ffill().shift(1)
    os_alpha_rets.index = pd.to_datetime(os_alpha_rets.index).normalize()  # 标准化索引
    os_alpha_rets = os_alpha_rets[os_alpha_rets.index > (os_alpha_rets.index.max() - pd.DateOffset(years=4))]
    
    return os_alpha_ids, os_alpha_rets

# ===================== 主逻辑 =====================
# 增量下载数据
download_data(flag_increment=True)

# 加载数据， 如果需要使用不同的标签，可以传入 tag 参数， 例如 tag='PPAC' 或 tag='SelfCorr'
os_alpha_ids, os_alpha_rets = load_data()

# 指定区域
region = "GLB"

# 获取初始Alpha列表
th_tracker = get_alphas(
    sess, 
    "2026-01-20", 
    "2026-01-23", 
    1.3, 
    0.7, 
    100,
    100, 
    region, 
    "MINVOL1M", 
    1, 
    "EQUITY", 
    1000, 
    "track", 
    color_exclude='RED'
)

## 将get的alpha的id取出至stone_bag
# 将 fail 的去掉了
initial_items = th_tracker.get('next', [])
initial_items = [item for item in initial_items if item[2] > 0]
print(f"获取到 {len(initial_items)} 个初始 alphas")

# --- 1. 数据前置下载 ---
print("\n--- 开始前置下载所有 Alpha 的 PnL 和详细信息 ---")

# 尝试加载缓存数据
cached_pnl = load_cached_data('ppa_select_alpha_pnl_cache')
cached_details = load_cached_data('ppa_select_alpha_details_cache')

alpha_pnl_results = {k: v for k, v in cached_pnl.items() if isinstance(v, pd.DataFrame)}
alpha_details_results = cached_details
all_alpha_ids = [item[0] for item in initial_items]

# 筛选出需要下载的alpha ID
pnl_to_download = [alpha_id for alpha_id in all_alpha_ids if alpha_id not in alpha_pnl_results]
details_to_download = [alpha_id for alpha_id in all_alpha_ids if alpha_id not in alpha_details_results]

print(f" 已从缓存加载 {len(alpha_pnl_results)} 个 PnL 和 {len(alpha_details_results)} 个 Details。")
print(f" 需要下载 {len(pnl_to_download)} 个 PnL 和 {len(details_to_download)} 个 Details。")

download_batch_size = 10  # 每下载10个保存一次
with ThreadPoolExecutor(max_workers=10) as executor:
    # PNL下载任务
    if pnl_to_download:
        future_to_id_pnl = {executor.submit(_safe_get_alpha_pnl, alpha_id): alpha_id for alpha_id in pnl_to_download}
        for i, future in enumerate(tqdm(as_completed(future_to_id_pnl), total=len(pnl_to_download), desc="下载 PnL")):
            alpha_id = future_to_id_pnl[future]
            try:
                result = future.result()
                if result is not None:
                    alpha_pnl_results[alpha_id] = result
            except Exception as e:
                print(f"下载 PnL 发生严重错误 (alpha_id: {alpha_id}): {str(e)}")

            # 每下载一定数量就保存一次
            if (i+1) % download_batch_size == 0 or (i+1) == len(pnl_to_download):
                save_cached_data('ppa_select_alpha_pnl_cache', alpha_pnl_results)
                print(f" 已保存 {len(alpha_pnl_results)} 个 PnL 数据到缓存。")

    # Details下载任务
    if details_to_download:
        future_to_id_details = {executor.submit(_safe_get_alpha_details, alpha_id): alpha_id for alpha_id in details_to_download}
        for i, future in enumerate(tqdm(as_completed(future_to_id_details), total=len(details_to_download), desc="下载 Details")):
            alpha_id = future_to_id_details[future]
            try:
                result = future.result()
                if result is not None:
                    alpha_details_results[alpha_id] = result
            except Exception as e:
                print(f"下载 Details 发生严重错误 (alpha_id: {alpha_id}): {str(e)}")

            # 每下载一定数量就保存一次
            if (i+1) % download_batch_size == 0 or (i+1) == len(details_to_download):
                save_cached_data('ppa_select_alpha_details_cache', alpha_details_results)
                print(f" 已保存 {len(alpha_details_results)} 个 Details 数据到缓存。")

# 确保最终保存一次所有数据
save_cached_data('ppa_select_alpha_pnl_cache', alpha_pnl_results)
save_cached_data('ppa_select_alpha_details_cache', alpha_details_results)
print(f"--- 前置下载完成: 获取到 {len(alpha_pnl_results)} 个 PnL, {len(alpha_details_results)} 个 Details ---\n")

# --- 2. 执行预处理 ---
stone_bag_items = _preprocess_alphas(initial_items, alpha_pnl_results, alpha_details_results)
stone_bag = [item[0] for item in stone_bag_items]  # 预处理后，只保留id列表
print(f"\n--- 预处理完成，剩余 {len(stone_bag)} 个 Alphas 进入下一步相关性计算 ---\n")
print("剩余的 Alpha IDs:", stone_bag)

gold_bag = []

# ===================== 辅助函数 =====================
def calc_self_corr_wrapper(alpha_id, alpha_detail, alpha_pnl, os_alpha_rets, os_alpha_ids):
    """
    calc_self_corr的包装函数，用于多线程调用。
    接收单个alpha的detail和pnl，避免在函数内部查询或拉取。
    """
    try:
        if alpha_detail is None:
            print(f"Warning: Alpha detail not provided for {alpha_id}. Skipping correlation calculation.")
            return alpha_id, 0.0
        
        if alpha_pnl is None or alpha_pnl.empty:
            print(f"Warning: Alpha PnL not provided or empty for {alpha_id}. Skipping correlation calculation.")
            return alpha_id, 0.0
        
        # 直接传递 PnL Series
        alpha_pnl_series = alpha_pnl[alpha_id]
        self_corr = calc_self_corr(
            alpha_id=alpha_id,
            os_alpha_rets=os_alpha_rets,
            os_alpha_ids=os_alpha_ids,
            alpha_result=alpha_detail,
            alpha_pnl_series=alpha_pnl_series
        )
        return alpha_id, self_corr
    except Exception as e:
        print(f"Error calculating self correlation for {alpha_id}: {e}")
        return alpha_id, 0.0


# 在处理stone_bag的部分替换为以下代码:
# 初始化一个空列表来存储符合条件的 alpha_id 和 self_corr
valid_alphas = []

# 获取总数
total = len(stone_bag)
print(f"开始处理 {total} 个 alpha 的自相关计算")

# 并行计算自相关性
results = []
with ThreadPoolExecutor(max_workers=10) as executor:  # 可根据需要调整max_workers
    # 准备任务参数
    tasks = []
    for alpha_id in stone_bag:
        alpha_detail = alpha_details_results.get(alpha_id)
        alpha_pnl = alpha_pnl_results.get(alpha_id)
        if alpha_detail and alpha_pnl is not None:
            tasks.append(executor.submit(
                calc_self_corr_wrapper, 
                alpha_id, 
                alpha_detail, 
                alpha_pnl, 
                os_alpha_rets, 
                os_alpha_ids
            ))
        else:
            print(f"Warning: Missing detail or PnL for {alpha_id}, skipping correlation calculation.")

    # 收集结果
    for future in tqdm(as_completed(tasks), total=len(tasks), desc="计算自相关性"):
        try:
            result = future.result()
            results.append(result)
        except Exception as e:
            # 异常已经在 wrapper 内部处理，这里可以添加额外的日志
            print(f"A future completed with an exception: {e}")

# 处理结果
for alpha_id, self_corr in results:  # 移除 alpha_pnls_result
    # 判断自相关性是否小于 0.5
    if self_corr < 0.5:
        # 如果符合条件，添加到列表中
        print(f"add alpha {alpha_id} with self_corr {self_corr:.4f} to list")
        valid_alphas.append((alpha_id, self_corr))
        print(f"valid_alphas: {len(valid_alphas)}")
    else:
        print(f"Excluded alpha {alpha_id} with self_corr {self_corr:.4f}")

# 移除保存 exist_alpha_pnls 的逻辑，因为这部分应该在数据下载和加载时统一处理。

# 打印出符合条件的 alpha_id 和 self_corr
for alpha_id, self_corr in valid_alphas:
    print(f"Alpha ID: {alpha_id}, Self Correlation: {self_corr}")

# # 打印可提交的alpha信息并按sharpe排序，在网页上找到alpha手动提交
# view_alphas(valid_alphas)

# 另外如果你想要以margin为顺序排序，用到下面的就可以了
def view_alphas_margin(gold_bag, region="GLB"):  # 添加region参数，默认为GLB
    """按margin排序展示Alpha信息并保存为CSV"""
    s = sess  # 使用全局 session
    sharp_list = []

    for i, (gold, pc) in enumerate(gold_bag):
        # 直接从预下载的结果中获取
        metrics = alpha_details_results.get(gold)
        if not metrics:
            print(f"警告: 在预下载的详情中未找到 Alpha {gold}，跳过。")
            continue
        
        pass_fail_result = base_check_alpha_in_memory(metrics)
        
        # 从 metrics 字典中提取所需信息
        dateCreated = metrics.get("dateCreated", "")
        sharpe = metrics.get("is", {}).get("sharpe", 0)
        fitness = metrics.get("is", {}).get("fitness", 0)
        turnover = metrics.get("is", {}).get("turnover", 0)
        margin = metrics.get("is", {}).get("margin", 0)
        exp = metrics.get('regular', {}).get('code', '')
        # glb_low 现在直接从预加载的 metrics 中获取
        glb_low = metrics.get('glb_low', '0')

        # 提取pyramids
        matches_pyramid = next((check for check in metrics.get('is', {}).get('checks', []) if check.get('name') == 'MATCHES_PYRAMID'), None)
        pyramids = ' '.join([p.get('name', '') for p in matches_pyramid.get('pyramids', [])]) if matches_pyramid else ''

        info = [
            gold,  # alpha_id
            sharpe,
            turnover,
            fitness,
            margin,
            pass_fail_result,  # result
            dateCreated,
            exp,
            glb_low,
            pc,  # self_corr
            pyramids
        ]
        sharp_list.append(info)
    
    # 按margin降序排序
    sharp_list.sort(reverse=True, key=lambda x: float(x[4]))  # x[4] 是 margin 的位置
    
    # 将结果保存到CSV文件
    # 定义列名
    columns = [
        'alpha_id', 'sharpe', 'turnover', 'fitness', 'margin', 
        'result', 'dateCreated', 'expression', 'glb_low', 'self_corr', 'pyramids'
    ]

    # 创建DataFrame
    df = pd.DataFrame(sharp_list, columns=columns)

    # 确保submit目录存在
    os.makedirs('submit', exist_ok=True)

    # 构建CSV文件路径
    csv_path = f'submit/{region}_ppa_select.csv'

    # 保存到CSV
    df.to_csv(csv_path, index=False)
    print(f"数据已成功保存到 {csv_path}")

    return sharp_list  # 保持原函数的返回值类型

# 打印可提交的alpha信息并按margin排序，在网页上找到alpha手动提交
view_alphas_margin(valid_alphas, region=region)
print(f"任务完成。")
