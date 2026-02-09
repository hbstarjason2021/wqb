from datetime import datetime, timedelta
import random
import requests
import pandas as pd
import logging
import time
import warnings
from typing import Optional, Tuple
from typing import Tuple, Dict, List
from typing import Union, List, Tuple
from concurrent.futures import ThreadPoolExecutor
import pickle
from collections import defaultdict
import numpy as np
from pathlib import Path
import json
import os


def sign_in(username, password):
    s = requests.Session()
    s.auth = (username, password)
    try:
        response = s.post('https://api.worldquantbrain.com/authentication')
        response.raise_for_status()
        logging.info("Successfully signed in")
        return s
    except requests.exceptions.RequestException as e:
        logging.error(f"Login failed: {e}")
        return None


class SessionManager:
    """
    统一的session管理器，避免重复登录
    """

    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.session = None
        self.last_login_time = None
        self.login_count = 0
        self._lock = False  # 简单的锁，防止并发登录

    def get_session(self, force_refresh=False):
        """
        获取有效的session，如果过期或不存在则重新登录
        """
        # 如果session存在且未强制刷新，直接返回
        if self.session and not force_refresh:
            return self.session

        # 防止并发登录
        if self._lock:
            # 等待其他线程完成登录
            while self._lock:
                time.sleep(0.1)
            return self.session

        self._lock = True
        try:
            new_session = sign_in(self.username, self.password)
            if new_session:
                self.session = new_session
                self.last_login_time = time.time()
                self.login_count += 1
                print(f"   🔐 [SessionManager] 登录成功 (总登录次数: {self.login_count})")
                return self.session
            else:
                print("   ❌ [SessionManager] 登录失败")
                return None
        finally:
            self._lock = False

    def refresh_on_401(self):
        """
        在遇到401错误时刷新session
        """
        print("   🔄 [SessionManager] 检测到401错误，刷新session...")
        return self.get_session(force_refresh=True)

    def update_session(self, new_session):
        """
        更新session（用于外部已经登录的情况）
        """
        if new_session:
            self.session = new_session
            self.last_login_time = time.time()
            self.login_count += 1


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


def wait_get(url: str, max_retries: int = 10) -> "requests.Response":
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
    pnl = wait_get("https://api.worldquantbrain.com/alphas/" + alpha_id + "/recordsets/pnl").json()
    df = pd.DataFrame(pnl['records'], columns=[item['name'] for item in pnl['schema']['properties']])
    df = df.rename(columns={'date': 'Date', 'pnl': alpha_id})
    df = df[['Date', alpha_id]]
    return df


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

    # 验证alphas数据结构并过滤有效数据
    valid_alphas = []
    for item in alphas:
        try:
            if not isinstance(item, dict):
                print(f"   ⚠️  [get_alpha_pnls] 跳过无效数据（非字典类型）: {type(item)}")
                continue

            if 'id' not in item:
                print(f"   ⚠️  [get_alpha_pnls] 跳过无效数据（缺少id字段）: {item}")
                continue

            if 'settings' not in item or 'region' not in item.get('settings', {}):
                print(f"   ⚠️  [get_alpha_pnls] 跳过无效数据（缺少settings.region）: {item.get('id', 'unknown')}")
                continue

            # 检查是否已存在于alpha_pnls中
            if item['id'] not in alpha_pnls.columns:
                valid_alphas.append(item)
        except Exception as e:
            print(f"   ⚠️  [get_alpha_pnls] 处理数据时出错，跳过: {type(e).__name__} - {str(e)[:50]}")
            continue

    if not valid_alphas:
        return alpha_ids, alpha_pnls

    # 按区域分类alpha ID
    for item_alpha in valid_alphas:
        try:
            alpha_ids[item_alpha['settings']['region']].append(item_alpha['id'])
        except Exception as e:
            print(f"   ⚠️  [get_alpha_pnls] 分类alpha时出错，跳过 {item_alpha.get('id', 'unknown')}: {type(e).__name__}")
            continue

    # 获取PnL数据（带错误处理）
    def safe_get_pnl(alpha_id):
        try:
            return _get_alpha_pnl(alpha_id).set_index('Date')
        except Exception as e:
            print(f"   ⚠️  [get_alpha_pnls] 获取 {alpha_id} 的PnL失败，跳过: {type(e).__name__} - {str(e)[:50]}")
            return None

    fetch_pnl_func = safe_get_pnl
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(fetch_pnl_func, [item['id'] for item in valid_alphas])

    # 过滤掉None结果
    valid_results = [r for r in results if r is not None]
    if valid_results:
        alpha_pnls = pd.concat([alpha_pnls] + valid_results, axis=1)
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
    total_alphas = 100  # 默认值

    while len(fetched_alphas) < total_alphas:
        try:
            print(f"Fetching alphas from offset {offset} to {offset + limit}")
            url = f"https://api.worldquantbrain.com/users/self/alphas?stage=OS&limit={limit}&offset={offset}&order=-dateSubmitted"
            res = wait_get(url).json()

            # 检查响应格式
            if not isinstance(res, dict):
                print(f"   ⚠️ API响应格式错误，不是字典类型: {type(res)}")
                break

            # 检查是否有错误信息
            if 'error' in res or 'message' in res:
                error_msg = res.get('error') or res.get('message', 'Unknown error')
                print(f"   ⚠️ API返回错误: {error_msg}")
                break

            # 安全获取count字段
            if offset == 0:
                if 'count' in res:
                    total_alphas = res['count']
                    print(f"   📊 总alpha数量: {total_alphas}")
                else:
                    # 如果没有count字段，尝试从results长度推断，或使用默认值
                    if 'results' in res and isinstance(res['results'], list):
                        # 如果第一次请求就返回了所有结果，使用results长度
                        if len(res['results']) < limit:
                            total_alphas = len(res['results'])
                            print(f"   ⚠️ 响应中没有'count'字段，从results长度推断: {total_alphas}")
                        else:
                            # 如果结果数等于limit，可能还有更多，设置一个较大的上限
                            total_alphas = limit * 10  # 假设最多10页
                            print(f"   ⚠️ 响应中没有'count'字段，使用默认上限: {total_alphas}")
                    else:
                        print(f"   ⚠️ 响应中既没有'count'也没有'results'字段，使用默认值: {total_alphas}")
                        break

            # 安全获取results字段
            if 'results' not in res:
                print(f"   ⚠️ API响应中没有'results'字段，跳过")
                break

            alphas = res["results"]
            if not isinstance(alphas, list):
                print(f"   ⚠️ 'results'字段不是列表类型: {type(alphas)}")
                break

            fetched_alphas.extend(alphas)
            print(f"   ✅ 获取到 {len(alphas)} 个alpha，累计: {len(fetched_alphas)}")

            if len(alphas) < limit:
                print(f"   📊 已获取所有alpha（最后一页仅{len(alphas)}个）")
                break
            offset += limit
            if get_first:
                break

        except KeyError as e:
            print(f"   ⚠️ KeyError: {e}，跳过此次请求")
            # 如果已经获取到一些alpha，返回已有的
            if fetched_alphas:
                print(f"   ✅ 返回已获取的 {len(fetched_alphas)} 个alpha")
                return fetched_alphas
            break
        except Exception as e:
            print(f"   ⚠️ 获取alpha列表时出错: {type(e).__name__} - {str(e)[:100]}")
            # 如果已经获取到一些alpha，返回已有的
            if fetched_alphas:
                print(f"   ✅ 返回已获取的 {len(fetched_alphas)} 个alpha")
                return fetched_alphas
            break

    # 确保不超过total_alphas（如果total_alphas是推断的，可能不准确）
    if total_alphas > len(fetched_alphas):
        return fetched_alphas
    return fetched_alphas[:total_alphas]


def calc_self_corr(
        alpha_id: str,
        os_alpha_rets: pd.DataFrame | None = None,
        os_alpha_ids: dict[str, str] | None = None,
        alpha_result: dict | None = None,
        return_alpha_pnls: bool = False,
        alpha_pnls: pd.DataFrame | None = None
) -> float | tuple[float, pd.DataFrame]:
    """
    计算指定 alpha 与其他 alpha 的最大自相关性。
    Args:
        alpha_id (str): 目标 alpha 的唯一标识符。
        os_alpha_rets (pd.DataFrame | None, optional): 其他 alpha 的收益率数据，默认为 None。
        os_alpha_ids (dict[str, str] | None, optional): 其他 alpha 的标识符映射，默认为 None。
        alpha_result (dict | None, optional): 目标 alpha 的详细信息，默认为 None。
        return_alpha_pnls (bool, optional): 是否返回 alpha 的 PnL 数据，默认为 False。
        alpha_pnls (pd.DataFrame | None, optional): 目标 alpha 的 PnL 数据，默认为 None。
    Returns:
        float | tuple[float, pd.DataFrame]: 如果 `return_alpha_pnls` 为 False，返回最大自相关性值；
            如果 `return_alpha_pnls` 为 True，返回包含最大自相关性值和 alpha PnL 数据的元组。
    """
    try:
        if alpha_result is None:
            try:
                alpha_result = wait_get(f"https://api.worldquantbrain.com/alphas/{alpha_id}").json()
            except Exception as e:
                print(f"   ⚠️  [calc_self_corr] 获取alpha {alpha_id} 信息失败: {type(e).__name__} - {str(e)[:50]}")
                return 0.0 if not return_alpha_pnls else (0.0, pd.DataFrame())

        # 验证alpha_result数据结构
        if not isinstance(alpha_result, dict) or 'id' not in alpha_result:
            print(f"   ⚠️  [calc_self_corr] alpha_result无效: {type(alpha_result)}")
            return 0.0 if not return_alpha_pnls else (0.0, pd.DataFrame())

        if 'settings' not in alpha_result or 'region' not in alpha_result.get('settings', {}):
            print(f"   ⚠️  [calc_self_corr] alpha {alpha_id} 缺少settings.region")
            return 0.0 if not return_alpha_pnls else (0.0, pd.DataFrame())

        if alpha_pnls is not None:
            if len(alpha_pnls) == 0:
                alpha_pnls = None
        if alpha_pnls is None:
            try:
                _, alpha_pnls = get_alpha_pnls([alpha_result])
                if alpha_id not in alpha_pnls.columns:
                    print(f"   ⚠️  [calc_self_corr] PnL数据中找不到 {alpha_id}")
                    return 0.0 if not return_alpha_pnls else (0.0, pd.DataFrame())
                alpha_pnls = alpha_pnls[alpha_id]
            except Exception as e:
                print(f"   ⚠️  [calc_self_corr] 获取 {alpha_id} 的PnL数据失败: {type(e).__name__} - {str(e)[:50]}")
                return 0.0 if not return_alpha_pnls else (0.0, pd.DataFrame())

        alpha_rets = alpha_pnls - alpha_pnls.ffill().shift(1)
        alpha_rets = alpha_rets[
            pd.to_datetime(alpha_rets.index) > pd.to_datetime(alpha_rets.index).max() - pd.DateOffset(years=4)]

        # 获取当前区域的其他alpha收益率数据
        region = alpha_result['settings']['region']
        if region not in os_alpha_ids or len(os_alpha_ids[region]) == 0:
            print(f"   ⚠️  [calc_self_corr] 区域 {region} 没有可用的OS alpha数据")
            return 0.0 if not return_alpha_pnls else (0.0, alpha_pnls)

        region_os_rets = os_alpha_rets[os_alpha_ids[region]]

        # 过滤掉标准差为0或NaN的alpha（避免除以零警告）
        valid_cols = region_os_rets.columns[
            (region_os_rets.std() > 1e-10) & (region_os_rets.std().notna())
            ]

        # 检查目标alpha的标准差是否有效
        if len(alpha_rets.dropna()) > 0 and alpha_rets.std() > 1e-10:
            # 只计算与有效alpha的相关性
            if len(valid_cols) > 0:
                region_os_rets_valid = region_os_rets[valid_cols]

                # 使用警告上下文管理器抑制预期的除以零警告
                with warnings.catch_warnings():
                    warnings.filterwarnings('ignore', category=RuntimeWarning, message='invalid value encountered')
                    corr_results = region_os_rets_valid.corrwith(alpha_rets)
                    corr_results = corr_results.dropna()  # 移除NaN结果

                    if len(corr_results) > 0:
                        corr_results.sort_values(ascending=False).round(4).to_csv(
                            str(cfg.data_path / 'os_alpha_corr.csv'))
                        self_corr = corr_results.max()
                    else:
                        self_corr = 0
            else:
                self_corr = 0
        else:
            # 目标alpha标准差无效，无法计算相关性
            self_corr = 0

        if np.isnan(self_corr):
            self_corr = 0

    except KeyError as e:
        print(f"   ⚠️  [calc_self_corr] KeyError for {alpha_id}: {e}")
        return 0.0 if not return_alpha_pnls else (0.0, pd.DataFrame())
    except Exception as e:
        print(f"   ⚠️  [calc_self_corr] Error for {alpha_id}: {type(e).__name__} - {str(e)[:100]}")
        return 0.0 if not return_alpha_pnls else (0.0, pd.DataFrame())

    if return_alpha_pnls:
        return self_corr, alpha_pnls
    else:
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
    ppac_alpha_ids += [item['id'] for item in alphas for item_match in item['classifications'] if
                       item_match['name'] == 'Power Pool Alpha']

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
    os_alpha_rets = os_alpha_rets[
        pd.to_datetime(os_alpha_rets.index) > pd.to_datetime(os_alpha_rets.index).max() - pd.DateOffset(years=4)]
    return os_alpha_ids, os_alpha_rets


def get_simulation_result_json(s, alpha_id, session_manager=None):
    """
    获取alpha的模拟结果JSON，使用SessionManager统一管理登录
    """
    url = "https://api.worldquantbrain.com/alphas/" + alpha_id
    max_retries = 10
    retries = 0
    while retries < max_retries:
        while True:
            response = s.get(url)
            retry_after = response.headers.get("Retry-After", 0)
            if retry_after == 0:
                break
            time.sleep(float(retry_after))
        if response.status_code < 400:
            try:
                return response.json()
            except ValueError:
                print(f"   ⚠️  JSON解析失败 for {alpha_id}")
                return {}
        elif response.status_code == 401:
            # 使用SessionManager统一处理401错误
            if session_manager:
                new_session = session_manager.refresh_on_401()
                if new_session:
                    s.cookies.update(new_session.cookies)
                    print("   ✅ 重新登录成功，继续获取...")
                    continue  # 重试请求，不增加retries计数
                else:
                    print("   ❌ 重新登录失败，跳过此alpha")
                    return {}
            else:
                # 兼容旧代码，直接登录
                new_session = sign_in(cfg.username, cfg.password)
                if new_session:
                    s.cookies.update(new_session.cookies)
                    print("   ✅ 重新登录成功，继续获取...")
                    continue
                else:
                    print("   ❌ 重新登录失败，跳过此alpha")
                    return {}
        elif response.status_code == 429:
            # 专门处理429速率限制错误
            # 检查响应消息是否包含 rate limit exceeded
            try:
                response_text = response.text.lower()
                if "rate limit exceeded" in response_text or "api rate limit exceeded" in response_text:
                    print(f"   🔄 [429] 检测到API速率限制，重新登录...")
                    # 使用SessionManager刷新session
                    if session_manager:
                        new_session = session_manager.refresh_on_401()
                        if new_session:
                            s.cookies.update(new_session.cookies)
                            print("   ✅ 重新登录成功，继续获取...")
                        else:
                            print("   ❌ 重新登录失败，跳过此alpha")
                            return {}
                    else:
                        # 兼容旧代码，直接登录
                        new_session = sign_in(cfg.username, cfg.password)
                        if new_session:
                            s.cookies.update(new_session.cookies)
                            print("   ✅ 重新登录成功，继续获取...")
                        else:
                            print("   ❌ 重新登录失败，跳过此alpha")
                            return {}
            except:
                pass  # 如果解析响应失败，继续原有逻辑

            retry_after = response.headers.get("Retry-After", 60)  # 默认等待60秒
            wait_time = float(retry_after)
            print(f"   ⚠️  [429] API速率限制，等待 {wait_time:.1f} 秒后重试 ({retries + 1}/{max_retries})")
            time.sleep(wait_time)
            retries += 1
            continue  # 重试请求
        else:
            print(f"   ⚠️  Status {response.status_code} for {alpha_id}, retrying after {2 ** retries} seconds...")
            time.sleep(2 ** retries)
            retries += 1
    print(f"   ❌  Failed to get {alpha_id} after {max_retries} retries")
    return {}


def get_prod_corr(s, alpha_id):
    """
    Function gets alpha's prod correlation
    and save result to dataframe
    """

    while True:
        result = s.get(
            "https://api.worldquantbrain.com/alphas/" + alpha_id + "/correlations/prod"
        )
        if "retry-after" in result.headers:
            time.sleep(float(result.headers["Retry-After"]))
        else:
            break
    if result.json().get("records", 0) == 0:
        return pd.DataFrame()
    columns = [dct["name"] for dct in result.json()["schema"]["properties"]]
    prod_corr_df = pd.DataFrame(result.json()["records"], columns=columns).assign(alpha_id=alpha_id)

    return prod_corr_df


def set_alpha_properties(
        s,
        alpha_id,
        name: str = None,
        color: str = None,
        selection_desc: str = "311111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111",
        combo_desc: str = "322222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222",
        description: str = 'None',
        tags=['c1'],
):
    """
    Function changes alpha's description parameters
    """

    if tags is None:
        tags = ["c2"]
    params = {
        "color": color,
        "name": name,
        "tags": tags,
        "category": None,
        "regular": {"description": description},
        "combo": {"description": combo_desc},
        "selection": {"description": selection_desc},
    }

    max_retries = 5
    base_timeout = 600

    for attempt in range(max_retries):
        try:
            response = s.patch(
                "https://api.worldquantbrain.com/alphas/" + alpha_id,
                json=params,
                timeout=base_timeout,
            )

            # 处理 Retry-After / retry-after
            retry_after = response.headers.get("Retry-After") or response.headers.get("retry-after")
            if retry_after:
                wait_time = float(retry_after)
                print(f"   ⏳ 设置 {alpha_id} 属性被限流，等待 {wait_time:.1f} 秒后重试 ({attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue

            if response.status_code in (401, 403):
                print(f"   🔐 设置 {alpha_id} 属性认证失败，尝试重新登录... ({attempt + 1}/{max_retries})")
                # 尝试从全局session_manager获取新session
                if hasattr(cfg, 'session_manager') and cfg.session_manager:
                    new_session = cfg.session_manager.refresh_on_401()
                else:
                    new_session = sign_in(cfg.username, cfg.password)
                if new_session is None:
                    raise Exception("重新登录失败，无法继续设置属性")
                s.cookies = new_session.cookies
                continue

            if response.status_code >= 400:
                raise Exception(f"API错误 {response.status_code}: {response.text[:200]}")

            return response

        except requests.exceptions.Timeout:
            wait_time = 2 ** attempt
            print(f"   ⏰ 设置 {alpha_id} 属性超时，{wait_time} 秒后重试 ({attempt + 1}/{max_retries})")
            time.sleep(wait_time)
        except requests.exceptions.RequestException as e:
            wait_time = 2 ** attempt
            print(
                f"   ⚠️ 设置 {alpha_id} 属性网络异常: {str(e)[:80]}，{wait_time} 秒后重试 ({attempt + 1}/{max_retries})")
            time.sleep(wait_time)

    raise Exception(f"设置 {alpha_id} 属性失败，已重试 {max_retries} 次仍未成功")


def check_submission(alpha_bag, gold_bag, start, sess=None, c_d=None, s_d=None, all_yellow_alphas=None):
    depot = []
    overtime_alphas = []  # 记录超时的alpha
    # 使用SessionManager统一管理登录，避免重复登录
    if hasattr(cfg, 'session_manager') and cfg.session_manager:
        s = cfg.session_manager.get_session()
    else:
        s = sign_in(cfg.username, cfg.password)

    # 如果没有传入sess，使用s
    if sess is None:
        sess = s

    for idx, g in enumerate(alpha_bag):
        if idx < start:
            continue
        if idx % 5 == 0:
            print(idx)
        # 移除每200个alpha的定期重新登录，改为在401时统一处理
        # if idx % 200 == 0:
        #     s = sign_in(cfg.username, cfg.password)
        # print(idx)
        status, payload = get_check_submission(s, g)
        if status == "sleep":
            time.sleep(100)
            # 使用SessionManager刷新session
            if hasattr(cfg, 'session_manager') and cfg.session_manager:
                s = cfg.session_manager.refresh_on_401()
                sess = s
            else:
                s = sign_in(cfg.username, cfg.password)
                sess = s
            alpha_bag.append(g)
        elif status == "fail":
            continue
        elif status == "error":
            depot.append(g)
        elif status == "overtime":
            # 提交检查超时，记录到overtime_alphas列表
            overtime_alphas.append(g)
        elif status == "success":
            info = payload or {}
            pc_value = info.get("pc")
            if pc_value is not None and pd.isna(pc_value):
                print("check self-corrlation error")
                time.sleep(100)
                alpha_bag.append(g)
                continue
            gold_bag.append((g, info))
        else:
            print(f"   ⚠️ 未知状态 {status}，跳过 {g}")
            continue

    # 处理超时的alpha，标记为黄色并添加overtime标签
    if overtime_alphas and c_d and s_d:
        print(f"\n⏰ 开始标记超时的alpha为YELLOW (共 {len(overtime_alphas)} 个)...")
        overtime_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        for alpha_id in overtime_alphas:
            try:
                response = set_alpha_properties(sess, alpha_id,
                                                name=f"{overtime_time}_OVERTIME",
                                                description="提交检查超时（超过10分钟）",
                                                combo_desc=c_d,
                                                selection_desc=s_d,
                                                color='YELLOW',
                                                tags=['overtime'])
                if all_yellow_alphas is not None and alpha_id not in all_yellow_alphas:
                    all_yellow_alphas.append(alpha_id)
                print(f"   🟡 {alpha_id[:8]}... → YELLOW (overtime, 状态: {response.status_code})")
            except Exception as e:
                print(f"   ⚠️ 标记YELLOW失败 {alpha_id[:8]}...: {str(e)[:120]}")
        print(f"   ⏰ OVERTIME标记完成: {len(overtime_alphas)} 个alpha")

    # print('depot')
    # print(depot)
    return gold_bag


def get_check_submission(s, alpha_id, max_retries=3):
    """
    获取alpha的提交检查结果，包含重试逻辑

    Args:
        s: session对象
        alpha_id: alpha ID
        max_retries: 最大重试次数，默认3次

    Returns:
        Tuple[str, dict | None]: 第一个元素是状态（"sleep"/"fail"/"error"/"success"/"overtime"），第二个元素是success时的额外数据
    """
    check_start_time = time.time()
    max_check_time = 10 * 60  # 10分钟 = 600秒

    for attempt in range(max_retries):
        try:
            # 获取检查结果（带重试等待）
            while True:
                # 检查是否超过10分钟
                elapsed_time = time.time() - check_start_time
                if elapsed_time > max_check_time:
                    elapsed_minutes = elapsed_time / 60
                    print(f"   ⚠️  {alpha_id}: 提交检查超时（已等待 {elapsed_minutes:.1f} 分钟）")
                    return "overtime", None

                result = s.get("https://api.worldquantbrain.com/alphas/" + alpha_id + "/check")

                # 检查429状态码和rate limit exceeded消息
                if result.status_code == 429:
                    try:
                        response_text = result.text.lower()
                        if "rate limit exceeded" in response_text or "api rate limit exceeded" in response_text:
                            print(f"   🔄 [429] 检测到API速率限制，重新登录...")
                            # 尝试从全局session_manager获取新session
                            if hasattr(cfg, 'session_manager') and cfg.session_manager:
                                new_session = cfg.session_manager.refresh_on_401()
                                if new_session:
                                    s.cookies.update(new_session.cookies)
                                    print("   ✅ 重新登录成功，继续检查...")
                                else:
                                    print("   ❌ 重新登录失败")
                                    return "error", None
                            else:
                                new_session = sign_in(cfg.username, cfg.password)
                                if new_session:
                                    s.cookies.update(new_session.cookies)
                                    print("   ✅ 重新登录成功，继续检查...")
                                else:
                                    print("   ❌ 重新登录失败")
                                    return "error", None
                    except:
                        pass  # 如果解析响应失败，继续原有逻辑

                retry_after = result.headers.get("retry-after")

                if retry_after:
                    retry_after_float = float(retry_after)
                    # 检查等待后是否超过10分钟
                    if elapsed_time + retry_after_float > max_check_time:
                        elapsed_minutes = elapsed_time / 60
                        print(f"   ⚠️  {alpha_id}: 提交检查超时（已等待 {elapsed_minutes:.1f} 分钟）")
                        return "overtime", None
                    time.sleep(retry_after_float)
                elif result.status_code == 429:
                    # 429但没有retry-after头，等待默认时间
                    default_wait = 60
                    if elapsed_time + default_wait > max_check_time:
                        elapsed_minutes = elapsed_time / 60
                        print(f"   ⚠️  {alpha_id}: 提交检查超时（已等待 {elapsed_minutes:.1f} 分钟）")
                        return "overtime", None
                    print(f"   ⚠️  [429] API速率限制，等待 {default_wait} 秒...")
                    time.sleep(default_wait)
                else:
                    break

            # 检查是否登出
            if result.json().get("is", 0) == 0:
                print(f"   ⚠️  {alpha_id}: logged out")
                return "sleep", None

            # 解析检查结果
            checks_df = pd.DataFrame(
                result.json()["is"]["checks"]
            )

            # 获取PROD_CORRELATION值
            pc_rows = checks_df[checks_df.name == "PROD_CORRELATION"]
            if len(pc_rows) == 0:
                raise ValueError("PROD_CORRELATION field not found in checks")

            pc = pc_rows["value"].values[0]

            false_flag = False
            for field in ("result", "value"):
                if field not in checks_df.columns:
                    continue
                for cell in checks_df[field]:
                    if isinstance(cell, (bool, np.bool_)):
                        if cell is False:
                            false_flag = True
                            break
                    elif isinstance(cell, str) and "false" in cell.lower():
                        false_flag = True
                        break
                if false_flag:
                    break

            # 检查是否有FAIL结果
            if not any(checks_df["result"] == "FAIL"):
                if false_flag:
                    print(f"   🟡 {alpha_id}: PC={pc} 包含 False 检查项")
                else:
                    print(f"   ✅ {alpha_id}: PC={pc}")
                return "success", {"pc": pc, "has_false": false_flag}
            else:
                print(f"   ❌ {alpha_id}: 检查失败 (PC={pc})")
                return "fail", None

        except KeyError as e:
            # 数据结构错误
            print(f"   ⚠️  catch {alpha_id} (尝试 {attempt + 1}/{max_retries}): 字段缺失 {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # 指数退避：1秒、2秒、4秒
                print(f"   🔄 等待 {2 ** attempt} 秒后重试...")
            else:
                print(f"   ❌ {alpha_id}: 重试失败，返回error")
                return "error", None

        except ValueError as e:
            # PROD_CORRELATION字段不存在
            print(f"   ⚠️  catch {alpha_id} (尝试 {attempt + 1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                print(f"   🔄 等待 {2 ** attempt} 秒后重试...")
            else:
                print(f"   ❌ {alpha_id}: 重试失败，返回error")
                return "error", None

        except Exception as e:
            # 其他未知错误
            error_type = type(e).__name__
            print(f"   ⚠️  catch {alpha_id} (尝试 {attempt + 1}/{max_retries}): {error_type} - {str(e)[:50]}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                print(f"   🔄 等待 {2 ** attempt} 秒后重试...")
            else:
                print(f"   ❌ {alpha_id}: 重试失败，返回error")
                return "error", None

    # 理论上不会到这里
    return "error", None


def get_alphas_posit(start_date, end_date, sharpe_th, fitness_th, region, alpha_num):
    print(
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] get_alphas_posit开始处理地区 {region}，目标数量: {alpha_num}")
    # 使用SessionManager统一管理登录
    if hasattr(cfg, 'session_manager') and cfg.session_manager:
        s = cfg.session_manager.get_session()
    else:
        s = sign_in(cfg.username, cfg.password)
    output = []
    count = 0

    for i in range(0, alpha_num, 40):
        offset_start = time.time()
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 地区 {region} API请求，offset={i}")

        url_e = "https://api.worldquantbrain.com/users/self/alphas?limit=100&offset=%d" % (i) \
                + "&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E=2026-" + start_date \
                + "T00:00:00-04:00&dateCreated%3C2026-" + end_date \
                + "T00:00:00-04:00&is.fitness%3E" + str(fitness_th) + "&is.sharpe%3E" \
                + str(
            sharpe_th) + "&settings.region=" + region  + "&is.color!=YELLOW"+ "&order=-is.sharpe&hidden=false&type!=SUPER"
            # + "&is.color!=PURPLE"
        urls = [url_e]

        for url in urls:  # 修复缩进，确保这个循环正确执行
            req_start = time.time()
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 发送API请求到: {url[:80]}...")  # 只打印URL前80字符

            try:
                # 添加超时30秒，避免无限挂起；如果需要重试机制，可以用wait_get替换
                response = s.get(url, timeout=30)
                req_time = time.time() - req_start
                print(
                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] API响应状态: {response.status_code}，响应时间: {req_time:.2f}秒")

                if response.status_code != 200:
                    print(
                        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] API错误 {response.status_code}: {response.text[:200]}")  # 只打印前200字符错误信息
                    # 如果非200，尝试重登录（使用SessionManager）
                    if response.status_code in (401, 403):
                        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 尝试重新登录...")
                        if hasattr(cfg, 'session_manager') and cfg.session_manager:
                            s = cfg.session_manager.refresh_on_401()
                        else:
                            s = sign_in(cfg.username, cfg.password)
                    elif response.status_code == 429:
                        # 检查响应消息是否包含 rate limit exceeded
                        try:
                            response_text = response.text.lower()
                            if "rate limit exceeded" in response_text or "api rate limit exceeded" in response_text:
                                print(
                                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🔄 [429] 检测到API速率限制，重新登录...")
                                if hasattr(cfg, 'session_manager') and cfg.session_manager:
                                    s = cfg.session_manager.refresh_on_401()
                                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ 重新登录成功")
                                else:
                                    s = sign_in(cfg.username, cfg.password)
                                    if s:
                                        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ✅ 重新登录成功")
                        except:
                            pass  # 如果解析响应失败，继续原有逻辑
                    continue  # 重试这个请求

                # 检查Retry-After头，如果有等待时间
                retry_after = response.headers.get("Retry-After", 0)
                if int(retry_after) > 0:
                    wait_time = int(retry_after)
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] API要求等待 {wait_time} 秒...")
                    time.sleep(wait_time)

                alpha_list = response.json()["results"]
                offset_count = len(alpha_list)
                print(
                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] offset={i} 获取到 {offset_count} 个alpha，总计数: {count + offset_count}")

                for j in range(len(alpha_list)):
                    alpha_id = alpha_list[j]["id"]
                    name = alpha_list[j]["name"]
                    dateCreated = alpha_list[j]["dateCreated"]
                    sharpe = alpha_list[j]["is"]["sharpe"]
                    fitness = alpha_list[j]["is"]["fitness"]
                    turnover = alpha_list[j]["is"]["turnover"]
                    margin = alpha_list[j]["is"]["margin"]
                    longCount = alpha_list[j]["is"]["longCount"]
                    shortCount = alpha_list[j]["is"]["shortCount"]
                    decay = alpha_list[j]["settings"]["decay"]
                    exp = alpha_list[j]['regular']['code']
                    count += 1

                    if (longCount + shortCount) > 100:
                        if sharpe < -sharpe_th:
                            exp = "-%s" % exp
                        rec = [alpha_id, exp, sharpe, turnover, fitness, margin, dateCreated, decay]
                        print(
                            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 添加alpha {alpha_id} (Sharpe: {sharpe:.3f})")

                        if turnover > 0.7:
                            rec.append(decay * 4)
                        elif turnover > 0.6:
                            rec.append(decay * 3 + 3)
                        elif turnover > 0.5:
                            rec.append(decay * 3)
                        elif turnover > 0.4:
                            rec.append(decay * 2)
                        elif turnover > 0.35:
                            rec.append(decay + 4)
                        elif turnover > 0.3:
                            rec.append(decay + 2)
                        output.append(rec)

                offset_time = time.time() - offset_start
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] offset={i} 处理完成，耗时: {offset_time:.2f}秒")

            except requests.exceptions.Timeout:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] API请求超时 (30秒)，offset={i}，跳过")
                continue
            except requests.exceptions.RequestException as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] API请求异常: {e}，offset={i}")
                # 尝试重登录（使用SessionManager）
                try:
                    if hasattr(cfg, 'session_manager') and cfg.session_manager:
                        s = cfg.session_manager.refresh_on_401()
                    else:
                        s = sign_in(cfg.username, cfg.password)
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 重登录成功，继续")
                except Exception as login_e:
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 重登录失败: {login_e}，跳过此offset")
                continue
            except Exception as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] JSON解析或其他错误: {e}")
                # 使用SessionManager重登录
                if hasattr(cfg, 'session_manager') and cfg.session_manager:
                    s = cfg.session_manager.refresh_on_401()
                else:
                    s = sign_in(cfg.username, cfg.password)
                continue

    total_time = time.time() - offset_start  # 注意：这里offset_start是最后一个循环的，实际应从函数开始计算
    print(
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] get_alphas_posit for {region} 完成，总计数: {count}，输出: {len(output)}，总耗时约: {total_time:.2f}秒 (估算)")
    return output


class cfg:
    # 从当前目录下的 brain.txt 文件读取账号密码
    brain_file = os.path.join(os.path.dirname(__file__), 'brain.txt')
    
    # 检查文件是否存在
    if not os.path.exists(brain_file):
        raise FileNotFoundError(
            f"配置文件 {brain_file} 不存在！\n"
            f"请在该路径创建 brain.txt 文件，内容格式为 JSON 数组：\n"
            f'["email", "pass"]\n'
            f"用户名和密码用双引号包围，不要有额外空格或换行。\n"
            f"例如：[\"email\", \"pass\"]"
        )
    
    # 读取账号密码
    try:
        with open(brain_file, 'r', encoding='utf-8') as f:
            credentials = json.load(f)
        
        if not isinstance(credentials, list) or len(credentials) != 2:
            raise ValueError(
                f"brain.txt 文件格式错误！\n"
                f"应该是包含两个元素的 JSON 数组：[\"username\", \"password\"]"
            )
        
        username, password = credentials
    except json.JSONDecodeError as e:
        raise ValueError(
            f"brain.txt 文件 JSON 格式错误：{str(e)}\n"
            f"请确保文件内容是有效的 JSON 格式：[\"username\", \"password\"]"
        )
    except Exception as e:
        raise RuntimeError(f"读取 brain.txt 文件时出错：{str(e)}")
    
    data_path = Path('.')
    session_manager = None  # 全局SessionManager实例


def get_date_range_from_user():
    """
    获取用户自定义的日期范围
    Returns:
        tuple: (start_date, end_date, description) - 格式化的开始日期、结束日期和描述
    """
    print("\n" + "=" * 80)
    print("📅 请设置查询日期范围（设置后将持续使用此范围运行）")
    print("=" * 80)
    print("选择输入方式：")
    print("  1. 使用天数偏移（推荐 - 如：从5天前到明天）")
    print("  2. 使用具体日期（如：01-20 到 01-25）")
    print("  3. 使用默认设置（5天前到明天）")
    print("  4. 使用滚动窗口（每轮自动更新为最近N天）")
    print("\n💡 提示：选项1-3设置后固定不变，选项4每轮自动更新")

    choice = input("\n请选择 [1/2/3/4，默认3]: ").strip() or "3"

    today = datetime.now()

    if choice == "1":
        # 天数偏移方式
        print("\n输入天数偏移（负数表示过去，正数表示未来）：")
        try:
            start_days = int(input("  开始日期偏移天数（如：-5 表示5天前）[默认-5]: ").strip() or "-5")
            end_days = int(input("  结束日期偏移天数（如：1 表示明天）[默认1]: ").strip() or "1")

            start_date_obj = today + timedelta(days=start_days)
            end_date_obj = today + timedelta(days=end_days)

            start_date = start_date_obj.strftime("%m-%d")
            end_date = end_date_obj.strftime("%m-%d")

            desc = f"{abs(start_days)}天前到{abs(end_days)}天后 (固定)" if end_days > 0 else f"{abs(start_days)}天前到{abs(end_days)}天前 (固定)"
            if start_days == 0:
                desc = f"今天到{abs(end_days)}天后 (固定)" if end_days > 0 else f"今天到{abs(end_days)}天前 (固定)"

            print(f"\n✅ 设置成功: {start_date} 到 {end_date} ({desc})")
            return start_date, end_date, desc, False  # False表示不自动更新

        except ValueError:
            print("❌ 输入无效，使用默认设置")

    elif choice == "2":
        # 具体日期方式
        print("\n输入具体日期（格式：MM-DD，如：01-20）：")
        try:
            start_input = input("  开始日期 [默认5天前]: ").strip()
            end_input = input("  结束日期 [默认明天]: ").strip()

            if start_input and end_input:
                # 验证日期格式
                datetime.strptime(start_input, "%m-%d")
                datetime.strptime(end_input, "%m-%d")
                start_date = start_input
                end_date = end_input
                desc = f"{start_date} 到 {end_date} (固定)"
                print(f"\n✅ 设置成功: {desc}")
                return start_date, end_date, desc, False  # False表示不自动更新
            else:
                print("❌ 日期不完整，使用默认设置")
        except ValueError:
            print("❌ 日期格式错误，使用默认设置")

    elif choice == "4":
        # 滚动窗口方式
        print("\n设置滚动窗口（每轮自动更新）：")
        try:
            days_back = int(input("  查询最近多少天的数据？[默认7]: ").strip() or "7")
            if days_back < 1:
                print("❌ 天数必须大于0，使用默认7天")
                days_back = 7

            # 返回特殊标记，表示需要每轮更新
            desc = f"滚动窗口(最近{days_back}天)"
            print(f"\n✅ 设置成功: {desc} - 每轮自动更新日期范围")
            return None, None, desc, days_back  # days_back作为滚动窗口的天数

        except ValueError:
            print("❌ 输入无效，使用默认设置")

    # 默认设置（选项3或其他情况）
    five_days_ago = today - timedelta(days=5)
    tomorrow = today + timedelta(days=1)
    start_date = five_days_ago.strftime("%m-%d")
    end_date = tomorrow.strftime("%m-%d")
    desc = "5天前到明天 (固定)"
    print(f"\n✅ 使用默认设置: {start_date} 到 {end_date} ({desc})")
    return start_date, end_date, desc, False  # False表示不自动更新


# 初始化全局SessionManager，统一管理登录，避免重复登录
cfg.session_manager = SessionManager(cfg.username, cfg.password)
sess = cfg.session_manager.get_session()

# 在循环开始前获取日期范围设置
print("\n" + "🎯" * 40)
print("欢迎使用 Alpha 自动筛选和标记系统")
print("🎯" * 40)
start_date, end_date, date_desc, rolling_window = get_date_range_from_user()

# 无限循环处理所有地区
loop_count = 0
while True:
    loop_count += 1
    print("\n" + "=" * 80)
    print(f"🔄 开始第 {loop_count} 轮处理 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80 + "\n")

    # 确保sess使用最新的session（从SessionManager获取）
    sess = cfg.session_manager.get_session()
    print(f"   📊 [SessionManager] 当前登录次数: {cfg.session_manager.login_count}")

    # 每轮开始时更新数据
    download_data(flag_increment=True)

    # 如果是滚动窗口模式，每轮更新日期范围
    if rolling_window and isinstance(rolling_window, int):
        today = datetime.now()
        days_ago = today - timedelta(days=rolling_window)
        start_date = days_ago.strftime("%m-%d")
        end_date = today.strftime("%m-%d")
        print(f"📅 查询日期范围: {start_date} 到 {end_date} ({date_desc}) - 已自动更新\n")
    else:
        # 使用固定的日期范围
        print(f"📅 查询日期范围: {start_date} 到 {end_date} ({date_desc})\n")

    region_list = ['USA', 'ASI', 'EUR', 'GLB', 'CHN', 'JPN', 'AMR', 'IND']
    random.shuffle(region_list)
    region_summaries = {}
    for region in region_list:
        alpha_records = get_alphas_posit(start_date, end_date, 1, 0.5, region, 100)

        # 提取alpha ID（第一个元素）并去重保序
        alpha_ids = []
        for rec in alpha_records:
            alpha_id = rec[0]  # alpha_id是第一个元素
            if alpha_id not in alpha_ids:
                alpha_ids.append(alpha_id)

        print(f"地区 {region} 获取到 {len(alpha_ids)} 个唯一alpha")

        alpha_bag = []
        gold_bag = []
        prod_corr_dict = {}  # 存储每个alpha的生产相关性值
        all_yellow_alphas = []  # 跟踪所有被标记为YELLOW的alpha（包括筛选阶段和提交检查阶段）
        project_spec = "Idea: 111111111111111\n" + \
                       "Rationale for data used: 11111111111111\n" + \
                       "Rationale for operators used: 111111111111111"
        c_d = "1Short descriptions of your Selection Expression and Combo Expression are required to submit this SuperAlpha."
        s_d = "1Short descriptions of your Selection Expression and Combo Expression are required to submit this SuperAlpha."

        # 检查是否有fail
        for idx, alpha_id in enumerate(alpha_ids, 1):
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            try:
                # 添加请求延迟，避免触发429速率限制（每10个请求后延迟稍长）
                if idx > 1 and idx % 10 == 1:
                    time.sleep(2)  # 每10个请求后延迟2秒
                elif idx > 1:
                    time.sleep(0.5)  # 每个请求之间延迟0.5秒

                result_fail = get_simulation_result_json(sess, alpha_id, session_manager=cfg.session_manager)
                # 检查是否包含FAIL：只有当result_fail不为空且明确包含"FAIL"时才跳过
                # 空字典或None表示获取失败，不应该被误判为包含FAIL
                has_fail = False
                if result_fail:
                    result_str = str(result_fail).upper()
                    if "FAIL" in result_str:
                        has_fail = True

                # 如果result_fail为空，可能是获取失败，跳过但不说是"包含 FAIL"
                if not result_fail:
                    print(f"[{current_time}] [{idx}/{len(alpha_ids)}] alpha_id: {alpha_id} 获取模拟结果失败，跳过")
                    continue

                if not has_fail:
                    print(f"[{current_time}] [{idx}/{len(alpha_ids)}] alpha_id: {alpha_id} 不包含 FAIL，继续")
                    os_alpha_ids, os_alpha_rets = load_data()
                    self_corr = calc_self_corr(
                        alpha_id=alpha_id,
                        os_alpha_rets=os_alpha_rets,
                        os_alpha_ids=os_alpha_ids,
                    )
                    if self_corr < 0.7:
                        print(
                            f"[{current_time}] [{idx}/{len(alpha_ids)}] alpha_id: {alpha_id} 自相关性: {self_corr} 符合条件")
                        # 直接调用 API 获取生产相关性
                        try:
                            prod_corr_start_time = time.time()
                            max_wait_time = 10 * 60  # 10分钟 = 600秒
                            prod_corr_value = None
                            error_count = {}  # 记录不同状态码的错误次数
                            last_error_time = {}  # 记录上次输出错误的时间

                            prod_corr_timeout = False  # 标记生产相关性检查是否超时
                            while True:
                                # 检查是否超过10分钟
                                elapsed_time = time.time() - prod_corr_start_time
                                if elapsed_time > max_wait_time:
                                    elapsed_minutes = elapsed_time / 60
                                    print(
                                        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{idx}/{len(alpha_ids)}] alpha_id: {alpha_id} 检查生产相关性超时（已等待 {elapsed_minutes:.1f} 分钟），直接进入提交检查")
                                    prod_corr_timeout = True
                                    break

                                response = sess.get(
                                    "https://api.worldquantbrain.com/alphas/" + alpha_id + "/correlations/prod",
                                    timeout=5 * 60  # 5分钟 = 300秒
                                )

                                # 处理429速率限制错误
                                if response.status_code == 429:
                                    # 检查响应消息是否包含 rate limit exceeded
                                    try:
                                        response_text = response.text.lower()
                                        if "rate limit exceeded" in response_text or "api rate limit exceeded" in response_text:
                                            print(f"   🔄 [429] 检测到API速率限制，重新登录...")
                                            # 使用SessionManager刷新session
                                            if cfg.session_manager:
                                                new_session = cfg.session_manager.refresh_on_401()
                                                if new_session:
                                                    sess.cookies.update(new_session.cookies)
                                                    print("   ✅ 重新登录成功，继续获取生产相关性...")
                                                else:
                                                    print("   ❌ 重新登录失败，跳过此alpha")
                                                    prod_corr_timeout = True
                                                    break
                                            else:
                                                # 兼容旧代码，直接登录
                                                new_session = sign_in(cfg.username, cfg.password)
                                                if new_session:
                                                    sess.cookies.update(new_session.cookies)
                                                    print("   ✅ 重新登录成功，继续获取生产相关性...")
                                                else:
                                                    print("   ❌ 重新登录失败，跳过此alpha")
                                                    prod_corr_timeout = True
                                                    break
                                    except:
                                        pass  # 如果解析响应失败，继续原有逻辑

                                    retry_after = float(response.headers.get("Retry-After", 60))
                                    if elapsed_time + retry_after > max_wait_time:
                                        elapsed_minutes = elapsed_time / 60
                                        print(
                                            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{idx}/{len(alpha_ids)}] alpha_id: {alpha_id} 检查生产相关性超时（已等待 {elapsed_minutes:.1f} 分钟），直接进入提交检查")
                                        prod_corr_timeout = True
                                        break
                                    print(f"   ⚠️  [429] API速率限制，等待 {retry_after:.1f} 秒...")
                                    time.sleep(retry_after)
                                    continue  # 重试请求

                                if "retry-after" in response.headers:
                                    retry_after = float(response.headers["Retry-After"])
                                    # 检查等待时间加上已用时间是否超过10分钟
                                    if elapsed_time + retry_after > max_wait_time:
                                        elapsed_minutes = elapsed_time / 60
                                        print(
                                            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{idx}/{len(alpha_ids)}] alpha_id: {alpha_id} 检查生产相关性超时（已等待 {elapsed_minutes:.1f} 分钟），直接进入提交检查")
                                        prod_corr_timeout = True
                                        break
                                    time.sleep(retry_after)
                                elif response.status_code == 200:
                                    prod_corr_data = response.json()
                                    prod_corr_value = prod_corr_data.get('max', None)
                                    break
                                elif response.status_code == 412:
                                    # 412 Precondition Failed - 特殊处理
                                    status_code = response.status_code
                                    error_count[status_code] = error_count.get(status_code, 0) + 1
                                    count = error_count[status_code]

                                    # 检查是否有Retry-After头
                                    retry_after = response.headers.get("Retry-After")
                                    if retry_after:
                                        wait_time = float(retry_after)
                                    else:
                                        # 412错误通常需要等待更长时间，使用递增等待时间
                                        wait_time = min(30 + (count - 1) * 5, 120)  # 30秒起步，每次增加5秒，最多120秒

                                    # 检查等待时间加上已用时间是否超过10分钟
                                    if elapsed_time + wait_time > max_wait_time:
                                        elapsed_minutes = elapsed_time / 60
                                        print(
                                            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{idx}/{len(alpha_ids)}] alpha_id: {alpha_id} 检查生产相关性超时（已等待 {elapsed_minutes:.1f} 分钟），直接进入提交检查")
                                        prod_corr_timeout = True
                                        break

                                    # 只在特定次数输出（减少日志噪音）
                                    if count == 1 or count % 5 == 0:
                                        elapsed_minutes = elapsed_time / 60
                                        print(
                                            f"   ⚠️  [412] 前置条件不满足（已重试 {count} 次，已等待 {elapsed_minutes:.1f} 分钟），等待 {wait_time:.1f} 秒后重试...")

                                    time.sleep(wait_time)
                                    continue
                                else:
                                    # 其他错误状态码，等待后重试
                                    status_code = response.status_code
                                    error_count[status_code] = error_count.get(status_code, 0) + 1
                                    current_time = time.time()

                                    # 智能输出策略：减少重复输出
                                    count = error_count[status_code]
                                    # 定义输出里程碑：1, 10, 50, 100, 200, 500, 1000, 2000...
                                    milestones = [1, 10, 50, 100, 200, 500, 1000, 2000, 5000]
                                    is_milestone = count in milestones

                                    # 时间间隔：第一次后，每60秒输出一次（而不是30秒）
                                    time_since_last = (current_time - last_error_time.get(status_code,
                                                                                          0)) if status_code in last_error_time else float(
                                        'inf')
                                    time_threshold = 60 if count > 1 else 0  # 第一次立即输出，之后每60秒

                                    # 只在里程碑或时间间隔达到时输出
                                    should_print = (
                                            is_milestone or  # 达到里程碑次数
                                            (count == 1) or  # 第一次
                                            (time_since_last >= time_threshold and count > 1)  # 达到时间间隔且不是第一次
                                    )

                                    if should_print:
                                        if count > 1:
                                            print(
                                                f"   ⚠️  获取生产相关性返回状态码 {status_code}（已重试 {count} 次），等待3分钟后继续重试...")
                                        else:
                                            print(f"   ⚠️  获取生产相关性返回状态码 {status_code}，等待3分钟后重试...")
                                        last_error_time[status_code] = current_time

                                    time.sleep(3 * 60)  # 3分钟 = 180秒
                                    continue

                            # 如果生产相关性检查超时，直接进入提交检查
                            if prod_corr_timeout:
                                print(
                                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{idx}/{len(alpha_ids)}] alpha_id: {alpha_id} 生产相关性检查超时，直接进入提交检查")
                                alpha_bag.append(alpha_id)
                                prod_corr_dict[alpha_id] = None  # 标记为超时，未获取到生产相关性值
                            elif prod_corr_value is not None:
                                if float(prod_corr_value) < 0.7:
                                    print(
                                        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{idx}/{len(alpha_ids)}] alpha_id: {alpha_id} 生产相关性: {prod_corr_value} 符合条件")
                                    alpha_bag.append(alpha_id)
                                    prod_corr_dict[alpha_id] = prod_corr_value  # 保存生产相关性值
                                else:
                                    print(
                                        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{idx}/{len(alpha_ids)}] alpha_id: {alpha_id} 生产相关性: {prod_corr_value} 不符合条件")
                                    # 生产相关性 > 0.7，标记为黄色
                                    try:
                                        yellow_time = datetime.now().strftime("%Y%m%d_%H%M%S")
                                        yellow_tag = "PROD_CORR_HIGH"
                                        response = set_alpha_properties(sess,
                                                                        alpha_id,
                                                                        name=f"{yellow_time}_{yellow_tag}",
                                                                        description=f"生产相关性>0.7 ({prod_corr_value:.3f})",
                                                                        combo_desc=c_d,
                                                                        selection_desc=s_d,
                                                                        color='YELLOW',
                                                                        tags=[yellow_tag])
                                        if alpha_id not in all_yellow_alphas:
                                            all_yellow_alphas.append(alpha_id)
                                        print(
                                            f"   🟡 {alpha_id[:8]}... → YELLOW (生产相关性: {prod_corr_value:.3f}, 状态: {response.status_code})")
                                    except Exception as e:
                                        print(f"   ⚠️ 标记YELLOW失败 {alpha_id[:8]}...: {str(e)[:120]}")

                        except requests.exceptions.Timeout:
                            print(
                                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{idx}/{len(alpha_ids)}] alpha_id: {alpha_id} 获取生产相关性请求超时，跳过")
                        except Exception as e:
                            print(
                                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{idx}/{len(alpha_ids)}] alpha_id: {alpha_id} 获取生产相关性失败: {str(e)[:50]}")
                    else:
                        print(
                            f"[{current_time}] [{idx}/{len(alpha_ids)}] alpha_id: {alpha_id} 自相关性: {self_corr} 不符合条件")
                        try:
                            purple_time = datetime.now().strftime("%Y%m%d_%H%M%S")
                            purple_tag = "SELF_CORR_FAIL"
                            response = set_alpha_properties(sess,
                                                            alpha_id,
                                                            name=f"{purple_time}_{purple_tag}",
                                                            description="自相关性过高，暂不提交",
                                                            combo_desc=c_d,
                                                            selection_desc=s_d,
                                                            color='PURPLE',
                                                            tags=[purple_tag])
                            print(f"   🟣 {alpha_id[:8]}... → PURPLE (状态: {response.status_code})")
                        except Exception as e:
                            print(f"   ⚠️ 标记PURPLE失败 {alpha_id[:8]}...: {str(e)[:120]}")
                        continue
                else:
                    # has_fail为True，包含FAIL，跳过
                    print(f"[{current_time}] [{idx}/{len(alpha_ids)}] alpha_id: {alpha_id} 包含 FAIL，跳过")
                    try:
                        fail_mark_time = datetime.now().strftime("%Y%m%d_%H%M%S")
                        fail_tag = "FAIL_CHECK"
                        response = set_alpha_properties(
                            sess,
                            alpha_id,
                            name=f"{fail_mark_time}_{fail_tag}",
                            color='YELLOW',
                            description="包含 FAIL 检查项，暂时跳过",
                            selection_desc="包含 FAIL 检查项，未提交",
                            tags=[fail_tag])
                        if alpha_id not in all_yellow_alphas:
                            all_yellow_alphas.append(alpha_id)
                        print(f"   🟡 {alpha_id[:8]}... → YELLOW (状态: {response.status_code})")
                    except Exception as e:
                        print(f"   ⚠️ 标记YELLOW失败 {alpha_id[:8]}...: {str(e)[:120]}")
                    continue  # 包含FAIL，跳过后续处理

            except Exception as e:
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                print(
                    f"[{current_time}] [{idx}/{len(alpha_ids)}] ❌ 处理 alpha_id: {alpha_id} 时出错: {type(e).__name__} - {str(e)[:100]}")
                continue

        print("添加描述")
        for alpha_id in alpha_bag:
            set_alpha_properties(sess, alpha_id, description=project_spec)
        print("添加描述完成")

        print("提交检查")
        result = check_submission(alpha_bag, gold_bag, 0, sess=sess, c_d=c_d, s_d=s_d,
                                  all_yellow_alphas=all_yellow_alphas)
        print("提交检查完成")
        print(f"   📊 检查结果: {len(result)}/{len(alpha_bag)} 个alpha通过检查")

        # 汇总检测通过的alpha信息
        result_info = {}
        alpha_lis = []
        for alpha_id, info in result:
            alpha_lis.append(alpha_id)
            result_info[alpha_id] = info or {}
        alpha_lis = list(dict.fromkeys(alpha_lis))

        yellow_alphas = [alpha for alpha in alpha_lis if result_info.get(alpha, {}).get("has_false")]
        green_alphas = [alpha for alpha in alpha_lis if alpha not in yellow_alphas]

        # 分离通过和失败的alpha
        passed_alphas = set(alpha_lis)
        failed_alphas = [aid for aid in alpha_bag if aid not in passed_alphas]

        if failed_alphas:
            print(f"🔴 标记 {len(failed_alphas)} 个失败的alpha为RED...")
            current_time_name = datetime.now().strftime("%Y%m%d_%H%M%S")  # 在循环外生成时间戳
            red_success_count = 0
            red_fail_count = 0
            for alpha in failed_alphas:
                try:
                    response = set_alpha_properties(sess, alpha,
                                                    name=current_time_name,
                                                    description=project_spec,
                                                    combo_desc=c_d,
                                                    color='RED',
                                                    selection_desc=s_d,
                                                    tags=['SUBMISSION_FAIL'])  # 标记为提交检查失败
                    red_success_count += 1
                    print(f"   🔴 {alpha[:8]}... → RED (状态: {response.status_code})")
                except Exception as e:
                    red_fail_count += 1
                    error_msg = str(e)
                    print(f"   ❌ 标记RED失败 {alpha[:8]}...: {error_msg[:100]}")
                    # 如果是401或403，尝试重新登录（使用SessionManager）
                    if "401" in error_msg or "403" in error_msg:
                        print(f"   🔄 检测到认证错误，尝试重新登录...")
                        if cfg.session_manager:
                            sess = cfg.session_manager.refresh_on_401()
                        else:
                            sess = sign_in(cfg.username, cfg.password)
                    continue
            print(f"   🔴 RED标记完成: 成功 {red_success_count}/{len(failed_alphas)}，失败 {red_fail_count}")

        # 显示最终选中的alpha列表
        print(f"\n🌟 地区 {region} 最终选中的 Alpha 列表（共 {len(alpha_lis)} 个）:")
        for idx_alpha, alpha_id in enumerate(alpha_lis, 1):
            info = result_info.get(alpha_id, {})
            pc_value = info.get("pc")
            flag_note = " (含False)" if info.get("has_false") else ""
            print(f"   {idx_alpha:2d}. {alpha_id} (PC: {pc_value}){flag_note}")

        if yellow_alphas:
            print(f"\n🟡 开始标记YELLOW (包含 False 的 alpha)...")
            yellow_time_name = datetime.now().strftime("%Y%m%d_%H%M%S")
            yellow_success_count = 0
            yellow_fail_count = 0
            for alpha in yellow_alphas:
                try:
                    info = result_info.get(alpha, {})
                    pc_value = info.get("pc")
                    if pc_value is not None:
                        tag_name = f"PC{float(pc_value):.2f}"
                    else:
                        tag_name = "PC0.00"

                    prod_corr_value = prod_corr_dict.get(alpha, 0.0)
                    alpha_name = f"{yellow_time_name}_{prod_corr_value:.3f}"

                    response = set_alpha_properties(sess, alpha,
                                                    name=alpha_name,
                                                    description=project_spec,
                                                    combo_desc=c_d,
                                                    selection_desc=s_d,
                                                    color='YELLOW',
                                                    tags=[tag_name])
                    if alpha not in all_yellow_alphas:
                        all_yellow_alphas.append(alpha)
                    yellow_success_count += 1
                    if yellow_success_count <= 5:
                        print(
                            f"   🟡 {alpha[:8]}... → YELLOW | Name: {alpha_name} | Tag: {tag_name} (状态: {response.status_code})")

                except Exception as e:
                    yellow_fail_count += 1
                    error_msg = str(e)
                    print(f"   ❌ 标记YELLOW失败 {alpha[:8]}...: {error_msg[:100]}")
                    if "401" in error_msg or "403" in error_msg:
                        print(f"   🔄 检测到认证错误，尝试重新登录...")
                        if cfg.session_manager:
                            sess = cfg.session_manager.refresh_on_401()
                        else:
                            sess = sign_in(cfg.username, cfg.password)
                    continue
            print(f"   🟡 YELLOW标记完成: 成功 {yellow_success_count}/{len(yellow_alphas)}，失败 {yellow_fail_count}")

        # ✅ 标记为绿色 (通过检查的alpha)
        print(f"\n🟢 开始标记GREEN...")
        current_time_name = datetime.now().strftime("%Y%m%d_%H%M%S")  # 在循环外生成时间戳
        green_success_count = 0
        green_fail_count = 0
        for alpha in green_alphas:
            try:
                info = result_info.get(alpha, {})
                pc_value = info.get("pc")
                if pc_value is not None:
                    tag_name = f"PC{float(pc_value):.2f}"
                else:
                    tag_name = "PC0.00"

                prod_corr_value = prod_corr_dict.get(alpha, 0.0)
                alpha_name = f"{current_time_name}_{prod_corr_value:.3f}"

                response = set_alpha_properties(sess, alpha,
                                                name=alpha_name,
                                                description=project_spec,
                                                combo_desc=c_d,
                                                selection_desc=s_d,
                                                color='GREEN',  # ✅ 确保是GREEN
                                                tags=[tag_name])
                green_success_count += 1
                if green_success_count <= 5:
                    print(
                        f"   ✅ {alpha[:8]}... → GREEN | Name: {alpha_name} | Tag: {tag_name} (状态: {response.status_code})")
            except Exception as e:
                green_fail_count += 1
                error_msg = str(e)
                print(f"   ❌ 标记GREEN失败 {alpha[:8]}...: {error_msg[:100]}")
                if "401" in error_msg or "403" in error_msg:
                    print(f"   🔄 检测到认证错误，尝试重新登录...")
                    if cfg.session_manager:
                        sess = cfg.session_manager.refresh_on_401()
                    else:
                        sess = sign_in(cfg.username, cfg.password)
                continue

        print(f"   🟢 GREEN标记完成: 成功 {green_success_count}/{len(green_alphas)}，失败 {green_fail_count}")

        print(f"\n✅ 地区 {region} 完成: 通过 {len(alpha_lis)} 个，失败 {len(failed_alphas)} 个")
        region_summaries[region] = {
            "total_candidates": len(alpha_ids),
            "selected": len(alpha_lis),
            "alpha_bag": len(alpha_bag),
            "green": len(green_alphas),
            "yellow": len(all_yellow_alphas),  # 统计所有被标记为YELLOW的alpha（包括筛选阶段和提交检查阶段）
            "failed": len(failed_alphas),
        }
        print("=" * 60)

    # 一轮完成后的统计和等待
    print("\n" + "=" * 80)
    print("📊 本轮地区汇总：")
    for region, stats in region_summaries.items():
        print(
            f"   {region}: candidates={stats['total_candidates']}, selected={stats['selected']}, "
            f"green={stats['green']}, yellow={stats['yellow']}, failed={stats['failed']}")
    print(f"🎉 第 {loop_count} 轮所有地区处理完成！- {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    # 等待30分钟后开始下一轮（可根据需要调整）
    wait_minutes = 300
    print(f"\n⏰ 等待 {wait_minutes} 分钟后开始下一轮...")
    print(f"   下一轮预计开始时间: {(datetime.now() + timedelta(minutes=wait_minutes)).strftime('%Y-%m-%d %H:%M:%S')}")
    time.sleep(wait_minutes * 60)
