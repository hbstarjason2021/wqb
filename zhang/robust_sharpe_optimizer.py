import sys

import time

from datetime import datetime

import pandas as pd

import os

import threading

import concurrent.futures

import requests

from requests.exceptions import RequestException, ConnectionError

import json

import argparse

import queue

import re # Added for modify_alpha_expression




# --- Global Variables and Constants (from machine_lib.py and optimizeAlpha.py) ---

brain_api_url = os.environ.get("BRAIN_API_URL", "https://api.worldquantbrain.com")




UNIVERSE_DICTS = {

    "USA": ["TOP3000", "TOP1000", "TOP500", "TOP200", "ILLIQUID_MINVOL1M", "TOPSP500"],

    "GLB": ["TOP3000", "MINVOL1M","TOPDIV3000"],

    "EUR": ["TOP2500", "TOP1200", "TOP800", "TOP400", "ILLIQUID_MINVOL1M"],

    "ASI": ["MINVOL1M", "ILLIQUID_MINVOL1M"],

    "CHN": ["TOP2000U"],

    "AMR": ["TOP600"],

    "IND": ["TOP500"]

}




NEUT_DICTS = {

    'USA': ['REVERSION_AND_MOMENTUM','STATISTICAL','CROWDING', 'FAST', 'SLOW_AND_FAST'],

    'GLB': ['REVERSION_AND_MOMENTUM','STATISTICAL','CROWDING', 'FAST'],

    'EUR': ['REVERSION_AND_MOMENTUM','STATISTICAL','CROWDING', 'FAST', 'SLOW_AND_FAST'],

    'ASI': ['REVERSION_AND_MOMENTUM','STATISTICAL','CROWDING', 'FAST', 'SLOW_AND_FAST'],

    'CHN': ['REVERSION_AND_MOMENTUM','STATISTICAL','CROWDING', 'FAST', 'SLOW_AND_FAST'],

    'KOR': ['MARKET', 'SECTOR', 'INDUSTRY', 'SUBINDUSTRY'],

    'TWN': ['MARKET', 'SECTOR', 'INDUSTRY', 'SUBINDUSTRY'],

    'HKG': ['MARKET', 'SECTOR', 'INDUSTRY', 'SUBINDUSTRY'],

    'JPN': ['MARKET', 'SECTOR', 'INDUSTRY', 'SUBINDUSTRY'],

    'AMR': ['MARKET', 'SECTOR', 'INDUSTRY', 'SUBINDUSTRY', 'COUNTRY'],

    'IND': ['REVERSION_AND_MOMENTUM','CROWDING', 'FAST', 'MARKET', 'SECTOR', 'INDUSTRY', 'SUBINDUSTRY']

}




file_lock = threading.Lock()




# --- Utility Functions (from machine_lib.py and optimizeAlpha.py) ---




def login():

    # 从txt文件解密并读取数据

    # txt格式:

    # password: 'password'

    # username: 'username'

    def load_decrypted_data(txt_file='user_info.txt'):

        try:

            with open(txt_file, 'r') as f:

                data = f.read()

                data = data.strip().split('\n')




                data = {line.split(': ')[0]: line.split(': ')[1] for line in data}




            return data['username'][1:-1], data['password'][1:-1]

        except FileNotFoundError:

            print(f"Error: {txt_file} not found. Please create it with 'username: 'your_username'\npassword: 'your_password'.")

            sys.exit(1)

        except Exception as e:

            print(f"Error loading user info from {txt_file}: {e}")

            sys.exit(1)




    username, password = load_decrypted_data("user_info.txt")




    # Create a session to persistently store the headers

    s = requests.Session()




    # Save credentials into session

    s.auth = (username, password)




    # Send a POST request to the /authentication API

    try:

        response = s.post(f'{brain_api_url}/authentication')

        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

        print("Authentication successful.")

    except RequestException as e:

        print(f"Authentication failed: {e}")

        sys.exit(1)

    return s




def set_alpha_properties(

        s,

        alpha_id,

        name: str = None,

        color: str = None,

        selection_desc: str = None,

        combo_desc: str = None,

        tags: list = None,  # ['tag1', 'tag2']

):

    """

    Function changes alpha's description parameters (with 3 retries)

    """




    if alpha_id is None:

        print("Alpha ID 为空，无法进行属性更新。")

        return False

    max_retries = 3

    params = {

        "category": None,

        "regular": {"description": None},

    }

    if color:

        params["color"] = color

    if name:

        params["name"] = name

    if tags:

        params["tags"] = tags

    if combo_desc:

        params["combo"] = {"description": combo_desc}

    if selection_desc:

        params["selection"] = {"description": selection_desc}




    for retry in range(max_retries):

        try:

            response = s.patch(

                f"{brain_api_url}/alphas/{alpha_id}", json=params

            )

            # 检查响应状态码是否为成功（2xx）

            if 200 <= response.status_code < 300:

                print(f"成功设置 alpha_id: {alpha_id}, 标签: {tags if tags else '无'}（第 {retry + 1}/{max_retries} 次尝试）")

                return response  # 成功则返回响应

            else:

                if response.status_code == 429:

                    print(f"请求过多（429），尝试重新登录...（第 {retry + 1}/{max_retries} 次尝试）")

                    s=login()

                print(f"请求失败（状态码: {response.status_code}），alpha_id: {alpha_id}（第 {retry + 1}/{max_retries} 次尝试）")

        except Exception as e:

            print(f"请求异常: {str(e)}，alpha_id: {alpha_id}（第 {retry + 1}/{max_retries} 次尝试）")

        

        # 非最后一次重试时等待1秒

        if retry < max_retries - 1:

            time.sleep(1)




    # 所有重试均失败

    print(f"三次重试均失败，alpha_id: {alpha_id}")

    return None




def get_alpha_byid(s, alpha_id):

    # 为API请求添加超时，防止长时间阻塞

    request_timeout = 60 # 60秒超时

    while True:

        try:

            alpha = s.get(f"{brain_api_url}/alphas/{alpha_id}", timeout=request_timeout)

            if "retry-after" in alpha.headers:

                time.sleep(float(alpha.headers["Retry-After"]))

            else:

                alpha.raise_for_status() # 检查HTTP状态码

                break

        except requests.exceptions.Timeout:

            print(f"请求 alpha_id={alpha_id} 的 '/alphas' 接口超时。")

            time.sleep(5) # 短暂等待后重试

            s=login() # 尝试重新登录

        except requests.exceptions.RequestException as e:

            print(f"请求 alpha_id={alpha_id} 的 '/alphas' 接口发生错误: {e}")

            time.sleep(5) # 短暂等待后重试

            s=login() # 尝试重新登录

    string = alpha.content.decode('utf-8')

    metrics = json.loads(string)

    return metrics




def write_to_file(alpha, name):

    with file_lock:

        try:

            os.makedirs('records', exist_ok=True)

            # Ensure alpha is stripped of any leading/trailing whitespace, including newlines

            cleaned_alpha = alpha.strip()

            with open(f'records/{name}_simulated_alpha_expression.txt', mode='a') as f:

                f.write(cleaned_alpha + '\n')

                f.flush()

                print(f"Alpha expression written to file: {cleaned_alpha}")

        except Exception as e:

            print(f"写入文件时出错: {e}")




class SessionManager:

    def __init__(self, session, start_time, expiry_time):

        self.session = session

        self.start_time = start_time

        self.expiry_time = expiry_time

        self.lock = threading.Lock()  # 添加线程锁保护session刷新

        self.needupdate = False # Add this attribute for consistency




    def refresh_session(self):

        with self.lock:  # 使用线程锁保护session刷新过程

            print("Session expired, logging in again...")

            if self.session:

                self.session.close()

            self.session = login()  # 使用同步login函数

            self.start_time = time.time()

            self.needupdate = False # Reset after refresh




def locate_details(s, alpha_id):

    while True:

        alpha = s.get(f"{brain_api_url}/alphas/" + alpha_id)

        if "retry-after" in alpha.headers:

            time.sleep(float(alpha.headers["Retry-After"]))

        else:

            break

    string = alpha.content.decode('utf-8')

    metrics = json.loads(string)




    # 使用 get 方法安全获取数据

    is_data = metrics.get("is", {})

    sharpe = is_data.get("sharpe", 0.0)

    fitness = is_data.get("fitness", 0.0)

    turnover = is_data.get("turnover", 0.0)

    margin = is_data.get("margin", 0.0)

    

    settings = metrics.get("settings", {})

    decay = settings.get("decay", 0)

    delay = settings.get("delay", 0)

    exp = metrics.get('regular', {}).get('code', "")

    universe = settings.get("universe", "")

    truncation = settings.get("truncation", 0)

    neutralization = settings.get("neutralization", "")

    region = settings.get("region", "")

    maxTrade = settings.get("maxTrade", 0)

    

    # 安全获取 pyramids 数据

    matches_pyramid = next((check for check in is_data.get('checks', []) if check.get('name') == 'MATCHES_PYRAMID'), None)

    pyramids = [p.get('name', '') for p in matches_pyramid.get('pyramids', [])] if matches_pyramid else []




    # 查找 LOW_ROBUST_UNIVERSE_SHARPE

    robust_sharpe = 0.0

    robust_sharpe_check = next((check for check in is_data.get('checks', []) if check.get('name') == 'LOW_ROBUST_UNIVERSE_SHARPE'), None)

    if robust_sharpe_check:

        robust_sharpe = robust_sharpe_check.get('value', 0.0)

    

    triple = [alpha_id, sharpe, turnover, fitness, margin, exp, region, universe, neutralization, decay, delay, truncation, maxTrade, pyramids, robust_sharpe]

    return triple




def get_pnl(s, alpha_id):

    """

    Fetches the profit and loss (PnL) data for a given alpha ID by making requests to an API endpoint.

    The function handles retry logic for requests when a 'Retry-After' header

    is present in the response from the server.




    Parameters:

        s: requests.Session

            The session object used to make requests to the API.

        alpha_id: str

            The unique identifier of the alpha whose PnL data is to be fetched.




    Returns:

        requests.Response

            The API response containing PnL data.




    """

    while True:

        pnl = s.get(f'{brain_api_url}/alphas/{alpha_id}/recordsets/pnl')

        if pnl.headers.get('Retry-After', 0) == 0:

             break

        time.sleep(float(pnl.headers['Retry-After']))

    return pnl




def modify_alpha_expression(original_exp, modification_type, value):

    """

    根据指定的修改类型和值，智能地修改Alpha表达式字符串。

    """

    modified_exp = original_exp




    if modification_type == "time_backfill_ts":

        # 查找 ts_backfill(X, N) 并修改 N

        # 匹配 ts_backfill( 任意非逗号字符 , 任意数字 )

        match = re.search(r"ts_backfill\(([^,]+),\s*(\d+)\)", original_exp)

        if match:

            # 替换捕获组2（数字）为新的值

            modified_exp = re.sub(r"ts_backfill\(([^,]+),\s*(\d+)\)", fr"ts_backfill(\1, {value})", original_exp, 1)

        else:

            # 如果没有找到 ts_backfill，则尝试添加

            modified_exp = f"ts_backfill({original_exp}, {value})"

            

    elif modification_type == "time_backfill_group":

        # 查找 group_backfill(X, Y, N) 并修改 N

        # 匹配 group_backfill( 任意非逗号字符 , 任意非逗号字符 , 任意数字 )

        match = re.search(r"group_backfill\(([^,]+),\s*([^,]+),\s*(\d+)\)", original_exp)

        if match:

            # 替换捕获组3（数字）为新的值

            modified_exp = re.sub(r"group_backfill\(([^,]+),\s*([^,]+),\s*(\d+)\)", fr"group_backfill(\1, \2, {value})", original_exp, 1)

        else:

            # 如果没有找到 group_backfill，则尝试添加

            # 假设 group_backfill 需要一个 group 参数，这里默认使用 'sector'

            modified_exp = f"group_backfill({original_exp}, sector, {value})"




    elif modification_type == "add_winsorize":

        # 将 original_exp 用 winsorize(original_exp, std=value) 包装

        modified_exp = f"winsorize({original_exp}, std={value})"




    elif modification_type == "add_signed_power":

        # 将 original_exp 用 signed_power(original_exp, value) 包装

        modified_exp = f"signed_power({original_exp}, {value})"




    elif modification_type == "add_group_zscore":

        # 将 original_exp 用 group_zscore(original_exp, value) 包装

        # value 预期为 'sector' 或 'industry'

        modified_exp = f"group_zscore({original_exp}, {value})"




    elif modification_type == "winsorize_std":

        # 查找 winsorize(X, std=N) 并修改 N

        match = re.search(r"winsorize\(([^,]+),\s*std=(\d+)\)", original_exp)

        if match:

            modified_exp = re.sub(r"winsorize\(([^,]+),\s*std=(\d+)\)", fr"winsorize(\1, std={value})", original_exp, 1)

        # 如果没有找到 winsorize，则不进行修改，或者可以考虑添加，但这里选择不修改

        

    else:

        print(f"未知修改类型: {modification_type}")




    return modified_exp




def simulate_multis(session_manager, alphas, name, tags):

    """

    模拟多个alpha表达式对应的某个地区的信息

    """

    if session_manager.session is None:

        session_manager.refresh_session()

    if time.time() - session_manager.start_time > session_manager.expiry_time:

        session_manager.refresh_session()




    result_ids = []  # 用于存储alpha_id结果

    

    if len(alphas) >1:

        while True:

            try:

                resp = session_manager.session.post(f'{brain_api_url}/simulations',

                                                    json=alphas)

                simulation_progress_url = resp.headers.get('Location', 0)

                if simulation_progress_url == 0:

                    json_data = resp.json()

                    print(json_data)

                    if isinstance(json_data, list):

                        detail = json_data[0].get("detail", 0) if json_data else 0

                    else:

                        detail = json_data.get("detail", 0)

                    if 'SIMULATION_LIMIT_EXCEEDED' in detail:

                        print("Limited by the number of simulations allowed per time")

                        time.sleep(1)

                    else:

                        print("detail:", detail)

                        print("json_data:", json_data)

                        print("Alpha expression is duplicated")

                        time.sleep(1)

                        return result_ids

                else:

                    print('simulation_progress_url:', simulation_progress_url)

                    break

            except KeyError:

                print("Location key error during simulation request")

                time.sleep(60)

            except Exception as e:

                print("An error occurred1:", str(e))

                time.sleep(60)

        # 检查进度阶段超时控制（20分钟）

        get_start_time = time.time()

        while True:

            if time.time() - get_start_time > 1200:

                print(f"模拟进度检查超时(20分钟), alpha: {alphas}, progress_url: {simulation_progress_url}")

                return result_ids

            try:

                resps = session_manager.session.get(simulation_progress_url)

                json_data = resps.json()

                # 提前初始化children变量，确保所有路径都能访问

                children = json_data.get("children", [])

                # 获取响应头

                headers = resps.headers

                retry_after = headers.get('Retry-After', 0)

                if retry_after == 0:

                    status = json_data.get("status", 0)

                    if status == 'ERROR':

                        print(f"Error in simulation: {simulation_progress_url}")

                    elif status != "COMPLETE":

                        print(f"Simulation not complete: {simulation_progress_url}")

                        delete_resp = session_manager.session.delete(simulation_progress_url)                    

                        delete_json_data = delete_resp.json()

                        if delete_json_data.get("detail", 0) == "未找到。":

                            print("Successfully deleted: %s", simulation_progress_url)

                        else:

                            print("Failed to delete: %s", simulation_progress_url)

                    else:

                        print('Simulation completed: %s', simulation_progress_url)

                    break

                time.sleep(float(retry_after))

            except Exception as e:

                print(f"Progress check error: %s", str(e))

                time.sleep(30) 

        

        # 将for循环移到while循环之外

        for alpha, child in zip(alphas, children):

            try:

                child_str = str(child) # 新增：确保child是字符串

                child_progress = session_manager.session.get(f"{brain_api_url}/simulations/" + child_str)

                json_data = child_progress.json()

                alpha_id = json_data["alpha"]

                print("set_alpha_properties alpha_id: %s"%alpha_id)

                set_alpha_properties(session_manager.session,

                                    alpha_id,

                                    name="%s" % name,

                                    color=None,

                                    tags=tags)

                # 使用原始alpha数据生成唯一ID

                settings_str = json.dumps(alpha['settings'], sort_keys=True)  # 使用原始配置

                regular_str = alpha['regular']  # 使用原始配置

                unique_id = f"{regular_str}|{settings_str}"

                # 确保optimize目录存在

                os.makedirs('optimize', exist_ok=True)

                result_file_path = f'optimize/{name}_simulated_alpha_expression.txt'

                with open(result_file_path, mode='a') as f:

                    f.write(f"{alpha_id}|{unique_id}\n")

                # 将alpha_id添加到结果列表

                result_ids.append(alpha_id)

            except KeyError:

                print("Failed to retrieve alpha ID for: %s" % (f"{brain_api_url}/simulations/" + child))

                try:

                    # 关联原始alpha信息并获取错误状态和消息

                    settings_str = json.dumps(alpha['settings'], sort_keys=True)  # 使用原始配置

                    regular_str = alpha['regular']  # 使用原始配置

                    unique_id = f"{regular_str}|{settings_str}"

                    status = json_data.get("status")

                    if status == "ERROR":

                        error_msg = json_data.get("message", "No error message available")

                        error_str = f"ERROR_{error_msg}"

                        print("write error msg to file")

                        # 确保optimize目录存在

                        os.makedirs('optimize', exist_ok=True)

                        result_file_path = f'optimize/{name}_simulated_alpha_expression.txt'

                        with open(result_file_path, mode='a') as f:

                            f.write(f"{error_str}|{unique_id}\n")

                except Exception as e:

                    print("get error status :",str(e)) 

            except Exception as e:

                print("An error occurred while setting alpha properties:" + str(e))

        return result_ids  # 将return移至for循环外部

    else:

        result_ids = []

        simulation_data = alphas[0]

        while True:

            try:

                resp = session_manager.session.post(f'{brain_api_url}/simulations',

                                                    json=simulation_data)

                simulation_progress_url = resp.headers.get('Location', 0)

                if simulation_progress_url == 0:

                    json_data = resp.json()

                    if isinstance(json_data, list):

                        print(json_data)

                        detail = json_data[0].get("detail", 0) if json_data else 0

                    else:

                        detail = json_data.get("detail", 0)

                    if 'SIMULATION_LIMIT_EXCEEDED' in detail:

                        print("Limited by the number of simulations allowed per time")

                        time.sleep(1)

                    else:

                        print("detail:", detail)

                        print("json_data:", json_data)

                        print("Alpha expression is duplicated")

                        time.sleep(1)

                        return result_ids

                else:

                    print('simulation_progress_url:', simulation_progress_url)

                    break

            except KeyError:

                print("Location key error during simulation request")

                time.sleep(60)

            except Exception as e:

                print("An error occurred2:", str(e))

                time.sleep(60)




        # 检查进度阶段超时控制（20分钟）

        get_start_time = time.time()

        while True:

            if time.time() - get_start_time > 1200:

                print(f"模拟进度检查超时（20分钟），alpha: {simulation_data}, progress_url: {simulation_progress_url}")

                return result_ids

            try:

                resp = session_manager.session.get(simulation_progress_url)

                json_data = resp.json()

                # 获取响应头

                headers = resp.headers

                retry_after = headers.get('Retry-After', 0)

                if retry_after == 0:

                    print("response done: %s" % json_data)

                    break

                time.sleep(float(retry_after))

            except Exception as e:

                print("Error while checking progress:", str(e))

                time.sleep(60)




        print("%s done simulating, getting alpha details" % (simulation_progress_url))

        try:

            alpha_id = json_data.get("alpha")

            alpha = json_data.get("regular")

            print("set_alpha_properties alpha_id: %s"%alpha_id)

            # 假设 async_set_alpha_properties 有对应的同步版本

            set_alpha_properties(session_manager.session,

                                alpha_id,

                                name="%s" % name,

                                color=None,

                                tags=tags)




            

            settings_str = json.dumps(simulation_data['settings'], sort_keys=True)  # 改为使用原始配置

            regular_str = simulation_data['regular']  # 改为使用原始配置

            unique_id = f"{regular_str}|{settings_str}"

            # 确保optimize目录存在

            os.makedirs('optimize', exist_ok=True)

            result_file_path = f'optimize/{name}_simulated_alpha_expression.txt'

            with open(result_file_path, mode='a') as f:

                f.write(f"{alpha_id}|{unique_id}\n")

            result_ids.append(alpha_id)




        except KeyError:

            print("Failed to retrieve alpha ID for: %s" % simulation_progress_url)

            try:

                # 关联原始alpha信息并获取错误状态和消息

                settings_str = json.dumps(simulation_data['settings'], sort_keys=True)  # 使用原始配置

                regular_str = simulation_data['regular']  # 使用原始配置

                unique_id = f"{regular_str}|{settings_str}"

                status = json_data.get("status")

                if status == "ERROR":

                    error_msg = json_data.get("message", "No error message available")

                    error_str = f"ERROR_{error_msg}"

                    print("write error msg to file")

                    # 确保optimize目录存在

                    os.makedirs('optimize', exist_ok=True)

                    result_file_path = f'optimize/{name}_simulated_alpha_expression.txt'

                    with open(result_file_path, mode='a') as f:

                        f.write(f"{error_str}|{unique_id}\n")

            except Exception as e:

                print("get error status :",str(e))

        except Exception as e:

            print("An error occurred while setting alpha properties:", str(e))




        return result_ids  # 返回收集的alpha_id列表，每个ID出现两次




def simulate_multiple_alphas_with_retry(alpha_list, name="optimize_alpha", n_jobs=8, max_retries=5, is_neut=False):

    """

    包装simulate_multiple_alphas函数，使用队列方式提供自动重试功能

    当结果列表长度等于初始alpha_list长度或重试次数达到上限时退出

    """

    original_alpha_count = len(alpha_list)

    all_results = []

    retries = 0

    # 创建用于存储结果的文件路径

    result_file_path = f'optimize/{name}_simulated_alpha_expression.txt'

    os.makedirs('optimize', exist_ok=True)

    

    while retries < max_retries:

        # 从文件中读取已完成的alpha表达式

        completed_alphas = set()

        try:

            with open(result_file_path, mode='r') as f:

                for line in f:

                    completed_alphas.add(line.strip())

            print(f"从文件中读取到{len(completed_alphas)}个已完成的alpha")

        except FileNotFoundError:

            print(f"文件{result_file_path}不存在，创建新文件")

        

        # 过滤出尚未完成的alpha

        remaining_alphas = []

        for alpha in alpha_list:

            # 生成唯一标识

            settings_str = json.dumps(alpha['settings'], sort_keys=True)

            regular_str = alpha['regular']

            unique_id = f"{regular_str}|{settings_str}"

            

            # 检查是否已完成

            if not any(unique_id in line for line in completed_alphas):

                remaining_alphas.append((alpha, unique_id))

            

            # 收集已完成的结果

            for line in completed_alphas:

                if "|" in line and unique_id in line:

                    alpha_id = line.split("|")[0]

                    if "ERROR" not in alpha_id and alpha_id not in all_results:

                        all_results.append(alpha_id)

        

        # 如果所有alpha都已完成，提前退出

        if len(remaining_alphas)==0:

            print(f"所有{original_alpha_count}个alpha已完成，无需继续重试")

            return all_results

        

        # 如果达到最大重试次数，退出

        if retries >= max_retries:

            print(f"已达到最大重试次数 {max_retries}，停止重试")

            break

        

        # 如果所有alpha都已完成，提前退出

        if len(remaining_alphas) == 0:

            print(f"所有{original_alpha_count}个alpha已完成，无需继续重试")

            return all_results

        

        # 如果达到最大重试次数，退出

        if retries >= max_retries:

            print(f"已达到最大重试次数 {max_retries}，停止重试")

            break

        

        print(f"第 {retries + 1} 次尝试，开始处理{len(remaining_alphas)}/{original_alpha_count}个未完成的alpha")

        

        # 创建线程安全的任务队列

        task_queue = queue.Queue()

        # 将所有任务添加到队列

        for alpha, unique_id in remaining_alphas:

            task_queue.put((alpha, unique_id))

        

        # 登录并创建会话管理器

        session = login()

        session_start_time = time.time()

        session_expiry_time = 3 * 60 * 60  # 3小时

        session_manager = SessionManager(session, session_start_time, session_expiry_time)

        

        BATCH_SIZE = min(8, len(remaining_alphas)) - retries

        BATCH_SIZE = max(1, BATCH_SIZE) # 确保 BATCH_SIZE 至少为 1

        

        alpha_for_region_check = remaining_alphas[0][0] # 确保 remaining_alphas 已经检查不为空

        if alpha_for_region_check.get('settings').get('region') == "GLB":

            BATCH_SIZE = min(6, len(remaining_alphas)) - retries

            BATCH_SIZE = max(1, BATCH_SIZE) # 确保 GLB 区域的 BATCH_SIZE 也至少为 1

            if len(remaining_alphas) < 10 and BATCH_SIZE > 3:

                BATCH_SIZE = 3

        if is_neut:

            BATCH_SIZE = 1

        # 添加批次计数器

        total_batches = (len(remaining_alphas) + BATCH_SIZE - 1) // BATCH_SIZE

        completed_batches = 0

        processed_tasks = 0 

        batch_lock = threading.Lock()  # 用于保护批次计数器的锁




        # 工作线程函数

        def worker(worker_id):

            nonlocal completed_batches, total_batches, processed_tasks

            while not task_queue.empty():

                try:

                    batch = []

                    for _ in range(BATCH_SIZE):

                        try:

                            item = task_queue.get(timeout=1)

                            batch.append(item)

                        except queue.Empty:

                            break

        

                    if not batch:

                        break  # 队列为空，退出循环

        

                    # 处理批次任务

                    alphas_to_simulate = [item[0] for item in batch]

                    unique_ids_for_batch = [item[1] for item in batch]

            

                    # 调用模拟函数

                    result_ids = simulate_multis(session_manager, alphas_to_simulate, name, [name])

            

                    # 记录结果

                    if result_ids:

                        for i, alpha_id in enumerate(result_ids):

                            if i < len(unique_ids_for_batch):

                                unique_id = unique_ids_for_batch[i]

                                if alpha_id and "ERROR" not in alpha_id:

                                    with open(result_file_path, mode='a') as f:

                                        f.write(f"{alpha_id}|{unique_id}\n")

                                    if alpha_id not in all_results:

                                        all_results.append(alpha_id)

        

                    # 更新批次计数器并打印进度

                    with batch_lock:

                        completed_batches += 1

                        processed_tasks += len(batch)

                        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                        print(f"[{current_time}] 完成批次 {completed_batches}/{total_batches}，累计处理了 {processed_tasks}/{len(remaining_alphas)} 个任务")

                except Exception as e:

                    print(f"批次任务失败：错误={type(e).__name__}-{str(e)}")

                finally:

                    # 标记批次中所有任务为完成

                    if 'batch' in locals():

                        for _ in batch:

                            task_queue.task_done()

        

        # 创建线程池

        with concurrent.futures.ThreadPoolExecutor(max_workers=n_jobs) as executor:

            # 提交工作线程

            futures = [executor.submit(worker, i) for i in range(n_jobs)]

        

            # 等待所有任务完成

            task_queue.join()

        

            # 检查是否有未完成的future

            for future in concurrent.futures.as_completed(futures):

                try:

                    future.result()

                except Exception as e:

                    print(f"线程执行异常: {str(e)}")

        

        # 关闭会话

        try:

            if session_manager.session:

                session_manager.session.close()

        except Exception as e:

            print(f"关闭会话失败: {str(e)}")

        

        retries += 1

        

        print(f"第 {retries} 次尝试完成，当前成功获取{len(all_results)}/{original_alpha_count}个alpha结果")

    

    print(f"完成处理，成功获取{len(all_results)}/{original_alpha_count}个alpha结果")

    return all_results




def runRobustSharpe(s, details):

    # 1. 获取原始Alpha信息

    # [alpha_id, sharpe, turnover, fitness, margin, exp, region, universe, neutralization, decay, delay, truncation, maxTrade, pyramids, robust_sharpe]

    original_alpha_id, original_sharpe, _, _, _, original_exp, region, universe, original_neutralization, original_decay, delay, original_truncation, maxTrade, _, original_robust_sharpe = details




    print(f"🚀 开始Robust Sharpe优化: {original_alpha_id} - {original_exp}")

    print(f"📊 原始配置: region={region}, universe={universe}, delay={delay}, neutralization={original_neutralization}, decay={original_decay}, truncation={original_truncation}, robust_sharpe={original_robust_sharpe:.2f}, sharpe={original_sharpe:.2f}")




    # 存储所有中间结果，方便调试和最终筛选

    all_results = []




    # --- 阶段1: 中性化方法遍历与初步筛选 ---

    print("\n--- 阶段1: 中性化方法遍历 ---")

    neutralizations = NEUT_DICTS[region] # 获取该地区支持的中性化列表

    neut_alpha_configs = []

    for neut in neutralizations:

        config = {

            'type': 'REGULAR',

            'settings': {

                'instrumentType': 'EQUITY',

                'region': region,

                'universe': universe,

                'delay': delay,

                'decay': original_decay,

                'neutralization': neut,

                'truncation': original_truncation,

                'pasteurization': 'ON',

                'unitHandling': 'VERIFY',

                'nanHandling': 'ON',

                'language': 'FASTEXPR',

                'visualization': False,

                'testPeriod': "P0Y",

                'maxTrade': maxTrade

            },

            'regular': original_exp

        }

        neut_alpha_configs.append(config)




    print(f"👨‍💻 生成了 {len(neut_alpha_configs)} 个中性化配置进行模拟.")

    neut_result_ids = simulate_multiple_alphas_with_retry(neut_alpha_configs, name=f"robust_sharpe_optimized")




    detailed_neut_results = []

    for alpha_id in neut_result_ids:

        if alpha_id == "None":

            continue

        # [alpha_id, sharpe, turnover, fitness, margin, exp, region, universe, neutralization, decay, delay, truncation, maxTrade, pyramids, robust_sharpe]

        current_details = locate_details(s, alpha_id)

        current_sharpe = current_details[1]

        current_robust_sharpe = current_details[-1]

        current_neutralization = current_details[8]




        if current_sharpe > 1.2: # 初步筛选：alpha sharpe > 1.2

            detailed_neut_results.append({

                'alpha_id': alpha_id,

                'sharpe': current_sharpe,

                'robust_sharpe': current_robust_sharpe,

                'neutralization': current_neutralization,

                'decay': original_decay,

                'truncation': original_truncation,

                'exp': original_exp # 记录当前使用的表达式

            })

    

    # 严格选择前两个最佳中性化配置

    detailed_neut_results.sort(key=lambda x: x['robust_sharpe'], reverse=True)

    best_neut_configs = detailed_neut_results[:2]

    print(f"✅ 筛选出 {len(best_neut_configs)} 个最佳中性化配置.")

    for cfg in best_neut_configs:

        print(f"   - Neut: {cfg['neutralization']}, Robust Sharpe: {cfg['robust_sharpe']:.2f}, Sharpe: {cfg['sharpe']:.2f}")




    # --- 阶段2: Decay/Truncation参数遍历与进一步筛选 ---

    print("\n--- 阶段2: Decay/Truncation参数遍历 ---")

    best_base_configs = [] # 存储最终选出的最佳中性化、decay、truncation组合




    decay_options = [original_decay, 10, 30, 60] # 示例值，可调整

    truncation_options = [original_truncation, 0.01, 0.03, 0.05] # 示例值，可调整




    for neut_cfg in best_neut_configs:

        current_neutralization = neut_cfg['neutralization']

        decay_trunc_alpha_configs = []

        for decay_val in decay_options:

            for trunc_val in truncation_options:

                config = {

                    'type': 'REGULAR',

                    'settings': {

                        'instrumentType': 'EQUITY',

                        'region': region,

                        'universe': universe,

                        'delay': delay,

                        'decay': decay_val,

                        'neutralization': current_neutralization,

                        'truncation': trunc_val,

                        'pasteurization': 'ON',

                        'unitHandling': 'VERIFY',

                        'nanHandling': 'ON',

                        'language': 'FASTEXPR',

                        'visualization': False,

                        'testPeriod': "P0Y",

                        'maxTrade': maxTrade

                    },

                    'regular': original_exp

                }

                decay_trunc_alpha_configs.append(config)

        

        print(f"👨‍💻 为中性化 {current_neutralization} 生成了 {len(decay_trunc_alpha_configs)} 个Decay/Truncation配置进行模拟.")

        decay_trunc_result_ids = simulate_multiple_alphas_with_retry(decay_trunc_alpha_configs, name=f"robust_sharpe_optimized")




        detailed_decay_trunc_results = []

        for alpha_id in decay_trunc_result_ids:

            if alpha_id == "None":

                continue

            current_details = locate_details(s, alpha_id)

            current_sharpe = current_details[1]

            current_robust_sharpe = current_details[-1]

            current_decay = current_details[9]

            current_truncation = current_details[11]




            if current_sharpe > 1.2: # 进一步筛选：alpha sharpe > 1.2

                detailed_decay_trunc_results.append({

                    'alpha_id': alpha_id,

                    'sharpe': current_sharpe,

                    'robust_sharpe': current_robust_sharpe,

                    'neutralization': current_neutralization,

                    'decay': current_decay,

                    'truncation': current_truncation,

                    'exp': original_exp

                })

        

        # 严格选择前两个最佳Decay/Truncation组合

        detailed_decay_trunc_results.sort(key=lambda x: x['robust_sharpe'], reverse=True)

        best_base_configs.extend(detailed_decay_trunc_results[:2])

    

    print(f"✅ 筛选出 {len(best_base_configs)} 个最佳基础配置 (中性化+Decay+Truncation).")

    for cfg in best_base_configs:

        print(f"   - Neut: {cfg['neutralization']}, Decay: {cfg['decay']:.2f}, Trunc: {cfg['truncation']:.2f}, Robust Sharpe: {cfg['robust_sharpe']:.2f}, Sharpe: {cfg['sharpe']:.2f}")




    # --- 阶段3: 生成优化后的Alpha表达式变体 ---

    print("\n--- 阶段3: 生成优化后的Alpha表达式变体 ---")

    optimized_alpha_variants = []




    # 定义表达式修改的选项

    expression_modifications = [

        ("time_backfill_ts", 75), ("time_backfill_ts", 90),

        ("time_backfill_group", 180), ("time_backfill_group", 275),

        ("add_winsorize", 3),

        ("add_signed_power", 0.5),("add_signed_power", 1.5),("add_signed_power", 2),

        ("add_group_zscore", "sector"), # Assuming 'sector' as default group for zscore

        ("winsorize_std", 3), ("winsorize_std", 5) # Assuming original_std for winsorize was 4, offering alternatives

    ]




    for base_cfg in best_base_configs:

        current_exp = base_cfg['exp']

        current_neutralization = base_cfg['neutralization']

        current_decay = base_cfg['decay']

        current_truncation = base_cfg['truncation']




        # 原始表达式作为基准变体

        optimized_alpha_variants.append({

            'type': 'REGULAR',

            'settings': {

                'instrumentType': 'EQUITY',

                'region': region,

                'universe': universe,

                'delay': delay,

                'decay': current_decay,

                'neutralization': current_neutralization,

                'truncation': current_truncation,

                'pasteurization': 'ON',

                'unitHandling': 'VERIFY',

                'nanHandling': 'ON',

                'language': 'FASTEXPR',

                'visualization': False,

                'testPeriod': "P0Y",

                'maxTrade': maxTrade

            },

            'regular': current_exp

        })




        for mod_type, mod_val in expression_modifications:

            modified_exp = modify_alpha_expression(current_exp, mod_type, mod_val)

            if modified_exp != current_exp: # 确保表达式确实被修改了

                optimized_alpha_variants.append({

                    'type': 'REGULAR',

                    'settings': {

                        'instrumentType': 'EQUITY',

                        'region': region,

                        'universe': universe,

                        'delay': delay,

                        'decay': current_decay,

                        'neutralization': current_neutralization,

                        'truncation': current_truncation,

                        'pasteurization': 'ON',

                        'unitHandling': 'VERIFY',

                        'nanHandling': 'ON',

                        'language': 'FASTEXPR',

                        'visualization': False,

                        'testPeriod': "P0Y",

                        'maxTrade': maxTrade

                    },

                    'regular': modified_exp

                })

    

    print(f"👨‍💻 生成了 {len(optimized_alpha_variants)} 个优化后的Alpha表达式变体进行模拟.")

    optimized_result_ids = simulate_multiple_alphas_with_retry(optimized_alpha_variants, name=f"robust_sharpe_optimized")




    # --- 阶段4: 验证与结果返回 ---

    print("\n--- 阶段4: 验证与结果返回 ---")

    all_final_stage_alphas = [] # New list to store all results

    satisfied_count = 0




    for alpha_id in optimized_result_ids:

        if alpha_id == "None":

            continue

        

        current_details = locate_details(s, alpha_id)

        current_sharpe = current_details[1]

        current_robust_sharpe = current_details[-1]

        current_exp = current_details[5]

        current_neutralization = current_details[8]

        current_decay = current_details[9]

        current_truncation = current_details[11]




        # Get basecheck result (Pass/Fail)

        alpha_detail = get_alpha_byid(s, alpha_id)

        result_basecheck = 'Pass'

        if alpha_detail:

            checks = alpha_detail['is']['checks']

            # Check if any basic checks fail or error

            if any(check.get("result") == "FAIL" or check.get("result") == "ERROR" for check in checks):

                result_basecheck = 'Fail'

            # Additional check for "Weight is too strongly"

            if "Weight is too strongly" in str(checks):

                result_basecheck = 'Fail'




        is_satisfied = (current_robust_sharpe >= 1.0 and current_sharpe > 1.2)

        if is_satisfied:

            satisfied_count += 1




        all_final_stage_alphas.append({

            'alpha_id': alpha_id,

            'optimized_expression': current_exp,

            'neutralization': current_neutralization,

            'decay': current_decay,

            'truncation': current_truncation,

            'robust_sharpe': current_robust_sharpe,

            'sharpe': current_sharpe,

            'basecheck_result': result_basecheck, # Add basecheck result

            'is_satisfied': is_satisfied # Add satisfaction flag

        })

    

    print(f"🎉 成功优化出 {satisfied_count} 个Alpha满足条件.")

    print(f"总共处理了 {len(all_final_stage_alphas)} 个最终阶段的Alpha。")




    if all_final_stage_alphas:

        # Print details for satisfied alphas

        print("\n--- 满足条件的优化Alpha详情 ---")

        for alpha in all_final_stage_alphas:

            if alpha['is_satisfied']:

                print(f"   - Alpha ID: {alpha['alpha_id']}, Robust Sharpe: {alpha['robust_sharpe']:.2f}, Sharpe: {alpha['sharpe']:.2f}")

                print(f"     Expression: {alpha['optimized_expression']}")

                print(f"     Settings: Neut={alpha['neutralization']}, Decay={alpha['decay']:.2f}, Trunc={alpha['truncation']:.2f}")

                print(f"     Basecheck: {alpha['basecheck_result']}")

        

        # Save all results to CSV

        df_results = pd.DataFrame(all_final_stage_alphas)

        save_path = os.path.join("optimize", f"{original_alpha_id}_robust_sharpe_all_results.csv")

        df_results.to_csv(save_path, index=False)

        print(f"\n所有最终阶段的优化结果已保存至：{save_path}")

    else:

        print("未能找到任何最终阶段的优化Alpha。")




    return all_final_stage_alphas # Return all results




def main():

    parser = argparse.ArgumentParser(description='Optimize Alpha expressions for Robust Sharpe.')

    parser.add_argument('alpha_id', help='The Alpha ID to optimize.')

    

    args = parser.parse_args()

    

    s = login()

    details = locate_details(s, args.alpha_id)

    

    runRobustSharpe(s, details)




if __name__ == '__main__':

    main()
