import requests
from os import environ
from time import sleep
import time
import json
import pandas as pd
import random
import pickle
from urllib.parse import urljoin
from itertools import product
from itertools import combinations
from collections import defaultdict
import pickle
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===================== 全局频率控制配置 =====================
GLOBAL_REQUEST_DELAY = 1.0
MAX_RETRIES = 5
RETRY_BACKOFF_FACTOR = 2

# 创建带重试的Session
def create_retry_session():
    """创建带重试机制的Session，处理429/5xx错误"""
    session = requests.Session()
    
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "PATCH"]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    # 添加请求间隔
    original_send = session.send
    def send_with_delay(request, **kwargs):
        time.sleep(GLOBAL_REQUEST_DELAY)
        response = original_send(request, **kwargs)
        
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", RETRY_BACKOFF_FACTOR))
            print(f"⚠ API频率超限，服务器要求等待 {retry_after} 秒")
            time.sleep(retry_after)
            return original_send(request, **kwargs)
        return response
    
    session.send = send_with_delay
    return session

# 基础操作符定义
basic_ops = ["reverse", "inverse", "rank", "zscore", "quantile", "normalize"]
ts_ops = ["ts_rank", "ts_zscore", "ts_delta",  "ts_sum", "ts_delay", 
          "ts_std_dev", "ts_mean",  "ts_arg_min", "ts_arg_max","ts_scale", "ts_quantile"]
ops_set = basic_ops + ts_ops 

def login():
    """登录函数 - 带重试和限流处理"""
    # ========== 请替换为你的账号密码 ==========
    username = "your_username_here"
    password = "your_password_here"
    # ==========================================
    
    if not username or not password:
        print("❌ 错误：请先在login()函数中填写你的账号密码！")
        return None
    
    s = create_retry_session()
    s.auth = (username, password)
    
    for attempt in range(MAX_RETRIES):
        try:
            print(f"[登录尝试 {attempt+1}/{MAX_RETRIES}]")
            response = s.post('https://api.worldquantbrain.com/authentication')
            
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", RETRY_BACKOFF_FACTOR * (2 ** attempt)))
                print(f"⚠ 登录限流，等待 {retry_after} 秒...")
                time.sleep(retry_after)
                continue
            
            response.raise_for_status()
            
            if response.status_code == 201:
                print("✅ 登录成功！")
                print(f"   登录响应状态: {response.status_code}")
                time.sleep(GLOBAL_REQUEST_DELAY)
                test_response = s.get('https://api.worldquantbrain.com/users/self')
                if test_response.status_code == 200:
                    print("   API测试响应: 200（认证正常）")
                return s
            else:
                print(f"❌ 登录失败：状态码 {response.status_code}")
                time.sleep(RETRY_BACKOFF_FACTOR * (2 ** attempt))
                
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                retry_after = int(e.response.headers.get("Retry-After", RETRY_BACKOFF_FACTOR * (2 ** attempt)))
                print(f"❌ 登录失败（429）：{e}，等待 {retry_after} 秒")
                time.sleep(retry_after)
                continue
            elif e.response.status_code == 401:
                print(f"❌ 登录失败：用户名或密码错误！")
                return None
            else:
                print(f"❌ 登录失败：{e}")
                time.sleep(RETRY_BACKOFF_FACTOR * (2 ** attempt))
        except Exception as e:
            print(f"❌ 登录异常：{str(e)}")
            time.sleep(RETRY_BACKOFF_FACTOR * (2 ** attempt))
    
    print(f"❌ 登录失败：已尝试{MAX_RETRIES}次")
    return None

def get_datasets(s, instrument_type='EQUITY', region='USA', delay=1, universe='TOP3000'):
    """获取数据集"""
    try:
        time.sleep(GLOBAL_REQUEST_DELAY)
        url = f"https://api.worldquantbrain.com/data-sets?instrumentType={instrument_type}&region={region}&delay={delay}&universe={universe}"
        result = s.get(url)
        result.raise_for_status()
        
        data = result.json()
        if 'results' in data:
            datasets_df = pd.DataFrame(data['results'])
        elif isinstance(data, list):
            datasets_df = pd.DataFrame(data)
        else:
            datasets_df = pd.DataFrame()
            
        return datasets_df
    except Exception as e:
        print(f"❌ 获取数据集失败：{str(e)}")
        return pd.DataFrame()

def get_datafields(s, instrument_type='EQUITY', region='USA', delay=1, universe='TOP3000', dataset_id='', search=''):
    """获取数据字段"""
    try:
        if len(search) == 0:
            url_template = f"https://api.worldquantbrain.com/data-fields?instrumentType={instrument_type}&region={region}&delay={delay}&universe={universe}&dataset.id={dataset_id}&limit=50&offset={{x}}"
        else:
            url_template = f"https://api.worldquantbrain.com/data-fields?instrumentType={instrument_type}&region={region}&delay={delay}&universe={universe}&limit=50&search={search}&offset={{x}}"
        
        time.sleep(GLOBAL_REQUEST_DELAY)
        first_page = s.get(url_template.format(x=0))
        first_page.raise_for_status()
        first_data = first_page.json()
        
        print(f"[调试] get_datafields API返回结构：")
        print(f"   所有键: {list(first_data.keys()) if isinstance(first_data, dict) else '列表'}")
        print(f"   前100字符: {str(first_data)[:100]}...")
        
        if len(search) == 0:
            count = first_data.get('count', 0)
            if count == 0 and 'results' in first_data:
                count = len(first_data['results']) * 2
        else:
            count = 100
        
        datafields_list = []
        max_pages = 10
        for x in range(0, min(count, max_pages*50), 50):
            try:
                time.sleep(GLOBAL_REQUEST_DELAY)
                datafields = s.get(url_template.format(x=x))
                datafields.raise_for_status()
                page_data = datafields.json()
                
                if 'results' in page_data:
                    page_list = page_data['results']
                elif isinstance(page_data, list):
                    page_list = page_data
                else:
                    page_list = []
                
                if page_list:
                    datafields_list.append(page_list)
                    print(f"[调试] 获取第{x//50+1}页，共{len(page_list)}个字段")
                else:
                    break
                
            except Exception as e:
                print(f"⚠ 获取第{x//50+1}页失败：{str(e)}，跳过")
                continue
        
        datafields_list_flat = [item for sublist in datafields_list for item in sublist]
        print(f"[成功] 共获取到 {len(datafields_list_flat)} 个字段")
        
        return pd.DataFrame(datafields_list_flat)
        
    except Exception as e:
        print(f"❌ 获取数据字段失败：{str(e)}")
        return pd.DataFrame(columns=['id', 'type', 'description'])

def get_vec_fields(fields):
    """生成向量字段"""
    vec_ops = ["vec_avg", "vec_sum"]
    vec_fields = []
 
    for field in fields:
        for vec_op in vec_ops:
            if vec_op == "vec_choose":
                vec_fields.append("%s(%s, nth=-1)"%(vec_op, field))
                vec_fields.append("%s(%s, nth=0)"%(vec_op, field))
            else:
                vec_fields.append("%s(%s)"%(vec_op, field))
 
    return vec_fields

def process_datafields(df):
    """处理数据字段"""
    datafields = []
    datafields += df[df['type'] == "MATRIX"]["id"].tolist()
    datafields += get_vec_fields(df[df['type'] == "VECTOR"]["id"].tolist())
    return ["winsorize(ts_backfill(%s, 120), std=4)"%field for field in datafields]

def ts_factory(op, field):
    """时间序列工厂函数"""
    output = []
    days = [5, 22, 66, 120, 240]
    
    for day in days:
        alpha = "%s(%s, %d)"%(op, field, day)
        output.append(alpha)
    
    return output

def first_order_factory(fields, ops_set):
    """生成一阶表达式"""
    alpha_set = []
    for field in fields:
        alpha_set.append(field)
        for op in ops_set:
            if op == "ts_percentage":
                alpha_set += ts_comp_factory(op, field, "percentage", [0.5])
            elif op == "ts_decay_exp_window":
                alpha_set += ts_comp_factory(op, field, "factor", [0.5])
            elif op == "ts_moment":
                alpha_set += ts_comp_factory(op, field, "k", [2, 3, 4])
            elif op == "ts_entropy":
                alpha_set += ts_comp_factory(op, field, "buckets", [10])
            elif op.startswith("ts_") or op == "inst_tvr":
                alpha_set += ts_factory(op, field)
            elif op.startswith("vector"):
                alpha_set += vector_factory(op, field)
            elif op == "signed_power":
                alpha = "%s(%s, 2)"%(op, field)
                alpha_set.append(alpha)
            else:
                alpha = "%s(%s)"%(op, field)
                alpha_set.append(alpha)
 
    return alpha_set

def load_task_pool(alpha_list, limit_of_children_simulations, limit_of_multi_simulations):
    """加载任务池"""
    tasks = [alpha_list[i:i + limit_of_children_simulations] for i in range(0, len(alpha_list), limit_of_children_simulations)]
    pools = [tasks[i:i + limit_of_multi_simulations] for i in range(0, len(tasks), limit_of_multi_simulations)]
    return pools

def generate_sim_data(alpha_list, region, uni, neut):
    """旧版生成模拟数据（保留兼容）"""
    sim_data_list = []
    for alpha, decay in alpha_list:
        simulation_data = {
            'type': 'REGULAR',
            'settings': {
                'instrumentType': 'EQUITY',
                'region': region,
                'universe': uni,
                'delay': 1,
                'decay': decay,
                'neutralization': neut,
                'truncation': 0.08,
                'pasteurization': 'ON',
                'testPeriod': 'P0Y',
                'unitHandling': 'VERIFY',
                'nanHandling': 'ON',
                'language': 'FASTEXPR',
                'visualization': False,
            },
            'regular': alpha}

        sim_data_list.append(simulation_data)
    return sim_data_list

def set_alpha_properties(s, alpha_id, name=None, color=None, selection_desc="None", combo_desc="None", tags=["ace_tag"]):
    """设置Alpha属性"""
    try:
        time.sleep(GLOBAL_REQUEST_DELAY)
        params = {
            "color": color,
            "name": name,
            "tags": tags,
            "category": None,
            "regular": {"description": None},
            "combo": {"description": combo_desc},
            "selection": {"description": selection_desc},
        }
        response = s.patch(f"https://api.worldquantbrain.com/alphas/{alpha_id}", json=params)
        response.raise_for_status()
    except Exception as e:
        print(f"❌ 设置Alpha属性失败: {e}")

def get_alphas(start_date, end_date, sharpe_th, fitness_th, region, alpha_num, usage):
    """获取Alpha列表"""
    s = login()
    output = []
    count = 0
    for i in range(0, alpha_num, 100):
        print(i)
        url_e = f"https://api.worldquantbrain.com/users/self/alphas?limit=100&offset={i}&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E=2025-{start_date}T00:00:00-04:00&dateCreated%3C2025-{end_date}T00:00:00-04:00&is.fitness%3E{fitness_th}&is.sharpe%3E{sharpe_th}&settings.region={region}&order=-is.sharpe&hidden=false&type!=SUPER"
        url_c = f"https://api.worldquantbrain.com/users/self/alphas?limit=100&offset={i}&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E=2025-{start_date}T00:00:00-04:00&dateCreated%3C2025-{end_date}T00:00:00-04:00&is.fitness%3C-{fitness_th}&is.sharpe%3C-{sharpe_th}&settings.region={region}&order=is.sharpe&hidden=false&type!=SUPER"
        urls = [url_e]
        if usage != "submit":
            urls.append(url_c)
        for url in urls:
            try:
                time.sleep(GLOBAL_REQUEST_DELAY)
                response = s.get(url)
                response.raise_for_status()
                alpha_list = response.json().get("results", [])
                
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
                            exp = "-%s"%exp
                        rec = [alpha_id, exp, sharpe, turnover, fitness, margin, dateCreated, decay]
                        print(rec)
                        if turnover > 0.7:
                            rec.append(decay*4)
                        elif turnover > 0.6:
                            rec.append(decay*3+3)
                        elif turnover > 0.5:
                            rec.append(decay*3)
                        elif turnover > 0.4:
                            rec.append(decay*2)
                        elif turnover > 0.35:
                            rec.append(decay+4)
                        elif turnover > 0.3:
                            rec.append(decay+2)
                        output.append(rec)
            except Exception as e:
                print(f"{i} finished re-login: {e}")
                time.sleep(60)
                s = login()

    print(f"count: {count}")
    return output

def prune(next_alpha_recs, prefix, keep_num):
    """修剪Alpha列表"""
    output = []
    num_dict = defaultdict(int)
    for rec in next_alpha_recs:
        exp = rec[1]
        field = exp.split(prefix)[-1].split(",")[0]
        sharpe = rec[2]
        if sharpe < 0:
            field = "-%s"%field
        if num_dict[field] < keep_num:
            num_dict[field] += 1
            decay = rec[-1]
            exp = rec[1]
            output.append([exp,decay])
    return output

def get_group_second_order_factory(first_order, group_ops, region):
    """生成分组二阶表达式"""
    second_order = []
    for fo in first_order:
        for group_op in group_ops:
            second_order += group_factory(group_op, fo, region)
    return second_order

def group_factory(op, field, region):
    """分组工厂函数"""
    output = []
    vectors = ["cap"] 
    
    chn_group_13 = ['pv13_h_min2_sector', 'pv13_di_6l', 'pv13_rcsed_6l', 'pv13_di_5l', 'pv13_di_4l', 
                    'pv13_di_3l', 'pv13_di_2l', 'pv13_di_1l', 'pv13_parent', 'pv13_level']
    chn_group_1 = ['sta1_top3000c30','sta1_top3000c20','sta1_top3000c10','sta1_top3000c2','sta1_top3000c5']
    chn_group_2 = ['sta2_top3000_fact4_c10','sta2_top2000_fact4_c50','sta2_top3000_fact3_c20']
    hkg_group_13 = ['pv13_10_f3_g2_minvol_1m_sector', 'pv13_10_minvol_1m_sector', 'pv13_20_minvol_1m_sector', 
                    'pv13_2_minvol_1m_sector', 'pv13_5_minvol_1m_sector', 'pv13_1l_scibr', 'pv13_3l_scibr',
                    'pv13_2l_scibr', 'pv13_4l_scibr', 'pv13_5l_scibr']
    hkg_group_1 = ['sta1_allc50','sta1_allc5','sta1_allxjp_513_c20','sta1_top2000xjp_513_c5']
    hkg_group_2 = ['sta2_all_xjp_513_all_fact4_c10','sta2_top2000_xjp_513_top2000_fact3_c10',
                   'sta2_allfactor_xjp_513_13','sta2_top2000_xjp_513_top2000_fact3_c20']
    twn_group_13 = ['pv13_2_minvol_1m_sector','pv13_20_minvol_1m_sector','pv13_10_minvol_1m_sector',
                    'pv13_5_minvol_1m_sector','pv13_10_f3_g2_minvol_1m_sector','pv13_5_f3_g2_minvol_1m_sector',
                    'pv13_2_f4_g3_minvol_1m_sector']
    twn_group_1 = ['sta1_allc50','sta1_allxjp_513_c50','sta1_allxjp_513_c20','sta1_allxjp_513_c2',
                   'sta1_allc20','sta1_allxjp_513_c5','sta1_allxjp_513_c10','sta1_allc2','sta1_allc5']
    twn_group_2 = ['sta2_allfactor_xjp_513_0','sta2_all_xjp_513_all_fact3_c20',
                   'sta2_all_xjp_513_all_fact4_c20','sta2_all_xjp_513_all_fact4_c50']
    usa_group_13 = ['pv13_h_min2_3000_sector','pv13_r2_min20_3000_sector','pv13_r2_min2_3000_sector',
                    'pv13_r2_min2_3000_sector', 'pv13_h_min2_focused_pureplay_3000_sector']
    usa_group_1 = ['sta1_top3000c50','sta1_allc20','sta1_allc10','sta1_top3000c20','sta1_allc5']
    usa_group_2 = ['sta2_top3000_fact3_c50','sta2_top3000_fact4_c20','sta2_top3000_fact4_c10']
    usa_group_6 = ['mdl10_group_name']
    asi_group_13 = ['pv13_20_minvol_1m_sector', 'pv13_5_f3_g2_minvol_1m_sector', 'pv13_10_f3_g2_minvol_1m_sector',
                    'pv13_2_f4_g3_minvol_1m_sector', 'pv13_10_minvol_1m_sector', 'pv13_5_minvol_1m_sector']
    asi_group_1 = ['sta1_allc50', 'sta1_allc10', 'sta1_minvol1mc50','sta1_minvol1mc20',
                   'sta1_minvol1m_normc20', 'sta1_minvol1m_normc50']
    jpn_group_1 = ['sta1_alljpn_513_c5', 'sta1_alljpn_513_c50', 'sta1_alljpn_513_c2', 'sta1_alljpn_513_c20']
    jpn_group_2 = ['sta2_top2000_jpn_513_top2000_fact3_c20', 'sta2_all_jpn_513_all_fact1_c5',
                   'sta2_allfactor_jpn_513_9', 'sta2_all_jpn_513_all_fact1_c10']
    jpn_group_13 = ['pv13_2_minvol_1m_sector', 'pv13_2_f4_g3_minvol_1m_sector', 'pv13_10_minvol_1m_sector',
                    'pv13_10_f3_g2_minvol_1m_sector', 'pv13_all_delay_1_parent', 'pv13_all_delay_1_level']
    kor_group_13 = ['pv13_10_f3_g2_minvol_1m_sector', 'pv13_5_minvol_1m_sector', 'pv13_5_f3_g2_minvol_1m_sector',
                    'pv13_2_minvol_1m_sector', 'pv13_20_minvol_1m_sector', 'pv13_2_f4_g3_minvol_1m_sector']
    kor_group_1 = ['sta1_allc20','sta1_allc50','sta1_allc2','sta1_allc10','sta1_minvol1mc50',
                   'sta1_allxjp_513_c10', 'sta1_top2000xjp_513_c50']
    kor_group_2 =['sta2_all_xjp_513_all_fact1_c50','sta2_top2000xjp_513_top2000_fact2_c50',
                  'sta2_all_xjp_513_all_fact4_c50','sta2_all_xjp_513_all_fact4_c5']
    eur_group_13 = ['pv13_5_sector', 'pv13_2_sector', 'pv13_v3_3l_scibr', 'pv13_v3_2l_scibr', 'pv13_2l_scibr',
                    'pv13_52_sector', 'pv13_v3_6l_scibr', 'pv13_v3_4l_scibr', 'pv13_v3_1l_scibr']
    eur_group_1 = ['sta1_allc10', 'sta1_allc2', 'sta1_top1200c2', 'sta1_allc20', 'sta1_top1200c10']
    eur_group_2 = ['sta2_top1200_fact3_c50','sta2_top1200_fact3_c20','sta2_top1200_fact4_c50']
    glb_group_13 = ["pv13_10_f2_g3_sector", "pv13_2_f3_g2_sector", "pv13_2_sector", "pv13_52_all_delay_1_sector"]
    glb_group_1 = ['sta1_allc20', 'sta1_allc10', 'sta1_allc50', 'sta1_allc5']
    glb_group_2 = ['sta2_all_fact4_c50', 'sta2_all_fact4_c20', 'sta2_all_fact3_c20', 'sta2_all_fact4_c10']
    glb_group_13 = ['pv13_2_sector', 'pv13_10_sector', 'pv13_3l_scibr', 'pv13_2l_scibr', 'pv13_1l_scibr',
                    'pv13_52_minvol_1m_all_delay_1_sector','pv13_52_minvol_1m_sector','pv13_52_minvol_1m_sector'] 
    amr_group_13 = ['pv13_4l_scibr', 'pv13_1l_scibr', 'pv13_hierarchy_min51_f1_sector',
                    'pv13_hierarchy_min2_600_sector', 'pv13_r2_min2_sector', 'pv13_h_min20_600_sector']
    
    cap_group = "bucket(rank(cap), range='0.1, 1, 0.1')"
    asset_group = "bucket(rank(assets),range='0.1, 1, 0.1')"
    sector_cap_group = "bucket(group_rank(cap, sector),range='0.1, 1, 0.1')"
    sector_asset_group = "bucket(group_rank(assets, sector),range='0.1, 1, 0.1')"
    vol_group = "bucket(rank(ts_std_dev(returns,20)),range = '0.1, 1, 0.1')"
    liquidity_group = "bucket(rank(close*volume),range = '0.1, 1, 0.1')"

    groups = ["market","sector", "industry", "subindustry",
            cap_group, asset_group, sector_cap_group, sector_asset_group, vol_group, liquidity_group]

    if region == "CHN":
        groups += chn_group_13 + chn_group_1 + chn_group_2  
    if region == "TWN":
        groups += twn_group_13 + twn_group_1 + twn_group_2 
    if region == "ASI":
        groups += asi_group_13 + asi_group_1 
    if region == "USA":
        groups += usa_group_13 + usa_group_1 + usa_group_2  
    if region == "HKG":
        groups += hkg_group_13 + hkg_group_1 + hkg_group_2 
    if region == "KOR":
        groups += kor_group_13 + kor_group_1 + kor_group_2 
    if region == "EUR": 
        groups += eur_group_13 + eur_group_1 + eur_group_2 
    if region == "GLB":
        groups += glb_group_13 + glb_group_1 + glb_group_2
    if region == "AMR":
        groups += amr_group_13 
    if region == "JPN":
        groups += jpn_group_1 + jpn_group_2 + jpn_group_13 
        
    for group in groups:
        if op.startswith("group_vector"):
            for vector in vectors:
                alpha = "%s(%s,%s,densify(%s))"%(op, field, vector, group)
                output.append(alpha)
        elif op.startswith("group_percentage"):
            alpha = "%s(%s,densify(%s),percentage=0.5)"%(op, field, group)
            output.append(alpha)
        else:
            alpha = "%s(%s,densify(%s))"%(op, field, group)
            output.append(alpha)
        
    return output

def trade_when_factory(op,field,region):
    """交易时机工厂函数"""
    output = []
    open_events = ["ts_arg_max(volume, 5) == 0", "ts_corr(close, volume, 20) < 0",
                   "ts_corr(close, volume, 5) < 0", "ts_mean(volume,10)>ts_mean(volume,60)",
                   "group_rank(ts_std_dev(returns,60), sector) > 0.7", "ts_zscore(returns,60) > 2",
                   "ts_arg_min(volume, 5) > 3",
                   "ts_std_dev(returns, 5) > ts_std_dev(returns, 20)",
                   "ts_arg_max(close, 5) == 0", "ts_arg_max(close, 20) == 0",
                   "ts_corr(close, volume, 5) > 0", "ts_corr(close, volume, 5) > 0.3", "ts_corr(close, volume, 5) > 0.5",
                   "ts_corr(close, volume, 20) > 0", "ts_corr(close, volume, 20) > 0.3", "ts_corr(close, volume, 20) > 0.5",
                   "ts_regression(returns, %s, 5, lag = 0, rettype = 2) > 0"%field,
                   "ts_regression(returns, %s, 20, lag = 0, rettype = 2) > 0"%field,
                   "ts_regression(returns, ts_step(20), 20, lag = 0, rettype = 2) > 0",
                   "ts_regression(returns, ts_step(5), 5, lag = 0, rettype = 2) > 0"]

    exit_events = ["abs(returns) > 0.1", "-1"]

    usa_events = ["rank(rp_css_business) > 0.8", "ts_rank(rp_css_business, 22) > 0.8", "rank(vec_avg(mws82_sentiment)) > 0.8",
                  "ts_rank(vec_avg(mws82_sentiment),22) > 0.8", "rank(vec_avg(nws48_ssc)) > 0.8",
                  "ts_rank(vec_avg(nws48_ssc),22) > 0.8", "rank(vec_avg(mws50_ssc)) > 0.8", "ts_rank(vec_avg(mws50_ssc),22) > 0.8",
                  "ts_rank(vec_sum(scl12_alltype_buzzvec),22) > 0.9", "pcr_oi_270 < 1", "pcr_oi_270 > 1",]

    asi_events = ["rank(vec_avg(mws38_score)) > 0.8", "ts_rank(vec_avg(mws38_score),22) > 0.8"]

    eur_events = ["rank(rp_css_business) > 0.8", "ts_rank(rp_css_business, 22) > 0.8",
                  "rank(vec_avg(oth429_research_reports_fundamental_keywords_4_method_2_pos)) > 0.8",
                  "ts_rank(vec_avg(oth429_research_reports_fundamental_keywords_4_method_2_pos),22) > 0.8",
                  "rank(vec_avg(mws84_sentiment)) > 0.8", "ts_rank(vec_avg(mws84_sentiment),22) > 0.8",
                  "rank(vec_avg(mws85_sentiment)) > 0.8", "ts_rank(vec_avg(mws85_sentiment),22) > 0.8",
                  "rank(mdl110_analyst_sentiment) > 0.8", "ts_rank(mdl110_analyst_sentiment, 22) > 0.8",
                  "rank(vec_avg(nws3_scores_posnormscr)) > 0.8",
                  "ts_rank(vec_avg(nws3_scores_posnormscr),22) > 0.8",
                  "rank(vec_avg(mws36_sentiment_words_positive)) > 0.8",
                  "ts_rank(vec_avg(mws36_sentiment_words_positive),22) > 0.8"]

    glb_events = ["rank(vec_avg(mdl109_news_sent_1m)) > 0.8",
                  "ts_rank(vec_avg(mdl109_news_sent_1m),22) > 0.8",
                  "rank(vec_avg(nws20_ssc)) > 0.8",
                  "ts_rank(vec_avg(nws20_ssc),22) > 0.8",
                  "vec_avg(nws20_ssc) > 0",
                  "rank(vec_avg(nws20_bee)) > 0.8",
                  "ts_rank(vec_avg(nws20_bee),22) > 0.8",
                  "rank(vec_avg(nws20_qmb)) > 0.8",
                  "ts_rank(vec_avg(nws20_qmb),22) > 0.8"]

    chn_events = ["rank(vec_avg(oth111_xueqiunaturaldaybasicdivisionstat_senti_conform)) > 0.8",
                  "ts_rank(vec_avg(oth111_xueqiunaturaldaybasicdivisionstat_senti_conform),22) > 0.8",
                  "rank(vec_avg(oth111_gubanaturaldaydevicedivisionstat_senti_conform)) > 0.8",
                  "ts_rank(vec_avg(oth111_gubanaturaldaydevicedivisionstat_senti_conform),22) > 0.8",
                  "rank(vec_avg(oth111_baragedivisionstat_regi_senti_conform)) > 0.8",
                  "ts_rank(vec_avg(oth111_baragedivisionstat_regi_senti_conform),22) > 0.8"]

    kor_events = ["rank(vec_avg(mdl110_analyst_sentiment)) > 0.8",
                  "ts_rank(vec_avg(mdl110_analyst_sentiment),22) > 0.8",
                  "rank(vec_avg(mws38_score)) > 0.8",
                  "ts_rank(vec_avg(mws38_score),22) > 0.8"]

    twn_events = ["rank(vec_avg(mdl109_news_sent_1m)) > 0.8",
                  "ts_rank(vec_avg(mdl109_news_sent_1m),22) > 0.8",
                  "rank(rp_ess_business) > 0.8",
                  "ts_rank(rp_ess_business,22) > 0.8"]

    for oe in open_events:
        for ee in exit_events:
            alpha = "%s(%s, %s, %s)"%(op, oe, field, ee)
            output.append(alpha)
    return output

def check_submission(alpha_bag, gold_bag, start):
    """检查提交状态"""
    depot = []
    s = login()
    for idx, g in enumerate(alpha_bag):
        if idx < start:
            continue
        if idx % 5 == 0:
            print(idx)
        if idx % 200 == 0:
            time.sleep(60)
            s = login()
        pc = get_check_submission(s, g)
        if pc == "sleep":
            sleep(100)
            s = login()
            alpha_bag.append(g)
        elif pc != pc:
            print("check self-corrlation error")
            sleep(100)
            alpha_bag.append(g)
        elif pc == "fail":
            continue
        elif pc == "error":
            depot.append(g)
        else:
            print(g)
            gold_bag.append((g, pc))
    print(depot)
    return gold_bag

def get_check_submission(s, alpha_id):
    """获取提交检查结果"""
    while True:
        time.sleep(GLOBAL_REQUEST_DELAY)
        result = s.get(f"https://api.worldquantbrain.com/alphas/{alpha_id}/check")
        if "retry-after" in result.headers:
            sleep_time = float(result.headers["Retry-After"])
            print(f"⚠ 检查Alpha被限流，等待 {sleep_time} 秒")
            time.sleep(sleep_time)
        else:
            break
    try:
        if result.json().get("is", 0) == 0:
            print("logged out")
            return "sleep"
        checks_df = pd.DataFrame(result.json()["is"]["checks"])
        pc = checks_df[checks_df.name == "PROD_CORRELATION"]["value"].values[0]
        if not any(checks_df["result"] ==
