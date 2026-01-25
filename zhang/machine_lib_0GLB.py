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
 
 
basic_ops = ["reverse", "inverse", "rank", "zscore", "quantile", "normalize"]
 
ts_ops = ["ts_rank", "ts_zscore", "ts_delta",  "ts_sum", "ts_delay", 
          "ts_std_dev", "ts_mean",  "ts_arg_min", "ts_arg_max","ts_scale", "ts_quantile"]
 
ops_set = basic_ops + ts_ops 

def login():
    """
    修复：增加用户名密码填写提示 + 登录状态验证 + 错误处理
    """
    # ========== 请在这里填写你的账号密码 ==========
    username = "your_username_here"  # 替换为你的WorldQuant账号
    password = "your_password_here"  # 替换为你的WorldQuant密码
    # =============================================
    
    if not username or not password:
        print("❌ 错误：请先在login()函数中填写你的账号密码！")
        exit(1)
    
    # Create a session to persistently store the headers
    s = requests.Session()
 
    # Save credentials into session
    s.auth = (username, password)
 
    # Send a POST request to the /authentication API
    try:
        response = s.post('https://api.worldquantbrain.com/authentication')
        response.raise_for_status()  # 触发HTTP错误
        
        if response.status_code == 201:
            print("✅ 登录成功！")
            print(f"   登录响应状态: {response.status_code}")
            # 测试API是否可用
            test_response = s.get('https://api.worldquantbrain.com/users/self')
            if test_response.status_code == 200:
                print("   API测试响应: 200（认证完全正常）")
            return s
        else:
            print(f"❌ 登录失败：响应状态码 {response.status_code}")
            print(f"   响应内容: {response.text}")
            exit(1)
            
    except requests.exceptions.HTTPError as e:
        print(f"❌ 登录失败：{e}")
        print(f"   响应内容: {response.text}")
        if "INVALID_CREDENTIALS" in response.text:
            print("   原因：用户名或密码错误，请检查！")
        exit(1)
    except Exception as e:
        print(f"❌ 登录异常：{str(e)}")
        exit(1)


def get_datasets(
    s,
    instrument_type: str = 'EQUITY',
    region: str = 'USA',
    delay: int = 1,
    universe: str = 'TOP3000'
):
    """修复：增加调试和容错"""
    try:
        url = "https://api.worldquantbrain.com/data-sets?" +\
            f"instrumentType={instrument_type}&region={region}&delay={str(delay)}&universe={universe}"
        result = s.get(url)
        result.raise_for_status()
        
        data = result.json()
        # 兼容不同的返回结构
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


def get_datafields(
    s,
    instrument_type: str = 'EQUITY',
    region: str = 'USA',
    delay: int = 1,
    universe: str = 'TOP3000',
    dataset_id: str = '',
    search: str = ''
):
    """
    修复：
    1. 增加API返回调试信息
    2. 处理count/results字段不存在的情况
    3. 增加容错机制
    4. 分页逻辑优化
    """
    try:
        if len(search) == 0:
            url_template = "https://api.worldquantbrain.com/data-fields?" +\
                f"&instrumentType={instrument_type}" +\
                f"&region={region}&delay={str(delay)}&universe={universe}&dataset.id={dataset_id}&limit=50" +\
                "&offset={x}"
        else:
            url_template = "https://api.worldquantbrain.com/data-fields?" +\
                f"&instrumentType={instrument_type}" +\
                f"&region={region}&delay={str(delay)}&universe={universe}&limit=50" +\
                f"&search={search}" +\
                "&offset={x}"
        
        # 第一步：获取第一页数据，调试返回结构
        first_page = s.get(url_template.format(x=0))
        first_page.raise_for_status()
        first_data = first_page.json()
        
        # 打印调试信息（关键）
        print(f"[调试] get_datafields API返回结构：")
        print(f"   所有键: {list(first_data.keys()) if isinstance(first_data, dict) else '返回的是列表'}")
        print(f"   前100字符: {str(first_data)[:100]}...")
        
        # 处理count字段
        if len(search) == 0:
            # 兼容count不存在的情况
            count = first_data.get('count', 0)
            # 如果没有count，尝试通过results长度估算
            if count == 0 and 'results' in first_data:
                count = len(first_data['results']) * 2  # 预估分页数量
        else:
            count = 100
        
        datafields_list = []
        # 分页获取数据（增加最大页数限制，避免无限循环）
        max_pages = 10  # 最多获取10页（500条）
        for x in range(0, min(count, max_pages*50), 50):
            try:
                datafields = s.get(url_template.format(x=x))
                datafields.raise_for_status()
                page_data = datafields.json()
                
                # 兼容不同的返回结构
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
                    break  # 没有更多数据
                
            except Exception as e:
                print(f"⚠ 获取第{x//50+1}页失败：{str(e)}，跳过")
                continue
        
        # 展平列表
        datafields_list_flat = [item for sublist in datafields_list for item in sublist]
        print(f"[成功] 共获取到 {len(datafields_list_flat)} 个字段")
        
        # 转换为DataFrame
        datafields_df = pd.DataFrame(datafields_list_flat)
        return datafields_df
        
    except Exception as e:
        print(f"❌ 获取数据字段失败：{str(e)}")
        # 返回空DataFrame避免程序崩溃
        return pd.DataFrame(columns=['id', 'type', 'description'])

def get_vec_fields(fields):
    # 请在此处添加获得权限的Vector操作符
    vec_ops = ["vec_avg", "vec_sum"]
    vec_fields = []
 
    for field in fields:
        for vec_op in vec_ops:
            if vec_op == "vec_choose":
                vec_fields.append("%s(%s, nth=-1)"%(vec_op, field))
                vec_fields.append("%s(%s, nth=0)"%(vec_op, field))
            else:
                vec_fields.append("%s(%s)"%(vec_op, field))
 
    return(vec_fields)

def process_datafields(df):
    datafields = []
    datafields += df[df['type'] == "MATRIX"]["id"].tolist()
    datafields += get_vec_fields(df[df['type'] == "VECTOR"]["id"].tolist())
    return ["winsorize(ts_backfill(%s, 120), std=4)"%field for field in datafields]

def ts_factory(op, field):
    output = []
    #days = [3, 5, 10, 20, 60, 120, 240]
    days = [5, 22, 66, 120, 240]
    
    for day in days:
        alpha = "%s(%s, %d)"%(op, field, day)
        output.append(alpha)
    
    return output

def first_order_factory(fields, ops_set):
    alpha_set = []
    #for field in fields:
    for field in fields:
        #reverse op does the work
        alpha_set.append(field)
        #alpha_set.append("-%s"%field)
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
    '''
    Input:
        alpha_list : list of (alpha, decay) tuples
        limit_of_multi_simulations : number of children simulation in a multi-simulation
        limit_of_multi_simulations : number of simultaneous multi-simulations
    Output:
        task : [10 * (alpha, decay)] for a multi-simulation
        pool : [10 * [10 * (alpha, decay)]] for simultaneous multi-simulations
        pools : [[10 * [10 * (alpha, decay)]]]
    '''
    tasks = [alpha_list[i:i + limit_of_children_simulations] for i in range(0, len(alpha_list), limit_of_children_simulations)]
    pools = [tasks[i:i + limit_of_multi_simulations] for i in range(0, len(tasks), limit_of_multi_simulations)]
    return pools

def multi_simulate(alpha_pools, neut, region, universe, start):
    """修复：复用已登录的session，避免重复登录"""
    # 不再重新登录，使用外部传入的session（主程序中修改）
    s = login()  # 保留登录逻辑，但主程序会提前登录
    
    brain_api_url = 'https://api.worldquantbrain.com'

    for x, pool in enumerate(alpha_pools):
        if x < start: continue
        progress_urls = []
        for y, task in enumerate(pool):
            # 10 tasks, 10 alpha in each task
            sim_data_list = generate_sim_data(task, region, universe, neut)
            try:
                simulation_response = s.post('https://api.worldquantbrain.com/simulations', json=sim_data_list)
                simulation_response.raise_for_status()
                simulation_progress_url = simulation_response.headers['Location']
                progress_urls.append(simulation_progress_url)
            except Exception as e:
                print(f"❌ 提交模拟任务失败: {e}")
                print(f"   响应内容: {simulation_response.content if 'simulation_response' in locals() else '无'}")
                sleep(600)
                s = login()

        print(f"pool {x} task {y} post done")

        for j, progress in enumerate(progress_urls):
            try:
                while True:
                    simulation_progress = s.get(progress)
                    if simulation_progress.headers.get("Retry-After", 0) == 0:
                        break
                    sleep(float(simulation_progress.headers["Retry-After"]))

                status = simulation_progress.json().get("status", 0)
                if status != "COMPLETE":
                    print(f"Not complete : {progress}")

            except KeyError:
                print(f"look into: {progress}")
            except Exception as e:
                print(f"other error: {e}")

        print(f"pool {x} task {j} simulate done")
    
    print("Simulate done")

def generate_sim_data(alpha_list, region, uni, neut):
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

def set_alpha_properties(
    s,
    alpha_id,
    name: str = None,
    color: str = None,
    selection_desc: str = "None",
    combo_desc: str = "None",
    tags: str = ["ace_tag"],
):
    """
    Function changes alpha's description parameters
    """
    params = {
        "color": color,
        "name": name,
        "tags": tags,
        "category": None,
        "regular": {"description": None},
        "combo": {"description": combo_desc},
        "selection": {"description": selection_desc},
    }
    try:
        response = s.patch(
            "https://api.worldquantbrain.com/alphas/" + alpha_id, json=params
        )
        response.raise_for_status()
    except Exception as e:
        print(f"❌ 设置Alpha属性失败: {e}")

def get_alphas(start_date, end_date, sharpe_th, fitness_th, region, alpha_num, usage):
    s = login()
    output = []
    # 3E large 3C less
    count = 0
    for i in range(0, alpha_num, 100):
        print(i)
        url_e = "https://api.worldquantbrain.com/users/self/alphas?limit=100&offset=%d"%(i) \
                + "&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E=2025-" + start_date  \
                + "T00:00:00-04:00&dateCreated%3C2025-" + end_date \
                + "T00:00:00-04:00&is.fitness%3E" + str(fitness_th) + "&is.sharpe%3E" \
                + str(sharpe_th) + "&settings.region=" + region + "&order=-is.sharpe&hidden=false&type!=SUPER"
        url_c = "https://api.worldquantbrain.com/users/self/alphas?limit=100&offset=%d"%(i) \
                + "&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E=2025-" + start_date  \
                + "T00:00:00-04:00&dateCreated%3C2025-" + end_date \
                + "T00:00:00-04:00&is.fitness%3C-" + str(fitness_th) + "&is.sharpe%3C-" \
                + str(sharpe_th) + "&settings.region=" + region + "&order=is.sharpe&hidden=false&type!=SUPER"
        urls = [url_e]
        if usage != "submit":
            urls.append(url_c)
        for url in urls:
            try:
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
                print(f"%d finished re-login: {e}"%i)
                s = login()

    print("count: %d"%count)
    return output

def prune(next_alpha_recs, prefix, keep_num):
    # prefix is the datafield prefix, fnd6, mdl175 ...
    # keep_num is the num of top sharpe same-datafield alpha
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
    second_order = []
    for fo in first_order:
        for group_op in group_ops:
            second_order += group_factory(group_op, fo, region)
    return second_order


def group_factory(op, field, region):
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
    
    #bps_group = "bucket(rank(fnd28_value_05480), range='0.1, 1, 0.1')"
    #pb_group = "bucket(rank(close/fnd28_value_05480), range='0.1, 1, 0.1')"
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
    depot = []
    s = login()
    for idx, g in enumerate(alpha_bag):
        if idx < start:
            continue
        if idx % 5 == 0:
            print(idx)
        if idx % 200 == 0:
            s = login()
        #print(idx)
        pc = get_check_submission(s, g)
        if pc == "sleep":
            sleep(100)
            s = login()
            alpha_bag.append(g)
        elif pc != pc:
            # pc is nan
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
    while True:
        result = s.get("https://api.worldquantbrain.com/alphas/" + alpha_id + "/check")
        if "retry-after" in result.headers:
            time.sleep(float(result.headers["Retry-After"]))
        else:
            break
    try:
        if result.json().get("is", 0) == 0:
            print("logged out")
            return "sleep"
        checks_df = pd.DataFrame(
                result.json()["is"]["checks"]
        )
        pc = checks_df[checks_df.name == "PROD_CORRELATION"]["value"].values[0]
        if not any(checks_df["result"] == "FAIL"):
            return pc
        else:
            return "fail"
    except:
        print("catch: %s"%(alpha_id))
        return "error"
    

def view_alphas(gold_bag):
    s = login()
    sharp_list = []
    for gold, pc in gold_bag:
        triple = locate_alpha(s, gold)
        info = [triple[0], triple[2], triple[3], triple[4], triple[5], triple[6], triple[1]]
        info.append(pc)
        sharp_list.append(info)

    sharp_list.sort(reverse=True, key = lambda x : x[1])
    for i in sharp_list:
        print(i)
 
def locate_alpha(s, alpha_id):
    while True:
        alpha = s.get("https://api.worldquantbrain.com/alphas/" + alpha_id)
        if "retry-after" in alpha.headers:
            time.sleep(float(alpha.headers["Retry-After"]))
        else:
            break
    string = alpha.content.decode('utf-8')
    metrics = json.loads(string)
    #print(metrics["regular"]["code"])
    
    dateCreated = metrics["dateCreated"]
    sharpe = metrics["is"]["sharpe"]
    fitness = metrics["is"]["fitness"]
    turnover = metrics["is"]["turnover"]
    margin = metrics["is"]["margin"]
    decay = metrics["settings"]["decay"]
    exp = metrics['regular']['code']
    
    triple = [alpha_id, exp, sharpe, turnover, fitness, margin, dateCreated, decay]
    return triple


# some factory for other operators 
def vector_factory(op, field):
    output = []
    vectors = ["cap"]
    
    for vector in vectors:
        alpha = "%s(%s, %s)"%(op, field, vector)
        output.append(alpha)
    
    return output
 
 
def ts_comp_factory(op, field, factor, paras):
    output = []
    #l1, l2 = [3, 5, 10, 20, 60, 120, 240], paras
    l1, l2 = [5, 22, 66, 240], paras
    comb = list(product(l1, l2))
    
    for day,para in comb:
        if type(para) == float:
            alpha = "%s(%s, %d, %s=%.1f)"%(op, field, day, factor, para)
        elif type(para) == int:
            alpha = "%s(%s, %d, %s=%d)"%(op, field, day, factor, para)
            
        output.append(alpha)
    
    return output
 
def twin_field_factory(op, field, fields):
    output = []
    #days = [3, 5, 10, 20, 60, 120, 240]
    days = [5, 22, 66, 240]
    outset = list(set(fields) - set([field]))
    
    for day in days:
        for counterpart in outset:
            alpha = "%s(%s, %s, %d)"%(op, field, counterpart, day)
            output.append(alpha)
    
    return output
 
def login_hk():
    """修复：增加用户名密码填写提示"""
    # ========== 请在这里填写你的账号密码 ==========
    username = "user"  # 替换为你的WorldQuant账号
    password = "pass"  # 替换为你的WorldQuant密码
    # =============================================
    
    if not username or not password:
        print("❌ 错误：请先在login_hk()函数中填写你的账号密码！")
        exit(1)
    
    # Create a session to persistently store the headers
    s = requests.Session()
    
    # Save credentials into session
    s.auth = (username, password)
    
    # Send a POST request to the /authentication API
    response = s.post('https://api.worldquantbrain.com/authentication')
    
    if response.status_code == requests.codes.unauthorized:
        # Check if biometrics is required
        if response.headers.get("WWW-Authenticate") == "persona":
            print(
                "Complete biometrics authentication by scanning your face. Follow the link: \n"
                + urljoin(response.url, response.headers["Location"]) + "\n"
            )
            input("Press any key after you complete the biometrics authentication.")
            
            # Retry the authentication after biometrics
            biometrics_response = s.post(urljoin(response.url, response.headers["Location"]))
            
            while biometrics_response.status_code != 201:
                input("Biometrics authentication is not complete. Please try again and press any key when completed.")
                biometrics_response = s.post(urljoin(response.url, response.headers["Location"]))
                
            print("Biometrics authentication completed.")
        else:
            print("\nIncorrect username or password. Please check your credentials.\n")
    else:
        print("Logged in successfully.")
    
    return s 
