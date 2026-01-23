"""
############
"""
import os
import sys

import requests
from time import sleep
import time
import json
import pandas as pd
from itertools import product
from collections import defaultdict
from datetime import datetime
import aiofiles
import aiohttp
import asyncio

def login():
    # 从txt文件解密并读取数据
    # txt格式:
    # password: 'password'
    # username: 'username'
    def load_decrypted_data(txt_file='user_info.txt'):
        with open(txt_file, 'r') as f:
            data = f.read()
            data = data.strip().split('\n')

            data = {line.split(': ')[0]: line.split(': ')[1] for line in data}

        return data['username'][1:-1], data['password'][1:-1]

    username, password = load_decrypted_data("user_info.txt")

    # Create a session to persistently store the headers
    s = requests.Session()

    # Save credentials into session
    s.auth = (username, password)

    # Send a POST request to the /authentication API
    response = s.post('https://api.worldquantbrain.com/authentication')
    print(response.content)
    return s

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 1000)

brain_api_url = os.environ.get("BRAIN_API_URL", "https://api.worldquantbrain.com")

basic_ops = ["log", "sqrt", "reverse", "inverse", "rank", "zscore", "log_diff", "s_log_1p",
             'fraction', 'quantile', "normalize", "scale_down"]

ts_ops = ["ts_rank", "ts_zscore", "ts_delta", "ts_sum", "ts_product",
          "ts_ir", "ts_std_dev", "ts_mean", "ts_arg_min", "ts_arg_max", "ts_min_diff",
          "ts_max_diff", "ts_returns", "ts_scale", "ts_skewness", "ts_kurtosis",
          "ts_quantile"]

ts_not_use = ["ts_min", "ts_max", "ts_delay", "ts_median", ]

arsenal = ["ts_moment", "ts_entropy", "ts_min_max_cps", "ts_min_max_diff", "inst_tvr", 'sigmoid',
           "ts_decay_exp_window", "ts_percentage", "vector_neut", "vector_proj", "signed_power"]

twin_field_ops = ["ts_corr", "ts_covariance", "ts_co_kurtosis", "ts_co_skewness", "ts_theilsen"]

group_ops = ["group_neutralize", "group_rank", "group_normalize", "group_scale", "group_zscore"]

group_ac_ops = ["group_sum", "group_max", "group_mean", "group_median", "group_min", "group_std_dev", ]

vec_ops = ["vec_avg", "vec_sum", "vec_ir", "vec_max",
                   "vec_count", "vec_skewness", "vec_stddev", "vec_choose"]

ops_set = basic_ops + ts_ops + arsenal + group_ops

s = login()
res = s.get("https://api.worldquantbrain.com/operators")
aval = pd.DataFrame(res.json())['name'].tolist()
ts_ops = [op for op in ts_ops if op in aval]
basic_ops = [op for op in basic_ops if op in aval]
group_ops = [op for op in group_ops if op in aval]
twin_field_ops = [op for op in twin_field_ops if op in aval]
arsenal = [op for op in arsenal if op in aval]
vec_ops = [op for op in vec_ops if op in aval]
s.close()


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
    Function changes alpha's description parameters
    """
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

    response = s.patch(
        "https://api.worldquantbrain.com/alphas/" + alpha_id, json=params
    )



def get_vec_fields(fields):
    vec_fields = []

    for field in fields:
        for vec_op in vec_ops:
            if vec_op == "vec_choose":
                vec_fields.append("%s(%s, nth=-1)" % (vec_op, field))
                vec_fields.append("%s(%s, nth=0)" % (vec_op, field))
            else:
                vec_fields.append("%s(%s)" % (vec_op, field))

    return (vec_fields)



def get_datafields(
        s,
        instrument_type: str = 'EQUITY',
        region: str = 'USA',
        delay: int = 1,
        universe: str = 'TOP3000',
        dataset_id: str = '',
        search: str = ''
):
    if len(search) == 0:
        url_template = "https://api.worldquantbrain.com/data-fields?" + \
                       f"&instrumentType={instrument_type}" + \
                       f"&region={region}&delay={str(delay)}&universe={universe}&dataset.id={dataset_id}&limit=50" + \
                       "&offset={x}"
        count = s.get(url_template.format(x=0)).json()['count']

    else:
        url_template = "https://api.worldquantbrain.com/data-fields?" + \
                       f"&instrumentType={instrument_type}" + \
                       f"&region={region}&delay={str(delay)}&universe={universe}&limit=50" + \
                       f"&search={search}" + \
                       "&offset={x}"
        count = 100

    datafields_list = []
    for x in range(0, count, 50):
        datafields = s.get(url_template.format(x=x))
        datafields_list.append(datafields.json()['results'])

    datafields_list_flat = [item for sublist in datafields_list for item in sublist]

    datafields_df = pd.DataFrame(datafields_list_flat)
    return datafields_df


def process_datafields(df, data_type, min_start_date: str = "2016-01-01"):
    if min_start_date and "startDate" in df.columns:
        try:
            cutoff = pd.to_datetime(min_start_date)
            start_dates = pd.to_datetime(df["startDate"], errors="coerce")
            valid_mask = start_dates.isna() | (start_dates <= cutoff)
            removed = (~valid_mask).sum()
            if removed:
                print(
                    f"[process_datafields] {removed} fields skipped: startDate after {min_start_date},",
                    "likely to avoid long-term NaN exposure.",
                )
            df = df.loc[valid_mask]
        except Exception as exc:
            print(f"[process_datafields] Failed to filter by startDate ({exc}); using all fields.")

    if data_type == "matrix":
        datafields = df[df['type'] == "MATRIX"]["id"].tolist()
    elif data_type == "vector":
        datafields = get_vec_fields(df[df['type'] == "VECTOR"]["id"].tolist())

    tb_fields = []
    for field in datafields:
        tb_fields.append("winsorize(ts_backfill(%s, 120), std=4)" % field)
        # tb_fields.append("%s" % field)
    return tb_fields


def get_alphas(start_date, end_date, sharpe_th, fitness_th, longCount_th, shortCount_th, region, universe, delay,
               instrumentType, alpha_num, usage, tag: str = '', color_exclude='', s=None):


    # color None, RED, YELLOW, GREEN, BLUE, PURPLE CYX专用
    if s is None:
        s = login()
    alpha_list = []
    next_alphas = []
    decay_alphas = []
    check_alphas = []
    # 3E large 3C less
    # 正的
    i = 0
    while True:
        url_e = (f"https://api.worldquantbrain.com/users/self/alphas?limit=100&offset={i}"
                 f"&tag%3D{tag}&is.longCount%3E={longCount_th}&is.shortCount%3E={shortCount_th}"
                 f"&settings.region={region}&is.sharpe%3E={sharpe_th}&is.fitness%3E={fitness_th}"
                 f"&settings.universe={universe}&status=UNSUBMITTED&dateCreated%3E={start_date}"
                 f"T00:00:00-04:00&dateCreated%3C{end_date}T00:00:00-04:00&type=REGULAR&color!={color_exclude}&"
                 f"settings.delay={delay}&settings.instrumentType={instrumentType}&order=-is.sharpe&hidden=false&type!=SUPER")

        response = s.get(url_e)
        # print(response.json())
        try:
            i += 100
            count = int(response.json()["count"])
            print(f"一共有{count}个因子等待被获取，已经获取了{i-100}个")
            alpha_list.extend(response.json()["results"])
            if i >= count or i == 9900:
                break
            time.sleep(3)
        except Exception as e:
            print(f"Failed to get alphas: {e}")
            i -= 100
            time.sleep(60)
            s = login()
            print("%d finished re-login" % i)

    # 负的
    if usage != "submit":
        i = 0
        while True:
            url_c = (f"https://api.worldquantbrain.com/users/self/alphas?limit=100&offset={i}"
                     f"&tag%3D{tag}&is.longCount%3E={longCount_th}&is.shortCount%3E={shortCount_th}"
                     f"&settings.region={region}&is.sharpe%3C=-{sharpe_th}&is.fitness%3C=-{fitness_th}"
                     f"&settings.universe={universe}&status=UNSUBMITTED&dateCreated%3E={start_date}"
                     f"T00:00:00-04:00&dateCreated%3C{end_date}T00:00:00-04:00&type=REGULAR&color!={color_exclude}&"
                     f"settings.delay={delay}&settings.instrumentType={instrumentType}&order=-is.sharpe&hidden=false&type!=SUPER")

            response = s.get(url_c)
            # print(response.json())
            try:
                count = response.json()["count"]
                if i >= count or i == 9900:
                    break
                alpha_list.extend(response.json()["results"])
                i += 100
            except Exception as e:
                print(f"Failed to get alphas: {e}")
                time.sleep(5)
                s = login()
                print("%d finished re-login" % i)

    # print(alpha_list)
    if len(alpha_list) == 0:
        if usage != "submit":
            return {"next": [], "decay": []}
        else:
            return {"check": []}

    # print(response.json())
    if usage != "submit":
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
            region = alpha_list[j]["settings"]["region"]

            concentrated_weight = next(
                (check.get('value', 0) for check in alpha_list[j]["is"]["checks"] if
                 check["name"] == "CONCENTRATED_WEIGHT"), 0)
            sub_universe_sharpe = next(
                (check.get('value', 99) for check in alpha_list[j]["is"]["checks"] if
                 check["name"] == "LOW_SUB_UNIVERSE_SHARPE"), 99)
            two_year_sharpe = next(
                (check.get('value', 99) for check in alpha_list[j]["is"]["checks"] if check["name"] == "LOW_2Y_SHARPE"),
                99)
            ladder_sharpe = next(
                (check.get('value', 99) for check in alpha_list[j]["is"]["checks"] if
                 check["name"] == "IS_LADDER_SHARPE"), 99)

            conditions = ((longCount > 100 or shortCount > 100) and
                          (concentrated_weight < 0.2) and
                          (abs(sub_universe_sharpe) > sharpe_th / 1.66) and
                          (abs(two_year_sharpe) > sharpe_th) and
                          (abs(ladder_sharpe) > sharpe_th) and
                          (not (region == "CHN" and sharpe < 0))
                          )
            # if (sharpe > 1.2 and sharpe < 1.6) or (sharpe < -1.2 and sharpe > -1.6):
            if conditions:
                if sharpe < 0:
                    exp = "-%s" % exp
                rec = [alpha_id, exp, sharpe, turnover, fitness, margin, longCount, shortCount, dateCreated, decay]
                # print(rec)
                if turnover > 0.7:
                    rec.append(decay * 4)
                    decay_alphas.append(rec)
                elif turnover > 0.6:
                    rec.append(decay * 3 + 3)
                    decay_alphas.append(rec)
                elif turnover > 0.5:
                    rec.append(decay * 3)
                    decay_alphas.append(rec)
                elif turnover > 0.4:
                    rec.append(decay * 2)
                    decay_alphas.append(rec)
                elif turnover > 0.35:
                    rec.append(decay + 4)
                    decay_alphas.append(rec)
                elif turnover > 0.3:
                    rec.append(decay + 2)
                    decay_alphas.append(rec)
                else:
                    next_alphas.append(rec)
        output_dict = {"next": next_alphas, "decay": decay_alphas}
        print("获取到了%d个因子" % (len(next_alphas) + len(decay_alphas)))
    else:
        for alpha_detail in alpha_list:
            id = alpha_detail["id"]
            type = alpha_detail["type"]
            author = alpha_detail["author"]
            instrumentType = alpha_detail["settings"]["instrumentType"]
            region = alpha_detail["settings"]["region"]
            universe = alpha_detail["settings"]["universe"]
            delay = alpha_detail["settings"]["delay"]
            decay = alpha_detail["settings"]["decay"]
            neutralization = alpha_detail["settings"]["neutralization"]
            truncation = alpha_detail["settings"]["truncation"]
            pasteurization = alpha_detail["settings"]["pasteurization"]
            unitHandling = alpha_detail["settings"]["unitHandling"]
            nanHandling = alpha_detail["settings"]["nanHandling"]
            language = alpha_detail["settings"]["language"]
            visualization = alpha_detail["settings"]["visualization"]
            code = alpha_detail["regular"]["code"]
            description = alpha_detail["regular"]["description"]
            operatorCount = alpha_detail["regular"]["operatorCount"]
            dateCreated = alpha_detail["dateCreated"]
            dateSubmitted = alpha_detail["dateSubmitted"]
            dateModified = alpha_detail["dateModified"]
            name = alpha_detail["name"]
            favorite = alpha_detail["favorite"]
            hidden = alpha_detail["hidden"]
            color = alpha_detail["color"]
            category = alpha_detail["category"]
            tags = alpha_detail["tags"]
            classifications = alpha_detail["classifications"]
            grade = alpha_detail["grade"]
            stage = alpha_detail["stage"]
            status = alpha_detail["status"]
            pnl = alpha_detail["is"]["pnl"]
            bookSize = alpha_detail["is"]["bookSize"]
            longCount = alpha_detail["is"]["longCount"]
            shortCount = alpha_detail["is"]["shortCount"]
            turnover = alpha_detail["is"]["turnover"]
            returns = alpha_detail["is"]["returns"]
            drawdown = alpha_detail["is"]["drawdown"]
            margin = alpha_detail["is"]["margin"]
            fitness = alpha_detail["is"]["fitness"]
            sharpe = alpha_detail["is"]["sharpe"]
            startDate = alpha_detail["is"]["startDate"]
            checks = alpha_detail["is"]["checks"]
            os = alpha_detail["os"]
            train = alpha_detail["train"]
            test = alpha_detail["test"]
            prod = alpha_detail["prod"]
            competitions = alpha_detail["competitions"]
            themes = alpha_detail["themes"]
            team = alpha_detail["team"]
            checks_df = pd.DataFrame(checks)
            pyramids = next(
                ([y['name'] for y in item['pyramids']] for item in checks if item['name'] == 'MATCHES_PYRAMID'), None)

            if any(checks_df["result"] == "FAIL"):
                # 最基础的项目不通过
                set_alpha_properties(s, id, color='RED')
                continue
            else:
                # 通过了最基础的项目
                # 把全部的信息以字典的形式返回
                rec = {"id": id, "type": type, "author": author, "instrumentType": instrumentType, "region": region,
                       "universe": universe, "delay": delay, "decay": decay, "neutralization": neutralization,
                       "truncation": truncation, "pasteurization": pasteurization, "unitHandling": unitHandling,
                       "nanHandling": nanHandling, "language": language, "visualization": visualization, "code": code,
                       "description": description, "operatorCount": operatorCount, "dateCreated": dateCreated,
                       "dateSubmitted": dateSubmitted, "dateModified": dateModified, "name": name, "favorite": favorite,
                       "hidden": hidden, "color": color, "category": category, "tags": tags,
                       "classifications": classifications, "grade": grade, "stage": stage, "status": status, "pnl": pnl,
                       "bookSize": bookSize, "longCount": longCount, "shortCount": shortCount, "turnover": turnover,
                       "returns": returns, "drawdown": drawdown, "margin": margin, "fitness": fitness, "sharpe": sharpe,
                       "startDate": startDate, "checks": checks, "os": os, "train": train, "test": test, "prod": prod,
                       "competitions": competitions, "themes": themes, "team": team, "pyramids": pyramids}
                check_alphas.append(rec)
        output_dict = {"check": check_alphas}

    # 超过了限制
    if usage == 'submit' and count >= 9900:
        if len(output_dict['check']) < len(alpha_list):
            # 那么就再来一遍
            output_dict = get_alphas(start_date, end_date, sharpe_th, fitness_th, longCount_th, shortCount_th,
                                     region, universe, delay, instrumentType, alpha_num, usage, tag, color_exclude)
        else:
            raise Exception("Too many alphas to check!! over 10000, universe: %s, region: %s" % (universe, region))

    return output_dict


def ts_comp_factory(op, field, factor, paras):
    output = []
    # l1, l2 = [3, 5, 10, 20, 60, 120, 240], paras
    l1, l2 = [5, 22, 66, 120, 240], paras
    comb = list(product(l1, l2))

    for day, para in comb:

        if type(para) == float:
            alpha = "%s(%s, %d, %s=%.1f)" % (op, field, day, factor, para)
        elif type(para) == int:
            alpha = "%s(%s, %d, %s=%d)" % (op, field, day, factor, para)

        output.append(alpha)

    return output


def first_order_factory(fields, ops_set):
    alpha_set = []

    for field in fields:
        # reverse op does the work
        alpha_set.append(field)
        for op in ops_set:
            if op in field:
                continue
            if op == "ts_percentage":
                alpha_set += ts_comp_factory(op, field, "percentage", [0.2, 0.5, 0.8])
            elif op == "ts_decay_exp_window":
                alpha_set += ts_comp_factory(op, field, "factor", [0.5])
            elif op == "ts_moment":
                alpha_set += ts_comp_factory(op, field, "k", [2, 3, 4])
            elif op == "ts_entropy":
                alpha_set += ts_comp_factory(op, field, "buckets", [10])
            elif op.startswith("ts_") or op == "inst_tvr":
                alpha_set += ts_factory(op, field)
            elif op.startswith("group_"):
                alpha_set += group_factory(op, field)
            elif op == "signed_power":
                alpha = "%s(%s, 2)" % (op, field)
                alpha_set.append(alpha)
            else:
                alpha = "%s(%s)" % (op, field)
                alpha_set.append(alpha)

    return alpha_set

def get_group_second_order_factory(first_order, group_ops, group_fields=[]):
    second_order = []
    for fo in first_order:
        for group_op in group_ops:
            second_order += group_factory(group_op, fo, group_fields)
    return second_order


def trade_when_factory(op, field, region, delay=1):
    output = []
    open_events = [
        "ts_arg_max(volume, 5) == 0",
        "ts_corr(close, volume, 252) <= 0",
        "ts_corr(close, volume, 20) < 0",
        "ts_corr(close, volume, 5) < 0",
        "ts_mean(volume, 10) > ts_mean(volume, 60)",
        "ts_mean(volume, 10) <= ts_mean(volume, 60)",
        "group_rank(ts_std_dev(returns, 60), sector) > 0.7",
        "ts_zscore(returns, 60) > 2",
        "ts_arg_min(volume, 5) > 3",
        "ts_arg_min(volume, 10) >= 5",
        "ts_std_dev(returns, 5) > ts_std_dev(returns, 20)",
        "ts_arg_max(close, 5) == 0",
        "ts_arg_max(close, 20) == 0",
        "ts_corr(close, volume, 5) > 0",
        "ts_corr(close, volume, 5) > 0.3",
        "ts_corr(close, volume, 5) > 0.5",
        "ts_corr(close, volume, 5) > 0.7",
        "ts_corr(close, volume, 5) > 0.9",
        "ts_corr(close, volume, 5) < 0",
        "ts_corr(close, volume, 5) < 0.3",
        "ts_corr(close, volume, 5) < 0.5",
        "ts_corr(close, volume, 5) < 0.7",
        "ts_corr(close, volume, 5) < 0.9",
        "ts_corr(close, volume, 20) > 0",
        "ts_corr(close, volume, 20) > 0.3",
        "ts_corr(close, volume, 20) > 0.5",
        "ts_corr(close, volume, 20) < 0",
        "ts_corr(close, volume, 20) < 0.3",
        "ts_corr(close, volume, 20) < 0.5",
        "ts_regression(returns, %s, 5, lag = 0, rettype = 2) > 0" % field,
        "ts_regression(returns, %s, 20, lag = 0, rettype = 2) > 0" % field,
        "ts_regression(returns, ts_step(20), 20, lag = 0, rettype = 2) > 0",
        "ts_regression(returns, ts_step(5), 5, lag = 0, rettype = 2) > 0",
    ]
    if delay==1:
        exit_events = ["abs(returns) > 0.1", "-1", "days_from_last_change(ern3_pre_reptime) > 20"]
    else:
        exit_events = ["abs(returns) > 0.1", "-1"]

    for oe in open_events:
        for ee in exit_events:
            alpha = "%s(%s, %s, %s)" % (op, oe, field, ee)
            output.append(alpha)
    return output


def ts_factory(op, field):
    output = []
    # 3天，1周，半个月，一个月，一个季度，半年，一年，两年
    days = [3, 5, 11, 22, 66, 122, 252, 504]

    for day in days:
        alpha = "%s(%s, %d)" % (op, field, day)
        output.append(alpha)

    return output


def group_factory(op, field, group_fields=[]):
    output = []
    vectors = ["cap"]

    # 量价
    cap_group = "bucket(rank(cap), range='0.1, 1, 0.1')"
    sector_cap_group = "bucket(group_rank(cap,sector),range='0,1,0.1')"
    vol_group = "bucket(rank(ts_std_dev(returns,240)),range = '0.1,1,0.1')"
    volatility_group = "bucket(rank(ts_std_dev(returns,20)),range = '0.1, 1, 0.1')"
    liquidity_group = "bucket(rank(close*volume),range = '0.1, 1, 0.1')"
    turnover_group = "bucket(rank(close*volume/cap),range='0.1, 1, 0.1')"
    dividend_yield_group = "bucket(rank(dividend/close), range='0.1, 1, 0.1')"
    adv20_group = "bucket(rank(adv20), range='0.1, 1, 0.1')"

    # 基本面
    sector_asset_group = "bucket(group_rank(assets, sector),range='0.1, 1, 0.1')"
    bps_group = "bucket(rank(fnd28_value_05480/close), range='0.2, 1, 0.2')"
    pb_group = "bucket(rank(fnd28_value_05480/bookvalue_ps), range='0.1, 1, 0.1')"
    debt_to_equity_group = "bucket(rank(liabilities/assets), range='0.1, 1, 0.1')"

    # 看自己有没有
    fnd23_net_income_group = "bucket(rank(fnd23_net_income/assets), range='0.1, 1, 0.1')"
    fnd23_net_debt_group = "bucket(rank(fnd23_net_debt/assets), range='0.1, 1, 0.1')"
    anl14_buy_group = "bucket(rank(anl14_buy), range='0.1, 1, 0.1')"
    anl15_bps_gr_12_m_1m_chg_group = "bucket(rank(anl15_bps_gr_12_m_1m_chg), range='0.1, 1, 0.1')"
    anl15_salgics_gr_18_m_pe_group = "bucket(rank(anl15_salgics_gr_18_m_pe), range='0.1, 1, 0.1')"
    anl4_adjusted_netincome_ft_group = "bucket(rank(anl4_adjusted_netincome_ft), range='0.1, 1, 0.1')"
    call_breakeven_10_group = "bucket(rank(call_breakeven_10), range='0.1, 1, 0.1')"
    correlation_last_60_days_spy_group = "bucket(rank(correlation_last_60_days_spy), range='0.1, 1, 0.1')"
    est_12m_eps_num_28d_group = "bucket(rank(est_12m_eps_num_28d), range='0.1, 1, 0.1')"

    base_group = ["market", "sector", "industry", "subindustry", "country"]

    # 经验总结出来的group
    experts_group = [
        bps_group, cap_group, sector_cap_group, turnover_group,
        volatility_group, liquidity_group, sector_asset_group,
        pb_group, debt_to_equity_group, dividend_yield_group,
        adv20_group
    ]

    if "ts_returns" in aval:
        experts_group.append(vol_group)

    group_fields += base_group
    group_fields +=experts_group
    group_fields = list(set(group_fields))

    for group in group_fields:
        if op.startswith("group_vector"):
            for vector in vectors:
                alpha = "%s(%s,%s,densify(%s))" % (op, field, vector, group)
                output.append(alpha)
        elif op.startswith("group_percentage"):
            alpha = "%s(%s,densify(%s),percentage=0.5)" % (op, field, group)
            output.append(alpha)
        else:
            alpha = "%s(%s,densify(%s))" % (op, field, group)
            output.append(alpha)

    return output


def template_factory(field, region):
    output = []

    output.append(f"""divide(rank({field}), rank(returns))""")
    output.append(f"""signed_power({field}, 0.5)""")
    output.append(f"""signed_power({field}, 2)""")
    output.append(f"""hump(zscore({field}), hump=0.01)""")
    output.append(f"""last_diff_value({field}, 22)""")
    output.append(f"""ts_regression({field}, returns, 252, lag=0, rettype=0)""")

    output.append(f"""
    my_group = market;
    my_group2 = bucket(rank(cap),range='0,1,0.1');
    alpha=rank(group_rank(ts_decay_linear(volume/ts_sum(volume,252),10),my_group)*group_rank(ts_rank({field}, 22),my_group)*group_rank(-ts_delta(close,5),my_group));
    trade_when(volume>adv20,group_neutralize(alpha,my_group2),-1)
    """)

    output.append(f"""
    ts_mean({field}, 252) / ts_std_dev({field}, 252)
    """)

    output.append("""ts_mean({field}, 252) + ts_std_dev({field}, 252)""")
    output.append("""ts_mean({field}, 22) + ts_std_dev({field}, 22)""")
    output.append("""ts_mean({field}, 22) * ts_std_dev({field}, 22)""")

    output.append(f"""ts_regression(ts_zscore(ts_mean({field}, 252),500), ts_zscore(ts_std_dev({field}, 252),500),500)""")
    output.append(f"""1 / ts_std_dev(ts_regression(ts_zscore(ts_mean({field}, 252),500), ts_zscore(ts_std_dev({field}, 252),500),500), 500)""")
    output.append(f"""
    residual = ts_regression(ts_zscore(ts_mean({field}, 252),500), ts_zscore(ts_std_dev({field}, 252), 500), 500);
    residual/ts_std_dev(residual, 500)
    """)

    return output

def while_true_try_decorator(func):
    def wrapper(*args, **kwargs):
        while True:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                print(f"发生错误: {e}. 正在重试...")
                time.sleep(2)
    return wrapper

async def async_login():
    """
    从YAML文件加载用户信息并异步登录到指定API
    """
    def load_decrypted_data(txt_file='user_info.txt'):
        with open(txt_file, 'r') as f:
            data = f.read()
            data = data.strip().split('\n')
            data = {line.split(': ')[0]: line.split(': ')[1] for line in data}

        return data['username'][1:-1], data['password'][1:-1]

    username, password = load_decrypted_data("user_info.txt")

    # 创建一个aiohttp的Session
    conn = aiohttp.TCPConnector(ssl=False)
    session = aiohttp.ClientSession(connector=conn)

    time_out = 5
    while True:
        if time_out < 0:
            print("Login timeout!")
            await session.close()
            raise Exception("Login timeout! 无法登录，退出程序中...")

        time_out -= 1

        try:
            # 发送一个POST请求到/authentication API
            async with session.post('https://api.worldquantbrain.com/authentication',
                                    auth=aiohttp.BasicAuth(username, password)) as response:
                # 检查状态码是否为201，确保登录成功
                if response.status == 201:
                    print("Login successful!")
                else:
                    print(f"Login failed! Status code: {response.status}, Response: {await response.text()}")
                    # 异步睡眠10s
                    await asyncio.sleep(10**time_out)

            return session

        except aiohttp.ClientError as e:
            print(f"Error during login request: {e}")
            await session.close()
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            await session.close()



async def simulate_single(session_manager, alpha_expression, region_info, name, neut,
                          decay, delay, stone_bag, tags=['None'],
                          semaphore=None):
    """
    单次模拟一个alpha表达式对应的某个地区的信息
    """
    async with semaphore:
        # 每个任务在执行前都检查会话时间
        if time.time() - session_manager.start_time > session_manager.expiry_time:
            await session_manager.refresh_session()

        region, uni = region_info
        alpha = "%s" % (alpha_expression)

        print("Simulating for alpha: %s, region: %s, universe: %s, decay: %s" % (alpha, region, uni, decay))

        simulation_data = {
            'type': 'REGULAR',
            'settings': {
                'instrumentType': 'EQUITY',
                'region': region,
                'universe': uni,
                'delay': delay,
                'decay': decay,
                'neutralization': neut,
                'truncation': 0.08,
                'pasteurization': 'ON',
                'unitHandling': 'VERIFY',
                'nanHandling': 'ON',
                'language': 'FASTEXPR',
                'visualization': False,
            },
            'regular': alpha
        }

        while True:
            try:
                async with session_manager.session.post('https://api.worldquantbrain.com/simulations',
                                                        json=simulation_data) as resp:
                    simulation_progress_url = resp.headers.get('Location', 0)
                    if simulation_progress_url == 0:
                        json_data = await resp.json()
                        if type(json_data) == list:
                            print(json_data)
                            detail = json_data.get("detail", 0)
                        else:
                            detail = json_data.get("detail", 0)
                        if detail == 'SIMULATION_LIMIT_EXCEEDED':
                            print("Limited by the number of simulations allowed per time")
                            await asyncio.sleep(5)
                        else:
                            print("detail:", detail)
                            print("json_data:", json_data)
                            print("Alpha expression is duplicated")
                            await asyncio.sleep(1)
                            return 0
                    else:
                        print('simulation_progress_url:', simulation_progress_url)
                        break
            except KeyError:
                print("Location key error during simulation request")
                await asyncio.sleep(60)
                return
            except Exception as e:
                print("An error occurred:", str(e))
                await asyncio.sleep(60)
                return

        while True:
            try:
                async with session_manager.session.get(simulation_progress_url) as resp:
                    json_data = await resp.json()
                    # 获取响应头
                    headers = resp.headers
                    retry_after = headers.get('Retry-After', 0)
                    if retry_after == 0:
                        break
                    await asyncio.sleep(float(retry_after))
            except Exception as e:
                print("Error while checking progress:", str(e))
                await asyncio.sleep(60)

        print("%s done simulating, getting alpha details" % (simulation_progress_url))
        try:
            alpha_id = json_data.get("alpha")

            await async_set_alpha_properties(session_manager.session,
                                             alpha_id,
                                             name="%s" % name,
                                             color=None,
                                             tags=tags)

            async with aiofiles.open(f'records/{name}_simulated_alpha_expression.txt', mode='a') as f:
                await f.write(alpha + '\n')

            # stone_bag.append(alpha_id)

        except KeyError:
            print("Failed to retrieve alpha ID for: %s" % simulation_progress_url)
        except Exception as e:
            print("An error occurred while setting alpha properties:", str(e))

        # return stone_bag
        return 0


async def async_set_alpha_properties(
        session,  # aiohttp 的 session
        alpha_id,
        name: str = None,
        color: str = None,
        description: str = None,
        selection_desc: str = None,
        combo_desc: str = None,
        tags: list = None,
):
    """
    异步函数，修改 alpha 的描述参数
    """

    params = {
        "category": None,
    }
    if color:
        params["color"] = color
    if name:
        params["name"] = name
    if tags:
        params["tags"] = tags
    if description:
        params["regular"] = {"description": description}
    if combo_desc:
        params["combo"] = {"description": combo_desc}
    if selection_desc:
        params["selection"] = {"description": selection_desc}

    url = f"https://api.worldquantbrain.com/alphas/{alpha_id}"

    try:
        async with session.patch(url, json=params) as response:
            # 检查状态码，确保请求成功
            if response.status == 200:
                print(f"Alpha {alpha_id} properties updated successfully! Tag: {tags}")
            else:
                print(
                    f"Failed to update alpha {alpha_id}. Status code: {response.status}, Response: {await response.text()}")

    except aiohttp.ClientError as e:
        print(f"Error during patch request for alpha {alpha_id}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred for alpha {alpha_id}: {e}")



class SessionManager:
    def __init__(self, session, start_time, expiry_time):
        self.session = session
        self.start_time = start_time
        self.expiry_time = expiry_time

    async def refresh_session(self):
        print(datetime.now(),"Session expired, logging in again...")
        await self.session.close()
        self.session = await async_login()
        self.start_time = time.time()


async def simulate_multi(session_manager, alpha_expression_list: list, region_info, name, neut, decay, delay, stone_bag,

                         tags=['None'], semaphore=None):
    """
    单次模拟一个alpha表达式对应的某个地区的信息
    """
    brain_api_url = 'https://api.worldquantbrain.com'

    async with semaphore:
        # 每个任务在执行前都检查会话时间
        if time.time() - session_manager.start_time > session_manager.expiry_time:
            await session_manager.refresh_session()

        if len(alpha_expression_list) > 10:
            raise ValueError("The number of alpha expressions in a pool should be less than 10")

        region, uni = region_info

        # 产生一个pool，一个pool里最多10个alpha
        sim_data_list = []
        for alpha_expression in alpha_expression_list:
            alpha = "%s" % (alpha_expression)
            print(datetime.now(),f"Simulating for alpha: {alpha}, region: {region},"
                         f" universe: {uni}, decay: {decay}, delay: {delay}")

            simulation_data = {
                'type': 'REGULAR',
                'settings': {
                    'instrumentType': 'EQUITY',
                    'region': region,
                    'universe': uni,
                    'delay': delay,
                    'decay': decay,
                    'neutralization': neut,
                    'truncation': 0.08,
                    'pasteurization': 'ON',
                    'unitHandling': 'VERIFY',
                    'nanHandling': 'ON',
                    'language': 'FASTEXPR',
                    'visualization': False,
                },
                'regular': alpha
            }
            sim_data_list.append(simulation_data)

        # 一次性提交10个alpha作为单个task
        max_retries = 5  # 最大重试次数
        retry_count = 0
        while retry_count < max_retries:
            try:
                async with session_manager.session.post('https://api.worldquantbrain.com/simulations',
                                                        json=sim_data_list) as simulation_response:
                    simulation_progress_url = simulation_response.headers.get('Location', 0)
                    if simulation_progress_url == 0:
                        json_data = await simulation_response.json()
                        if type(json_data) == list:
                            print(datetime.now(),f"Response data: {json_data}")
                            detail = json_data.get("detail", 0)
                        else:
                            detail = json_data.get("detail", 0)
                        if detail == 'SIMULATION_LIMIT_EXCEEDED':
                            print(datetime.now(),"Limited by the number of simulations allowed per time")
                            await asyncio.sleep(5)
                            continue  # 继续重试
                        else:
                            print(datetime.now(),"detail: {}, json_data: {}".format(detail, json_data))
                            print(datetime.now(),"Alpha expression is duplicated")
                            await asyncio.sleep(1)
                            return 0  # 表达式重复，直接返回
                    else:
                        print(datetime.now(),'Simulation progress URL: {}'.format(simulation_progress_url))
                        break  # 成功获取进度URL，退出重试循环
            except Exception as e:
                retry_count += 1
                print(datetime.now(),"Error occurred (attempt {}/{}): {}".format(retry_count, max_retries, e))
                if retry_count >= max_retries:
                    print(datetime.now(),"Max retries reached, aborting...")
                    return 1  # 达到最大重试次数，返回错误
                await asyncio.sleep(60)


        # 进度检查循环优化
        max_progress_retries = 10  # 最大重试次数
        progress_retry_count = 0
        while progress_retry_count < max_progress_retries:
            try:
                async with session_manager.session.get(simulation_progress_url) as resp:
                    json_data = await resp.json()
                    # 获取响应头
                    headers = resp.headers
                    retry_after = headers.get('Retry-After', 0)
                    if retry_after == 0:
                        status = json_data.get("status", 0)
                        children = json_data.get("children", [])
                        if status == 'ERROR':
                            print(datetime.now(),"Error in simulation: {}".format(simulation_progress_url))
                        elif status != "COMPLETE":
                            print(datetime.now(),"Simulation not complete: {}".format(simulation_progress_url))
                            async with session_manager.session.delete(simulation_progress_url) as delete_resp:
                                delete_json_data = await delete_resp.json()
                                if delete_json_data.get("detail", 0) == "未找到。":
                                    print(datetime.now(),"Successfully deleted: {}".format(simulation_progress_url))
                                else:
                                    print(datetime.now(),"Failed to delete: {}".format(simulation_progress_url))
                        else:
                            print(datetime.now(),'Simulation completed: {}'.format(simulation_progress_url))
                        break
                    await asyncio.sleep(float(retry_after))
            except Exception as e:
                progress_retry_count += 1
                print(datetime.now(),"Progress check error (attempt {}/{}): {}". format(progress_retry_count, max_progress_retries, str(e)))
                if progress_retry_count >= max_progress_retries:
                    print(datetime.now(),"Max progress check retries reached")
                    return 2  # 新增错误码
                await asyncio.sleep(30)  # 平方退避

        # alpha_id = simulation_progress.json()["alpha"]
        children_list = []
        for child in children:
            try:
                async with session_manager.session.get(brain_api_url + "/simulations/" + child) as child_progress:
                    json_data = await child_progress.json()
                    alpha_id = json_data["alpha"]
                    alpha_express = json_data["regular"]

                    await async_set_alpha_properties(session_manager.session,
                                                     alpha_id,
                                                     name="%s" % name,
                                                     description="""Idea: 11111111111111111111111111111111.
Rationale for data used: 22222222222222222222222222222222222222.
Rationale for operators used: 33333333333333333333333333333333333333.""",
                                                     color=None,
                                                     tags=tags)

                    # 将alpha保存到文件
                    async with aiofiles.open(f'records/{name}_simulated_alpha_expression.txt', mode='a') as f:
                        await f.write(alpha_express + '\n')

            except KeyError:
                print(datetime.now(),"Failed to retrieve alpha ID for: {}".format(brain_api_url + "/simulations/" + child))
            except Exception as e:
                print(datetime.now(),"An error occurred while setting alpha properties:" + str(e))

        return 0

def prune(next_alpha_recs, prefix, keep_num):
    output = []
    num_dict = defaultdict(int)
    for rec in next_alpha_recs:
        exp = rec[1]
        field = exp.split(prefix)[-1].split(",")[0]
        if num_dict[field] < keep_num:
            num_dict[field] += 1
            decay = rec[-1]
            exp = rec[1]
            output.append([exp, decay])
    return output

async def simulate_multiple_tasks(alpha_list, region_list, decay_list, delay_list, name, neut, stone_bag, n=10):
    semaphore = asyncio.Semaphore(n)
    tasks = []
    tags = [name]

    session = await async_login()
    session_start_time = time.time()
    session_expiry_time = 3 * 60 * 60  # 3小时
    session_manager = SessionManager(session, session_start_time, session_expiry_time)

    if region_list[0][0] == "GLB":
        alpha_list = [alpha_list[i:i + 5] for i in range(0, len(alpha_list), 5)]
    else:
        alpha_list = [alpha_list[i:i + 10] for i in range(0, len(alpha_list), 10)]

    # 将任务划分成 n 份
    chunk_size = (len(alpha_list) + n - 1) // n  # 向上取整
    task_chunks = [alpha_list[i:i + chunk_size] for i in range(0, len(alpha_list), chunk_size)]
    region_chunks = [region_list[i:i + chunk_size] for i in range(0, len(region_list), chunk_size)]
    decay_chunks = [decay_list[i:i + chunk_size] for i in range(0, len(decay_list), chunk_size)]
    delay_chunks = [delay_list[i:i + chunk_size] for i in range(0, len(delay_list), chunk_size)]

    for i, (alpha_chunks, region_chunk, decay_chunk, delay_chunk) in enumerate(
            zip(task_chunks, region_chunks, decay_chunks, delay_chunks)):
        # 获取当前 chunk 对应的 session_manager
        current_session_manager = session_manager
        for alpha_chunk, region, decay, delay in zip(alpha_chunks, region_chunk, decay_chunk, delay_chunk):
            # 将任务与当前的 session_manager 关联
            task = simulate_multi(current_session_manager, alpha_chunk, region, name, neut, decay, delay, stone_bag,
                                  tags, semaphore)
            tasks.append(task)

    try:
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=6*60*60)  # 改为6小时与注释一致
    except asyncio.TimeoutError:
        print(datetime.now(),"Task group timed out after 6 hours")
    finally:  # 添加finally块确保资源释放
        try:
            await session_manager.session.close()
        except Exception as e:
            print(datetime.now(),f"Error closing session: {str(e)}")


def read_completed_alphas(filepath):
    """
    从指定文件中读取已经完成的alpha表达式
    """
    completed_alphas = set()
    try:
        with open(filepath, mode='r') as f:
            for line in f:
                completed_alphas.add(line.strip())
    except FileNotFoundError:
        print(datetime.now(),f"File not found: {filepath}")
    return completed_alphas
