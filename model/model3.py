import os
from time import sleep
import requests
from requests.auth import HTTPBasicAuth


def sign_in():
    # Load credentials
    # with open(expanduser('brain_credentials.txt')) as f:
    #     credentials = json.load(f)

    # Extract username and password from the list
    # username, password = credentials
    username = os.getenv("username")
    password = os.getenv("password")
    print("username:", username)
    print("password:", password)

    # Create a session object
    sess = requests.Session()

    # Set up basic authentication
    sess.auth = HTTPBasicAuth(username, password)

    # Send a POST request to the API for authentication
    response = sess.post("https://api.worldquantbrain.com/authentication")

    # Print response status and content for debugging
    print("response.status_code:", response.status_code)
    print("response.json:", response.json())
    return sess


sess = sign_in()


# 获取数据集ID为fundamental6(Company Fundamental Data for Equity)下的所有数据字段
### Get Data_fields like Data Explorer 获取所有满足条件的数据字段及其ID
def get_datafields(sess, searchScope, dataset_id: str = "", search: str = ""):
    import pandas as pd

    instrument_type = searchScope["instrumentType"]
    region = searchScope["region"]
    delay = searchScope["delay"]
    universe = searchScope["universe"]

    if len(search) == 0:
        url_template = (
            "https://api.worldquantbrain.com/data-fields?"
            + f"&instrumentType={instrument_type}"
            + f"&region={region}&delay={str(delay)}&universe={universe}&dataset.id={dataset_id}&limit=50"
            + "&offset={x}"
        )
        # count = sess.get(url_template.format(x=0)).json()["count"]
    else:
        url_template = (
            "https://api.worldquantbrain.com/data-fields?"
            + f"&instrumentType={instrument_type}"
            + f"&region={region}&delay={str(delay)}&universe={universe}&limit=50"
            + f"&search={search}"
            + "&offset={x}"
        )
        # count = 100

    datafields_list = []
    for x in range(0, 400, 50):
        datafields = sess.get(url_template.format(x=x))
        datafields_list.append(datafields.json()["results"])

    datafields_list_flat = [item for sublist in datafields_list for item in sublist]

    datafields_df = pd.DataFrame(datafields_list_flat)
    return datafields_df


# 定义搜索范围
searchScope = {
    "instrumentType": "EQUITY",
    "region": "USA",
    "delay": "1",
    "universe": "TOP3000",
}
# 从数据集中获取数据字段
fnd6 = get_datafields(sess=sess, searchScope=searchScope, dataset_id="option8")
# 过滤类型为 "MATRIX" 的数据字段
fnd6 = fnd6[fnd6["type"] == "MATRIX"]
# 提取数据字段的ID并转换为列表
datafields_list_fnd6 = fnd6["id"].values
# 输出数据字段的ID列表
print("datafields_list_fnd6:", datafields_list_fnd6)
print("len(datafields_list_fnd6):", len(datafields_list_fnd6))

# 模板
# <group_compare_op>(<ts_compare_op>(<op>(<company_fundamentals>)/<op>(enterprise_value),<days>),<group>)
# 定义分组比较操作符
group_compare_op = ["group_neutralize"]
# 定义时间序列比较操作符
ts_compare_op = ["ts_rank", "ts_zscore"]  # 时间序列比较操作符列表
# 定义Cross Sectional操作符
cross_sectional_op = ["rank"]
# 定义公司基本面数据的字段列表
company_fundamentals = datafields_list_fnd6
# 定义时间周期列表
days = [10, 20]
# 定义分组依据列表
group = ["industry"]
# 初始化alpha表达式列表
# alpha_expressions = []

index = 0
for gco in group_compare_op:
    for tco in ts_compare_op:
        for cso in cross_sectional_op:
            for cf in company_fundamentals:
                for d in days:
                    for grp in group:
                        if index % 100 == 0:
                            sess = sign_in()
                            print(f"重新登录, 当前 index 为{index}")
                        index += 1
                        alpha_expression = f"{gco}({tco}({cso}({cf})/{cso}(enterprise_value), {d}), {grp})"
                        print("alpha_expression:", alpha_expression)
                        # alpha_expressions.append(
                        #     f"{gco}({tco}({cso}({cf})/{cso}(enterprise_value), {d}), {grp})"
                        # )
                        simulation_data = {
                            "type": "REGULAR",
                            "settings": {
                                "instrumentType": "EQUITY",
                                "region": "USA",
                                "universe": "TOP3000",
                                "delay": 1,
                                "decay": 6,
                                "neutralization": "SUBINDUSTRY",
                                "truncation": 0.01,
                                "pasteurization": "ON",
                                "unitHandling": "VERIFY",
                                "nanHandling": "ON",
                                "language": "FASTEXPR",
                                "visualization": False,
                            },
                            "regular": alpha_expression,
                        }
                        sim_resp = sess.post(
                            "https://api.worldquantbrain.com/simulations",
                            json=simulation_data,
                        )
                        print("sim_resp.status_code:", sim_resp.status_code)
                        sleep(15)

# 输出生成的alpha表达式总数 # 打印或返回结果字符串列表
# print(f"there are total {len(alpha_expressions)} alpha expressions")

# 打印结果
# print(alpha_expressions[:5])
# print(len(alpha_expressions))

# 将datafield替换到Alpha模板(框架)中group_rank({fundamental model data}/cap,subindustry)批量生成Alpha
# alpha_list = []

# print("将alpha表达式与setting封装")
# for index, alpha_expression in enumerate(alpha_expressions, start=1):
#     print(f"正在循环第 {index} 个元素,组装alpha表达式: {alpha_expression}")
#     simulation_data = {
#         "type": "REGULAR",
#         "settings": {
#             "instrumentType": "EQUITY",
#             "region": "USA",
#             "universe": "TOP3000",
#             "delay": 1,
#             "decay": 6,
#             "neutralization": "SUBINDUSTRY",
#             "truncation": 0.01,
#             "pasteurization": "ON",
#             "unitHandling": "VERIFY",
#             "nanHandling": "ON",
#             "language": "FASTEXPR",
#             "visualization": False,
#         },
#         "regular": alpha_expression,
#     }
#     alpha_list.append(simulation_data)
# print(f"there are {len(alpha_list)} Alphas to simulate")

# # 输出
# print(alpha_list[0])


# # 在使用该代码前, 需将Course3的Alpha列表里的所有alpha存入csv文件. headers of the csv：type,settings,regular
# import csv
# import os

# # Check if the file exists
# alpha_list_file_path = (
#     "alpha_list_pending_simulated.csv"  # replace with your actual file path
# )
# file_exists = os.path.isfile(alpha_list_file_path)

# # Write the list of dictionaries to a CSV file, when append keep the original header
# with open(alpha_list_file_path, "a", newline="") as output_file:
#     dict_writer = csv.DictWriter(
#         output_file, fieldnames=["type", "settings", "regular"]
#     )
#     # If the file does not exist, write the header
#     if not file_exists:
#         dict_writer.writeheader()

#     dict_writer.writerows(alpha_list)

# print("Alpha list has been saved to alpha_list_pending_simulated.csv")

# # 将Alpha一个一个发送至服务器进行回测,并检查是否断线, 如断线则重连
# ##设置log
# import logging

# # Configure the logging setting
# logging.basicConfig(
#     filename="simulation.log",
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s",
# )


# from time import sleep
# import logging


# alpha_fail_attempt_tolerance = 15  # 每个alpha允许的最大失败尝试次数
# is_submit = False  # 标志变量, 用于控制是否提交alpha
# if is_submit:
#     # 从第0个元素开始迭代回测alpha_list
#     for index in range(0, len(alpha_list)):
#         alpha = alpha_list[index]
#         print(f"{index}: {alpha['regular']}")
#         logging.info(f"{index}: {alpha['regular']}")
#         keep_trying = True  # 控制while循环继续的标志
#         failure_count = 0  # 记录失败尝试次数的计数器

#         while keep_trying:
#             try:
#                 # 尝试发送POST请求
#                 sim_resp = sess.post(
#                     "https://api.worldquantbrain.com/simulations",
#                     json=alpha,  # 将当前alpha(一个JSON)发送到服务器
#                 )

#                 # 从响应头中获取位置
#                 sim_progress_url = sim_resp.headers["Location"]
#                 logging.info(f"Alpha location is: {sim_progress_url}")  # 记录位置
#                 print(f"Alpha location is: {sim_progress_url}")  # 打印位置
#                 keep_trying = False  # 成功获取位置, 退出while循环

#             except Exception as e:
#                 # 处理异常：记录错误, 让程序休眠15秒后重试
#                 logging.error(
#                     f"No Location, sleep 15 and retry, error message: {str(e)}"
#                 )
#                 print("No Location, sleep 15 and retry")
#                 sleep(15)  # 休眠15秒后重试
#                 failure_count += 1  # 增加失败尝试次数

#                 # 检查失败尝试次数是否达到容忍上限
#                 if failure_count >= alpha_fail_attempt_tolerance:
#                     sess = sign_in()  # 重新登录会话
#                     failure_count = 0  # 重置失败尝试次数
#                     logging.error(
#                         f"No location for too many times, move to next alpha {alpha['regular']}"
#                     )  # 记录错误
#                     print(
#                         f"No location for too many times, move to next alpha {alpha['regular']}"
#                     )  # 打印信息
#                     break  # 退出while循环, 移动到for循环中的下一个alpha
