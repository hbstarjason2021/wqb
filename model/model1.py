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


# Get Data_fields like Data Explorer
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
        # print("url_template:", url_template)
        # url_template: https://api.worldquantbrain.com/data-fields?
        # &instrumentType=EQUITY
        # &region=USA
        # &delay=1
        # &universe=TOP3000
        # &dataset.id=fundamental6
        # &limit=50
        # &offset={x}

        # print("url_template.format(x=0):", url_template.format(x=0))
        # url_template.format(x=0): https://api.worldquantbrain.com/data-fields?
        # &instrumentType=EQUITY
        # &region=USA
        # &delay=1
        # &universe=TOP3000
        # &dataset.id=fundamental6
        # &limit=50
        # &offset=0

        data = sess.get(url_template.format(x=0)).json()
        # print("data1:", data)
        count = data["count"]
    else:
        url_template = (
            "https://api.worldquantbrain.com/data-fields?"
            + f"&instrumentType={instrument_type}"
            + f"&region={region}&delay={str(delay)}&universe={universe}&limit=50"
            + f"&search={search}"
            + "&offset={x}"
        )
        count = 100

    print("count:", count)

    datafields_list = []
    # 0 50 100 150 200 250 300 350 400 450 500 550 600 650 700 750 800 850
    for x in range(0, 400, 50):
        datafields = sess.get(url_template.format(x=x))
        # print("data2:", datafields.json())

        datafields_list.append(datafields.json()["results"])

    # print("datafields_list:", datafields_list)
    datafields_list_flat = [item for sublist in datafields_list for item in sublist]
    # print("datafields_list_flat:", datafields_list_flat)

    datafields_df = pd.DataFrame(datafields_list_flat)
    return datafields_df


# 爬取 id
searchScope = {
    "instrumentType": "EQUITY",
    "region": "USA",
    "delay": "1",
    "universe": "TOP3000",
}
# 获取数据集 ID 为 fundamental6(Company Fundamental Data for Equity) 下的所有数据字段
fundamental6 = get_datafields(sess=sess, searchScope=searchScope, dataset_id="option8")

# 筛选(这里是 type 的 MATRIX)
fundamental6 = fundamental6[fundamental6["type"] == "MATRIX"]
# fundamental6.head()

datafields_list_fundamental6 = fundamental6["id"].values
print("datafields_list_fundamental6:", datafields_list_fundamental6)
print("len(datafields_list_fundamental6):", len(datafields_list_fundamental6))


# 将 datafield 替换到 Alpha 模板(框架)中 group_rank({fundamental model data}/cap, subindustry) 批量生成 Alpha
alpha_list = []
for index, datafield in enumerate(datafields_list_fundamental6, start=1):
    print("index:", index)
    print("datafield:", datafield)
    alpha_expression = f"group_rank(({datafield})/cap, subindustry)"
    print(f"正在循环第 {index} 个元素, 组装 alpha 表达式: {alpha_expression}")
    simulation_data = {
        "type": "REGULAR",
        "settings": {
            "instrumentType": "EQUITY",
            "region": "USA",
            "universe": "TOP3000",
            "delay": 1,
            "decay": 6,
            "neutralization": "SUBINDUSTRY",
            "truncation": 0.08,
            "pasteurization": "ON",
            "unitHandling": "VERIFY",
            "nanHandling": "ON",
            "language": "FASTEXPR",
            "visualization": False,
        },
        "regular": alpha_expression,
    }
    if index % 50 == 0:
        sess = sign_in()
        print(f"重新登录, 当前 index 为{index}")

    sim_resp = sess.post(
        "https://api.worldquantbrain.com/simulations",
        json=simulation_data,
    )
    print("sim_resp.status_code:", sim_resp.status_code)
    sleep(15)
    # alpha_list.append(simulation_data)

# print(f"there are {len(alpha_list)} Alphas to simulate")
# print(alpha_list[0])
# there are 574 Alphas to simulate
# {
#     'type': 'REGULAR',
#     'settings': {
#         'instrumentType': 'EQUITY',
#         'region': 'USA',
#         'universe': 'TOP3000',
#         'delay': 1,
#         'decay': 6,
#         'neutralization': 'SUBINDUSTRY',
#         'truncation': 0.08,
#         'pasteurization': 'ON',
#         'unitHandling': 'VERIFY',
#         'nanHandling': 'ON',
#         'language': 'FASTEXPR',
#         'visualization': False
#     },
#     'regular': 'group_rank((assets)/cap, subindustry)'
# }

# 将 Alpha 一个一个发送至服务器进行回测, 并检查是否断线, 如断线则重连, 并继续发送
# for index, alpha in enumerate(alpha_list, start=1):
#     print("index:", index)
#     print("alpha:", alpha["regular"])
#     if index % 50 == 0:
#         sess = sign_in()
#         print(f"重新登录, 当前 index 为{index}")

#     sim_resp = sess.post(
#         "https://api.worldquantbrain.com/simulations",
#         json=alpha,
#     )
#     print("sim_resp.status_code:", sim_resp.status_code)

#     try:
#         sim_progress_url = sim_resp.headers["Location"]
#         while True:
#             sim_progress_resp = sess.get(sim_progress_url)
#             retry_after_sec = float(sim_progress_resp.headers.get("Retry-After", 0))
#             if retry_after_sec == 0:  # simulation done!
#                 break
#             sleep(retry_after_sec)
#         # the final simulation result.
#         alpha_id = sim_progress_resp.json()["alpha"]
#         print(f"{index}: {alpha_id}: {alpha['regular']}")
#         print(alpha_id)
#     except:
#         print("no location, sleep for 10 seconds and try next alpha.")
#         sleep(10)
