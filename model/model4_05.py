import os
from time import sleep
import requests
from requests.auth import HTTPBasicAuth


def sign_in():
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
        data = sess.get(url_template.format(x=0)).json()
    else:
        url_template = (
            "https://api.worldquantbrain.com/data-fields?"
            + f"&instrumentType={instrument_type}"
            + f"&region={region}&delay={str(delay)}&universe={universe}&limit=50"
            + f"&search={search}"
            + "&offset={x}"
        )

    datafields_list = []
    # 0 50 100 150 200 250 300 350 400 450 500 550 600 650 700 750 800 850
    for x in range(30, 75, 50):
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
fundamental6 = get_datafields(sess=sess, searchScope=searchScope, dataset_id="option9")

# 筛选(这里是 type 的 MATRIX)
fundamental6 = fundamental6[fundamental6["type"] == "MATRIX"]
# fundamental6.head()

datafields_list_fundamental6 = fundamental6["id"].values
print("datafields_list_fundamental6:", datafields_list_fundamental6)
print("len(datafields_list_fundamental6):", len(datafields_list_fundamental6))

"""
运行 world2.py 生成 574 个 alpha, 其中获得几个可提交的 alpha.
修改 world2.py 的模版运行可获得更多可提交的alpha, 比如把 cap 换成
assets / liabilities / revenue / sales / debt / ebit / ebitda / equity / intpn / capex
之类的数据字段变化模版, 还可以尝试在表达式前面加负号变化模版寻找更多的 Alpha.
"""
economics = [
    "assets",
    "liabilities",
    "revenue",
    "sales",
    "debt",
    "ebit",
    "ebitda",
    "equity",
    "intpn",
    "capex",
    "cap",
]
symbols = ["", "-"]

index = 0
for datafield in datafields_list_fundamental6:
    for economic in economics:
        for symbol in symbols:
            if index % 100 == 0:
                sess = sign_in()
                print(f"重新登录, 当前 index 为{index}")
            index += 1
            alpha_expression = (
                f"{symbol}group_rank(({datafield})/{economic}, subindustry)"
            )
            print(f"组装 alpha 表达式: {alpha_expression}")
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
            sim_resp = sess.post(
                "https://api.worldquantbrain.com/simulations",
                json=simulation_data,
            )
            print("sim_resp.status_code:", sim_resp.status_code)
            sleep(15)
