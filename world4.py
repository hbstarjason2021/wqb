import requests
import json
from os.path import expanduser
from requests.auth import HTTPBasicAuth


def sign_in():
    # Load credentials # 加载凭证
    with open(expanduser('brain_credentials.txt')) as f:
        credentials = json.load(f)

    # Extract username and password from the list # 从列表中提取用户名和密码
    username, password = credentials

    # Create a session object # 创建会话对象
    sess = requests.Session()

    # Set up basic authentication # 设置基本身份验证
    sess.auth = HTTPBasicAuth(username, password)

    # Send a POST request to the API for authentication # 向API发送POST请求进行身份验证
    response = sess.post('https://api.worldquantbrain.com/authentication')

    # Print response status and content for debugging # 打印响应状态和内容以调试
    print(response.status_code)
    print(response.json())
    return sess


sess = sign_in()

# 获取数据集ID为fundamental6（Company Fundamental Data for Equity）下的所有数据字段
### Get Data_fields like Data Explorer 获取所有满足条件的数据字段及其ID
def get_datafields(
        s,
        searchScope,
        dataset_id: str = '',
        search: str = ''
):
    import pandas as pd
    instrument_type = searchScope['instrumentType']
    region = searchScope['region']
    delay = searchScope['delay']
    universe = searchScope['universe']

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

# 爬取id
searchScope = {'region': 'USA', 'delay': '1', 'universe': 'TOP3000', 'instrumentType': 'EQUITY'}
fundamental6 = get_datafields(s=sess, searchScope=searchScope, dataset_id='fundamental6') # id设置

# 筛选（这里是type的MATRIX）
fundamental6 = fundamental6[fundamental6['type'] == "MATRIX"]
fundamental6.head()

datafields_list_fundamental6 = fundamental6['id'].values
print(datafields_list_fundamental6)
print(len(datafields_list_fundamental6))

size_fields = ['assets','debt','liabilities','revenue','sales','ebit','ebitda','equity','capex','fnd6_intpn']

# 将datafield替换到Alpha模板(框架)中group_rank({fundamental model data}/{size_fields},subindustry)批量生成Alpha
alpha_list = []

for index,datafield in enumerate(datafields_list_fundamental6,start=1):
    for size_field in size_fields:
        alpha_expression = f'group_rank({datafield}/{size_field}, subindustry)'
        print(f"正在循环组装alpha表达式: {alpha_expression}")
        simulation_data = {
            "type": "REGULAR",
            "settings": {
                "instrumentType": "EQUITY",
                "region": "USA",
                "universe": "TOP3000",
                "delay": 1,
                "decay": 0,
                "neutralization": "SUBINDUSTRY",
                "truncation": 0.08,
                "pasteurization": "ON",
                "unitHandling": "VERIFY",
                "nanHandling": "ON",
                "language": "FASTEXPR",
                "visualization": False,
            },
            "regular": alpha_expression
        }
        alpha_list.append(simulation_data)

print(f"there are {len(alpha_list)} Alphas to simulate")
#print(alpha_list[0])

# 将Alpha列表里的所有alpha存入csv文件。headers of the csv：type,settings,regular
import csv
import os

# Check if the file exists
alpha_list_file_path = 'alpha_list_pending_simulated.csv'  # replace with your actual file path
file_exists = os.path.isfile(alpha_list_file_path)

# Write the list of dictionaries to a CSV file, when append keep the original header
with open(alpha_list_file_path, 'a', newline='') as output_file:
    dict_writer = csv.DictWriter(output_file, fieldnames=['type', 'settings', 'regular'])
    # If the file does not exist, write the header
    if not file_exists:
        dict_writer.writeheader()

    dict_writer.writerows(alpha_list)

print("Alpha list has been saved to alpha_list_pending_simulated.csv")
print(f"Total {len(alpha_list)} Alphas have been saved to the file")
print("Please run AlphaSimulator.py to simulate the Alphas")

