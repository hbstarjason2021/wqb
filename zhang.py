# 登录
import requests
import json
from os.path import expanduser
from requests.auth import HTTPBasicAuth


def sign_in():
    # Load credentials # 加载凭证
    with open(expanduser('credentials.txt')) as f:
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


##########################
def get_datasets(
    s,
    instrument_type: str = 'EQUITY',
    region: str = 'USA',
    delay: int = 1,
    universe: str = 'TOP3000'
):
    url = "https://api.worldquantbrain.com/data-sets?" +\
        f"instrumentType={instrument_type}&region={region}&delay={str(delay)}&universe={universe}"
    result = s.get(url)
    datasets_df = pd.DataFrame(result.json()['results'])
    return datasets_df



##########################

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


# 定义搜索范围
searchScope = {'region': 'GLB', 'delay': '1', 'universe': 'TOP3000', 'instrumentType': 'EQUITY'}
# 从数据集中获取数据字段
fnd6 = get_datafields(s=sess, searchScope=searchScope, dataset_id='other450')

print(fnd6.columns.tolist())

# 过滤类型为 "MATRIX" 的数据字段
fnd6 = fnd6[fnd6['type'] == "MATRIX"]
# 提取数据字段的ID并转换为列表
datafields_list_fnd6 = fnd6['id'].values
# 输出数据字段的ID列表
print(datafields_list_fnd6)
print(len(datafields_list_fnd6))


### df = get_datafields(s, dataset_id = 'analyst4', region='USA', universe='TOP3000', delay=1)
### df

############# 

def get_ops(s):
    # s=s_ll
    # 获取ops
    url="https://api.worldquantbrain.com/operators"
    res=s.get(url)
    # print(res.json())
    df = pd.DataFrame(res.json())
    return df

# 获取账号可用的ops
ops_lst=get_ops(s)['name'].to_list()   
ops=intersection_of_lists(ops_lst,ts_ops)


############
