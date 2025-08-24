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

 def locate_alpha(self, alpha_id):
        alpha = self.sess.get("https://api.worldquantbrain.com/alphas/" + alpha_id)
        string = alpha.content.decode('utf-8')
        metrics = json.loads(string)
        
        sharpe = metrics["is"]["sharpe"]
        turnover = metrics["is"]["turnover"]
        fitness = metrics["is"]["fitness"]
        returns=metrics["is"]["returns"]
        drawdown=metrics["is"]["drawdown"]
        margin = metrics["is"]["margin"]
        settings=str(metrics['settings'])
        
        triple = [sharpe, turnover,fitness,returns,drawdown,margin,settings]
        triple = [ i if i != 'None' else None for i in triple]
        return triple

    def get_corr(self, alpha_id):
        start_time = time.time()
        timeout = 30

        while True:
            corr_respond = self.sess.get(f"https://api.worldquantbrain.com/alphas/{alpha_id}/correlations/self")
            corr = corr_respond.content.decode('utf-8')
            if corr:
                corr = json.loads(corr)
                if corr.get('min'):
                    min_corr = corr['min']
                    max_corr = corr['max']
                    return [min_corr, max_corr]

            if time.time() - start_time > timeout:
                return [None, None]

            sleep(5)

    def get_score(self, alpha_id):
        start_time = time.time()
        timeout = 30

        while True:
            performance_response = self.sess.get(f'https://api.worldquantbrain.com/competitions/IQC2025S2/alphas/{alpha_id}/before-and-after-performance')
            performance = performance_response.content.decode('utf-8')
            if performance:
                performance = json.loads(performance)
                if performance.get('score'):
                    before_score = performance['score']['before']
                    after_score = performance['score']['after']
                    score = after_score - before_score
                    return [score]

            if time.time() - start_time > timeout:
                return [None]

            sleep(5)

    def get_pl(self, alpha_id):
        while True:
            pl_obj = self.sess.get(f'https://api.worldquantbrain.com/alphas/{alpha_id}/recordsets/pnl')
            if pl_obj.content:
                pl = pl_obj.json()
                pl = pl.get('records')
                pl_df = pd.DataFrame(pl, columns=['date', 'returns'])
                pl_df['returns'] = pl_df['returns'] - pl_df['returns'].shift(1)
                pl_df.dropna(inplace=True)
                return pl_df

    def get_turnover(self, alpha_id):
        while True:
            turnover_obj = self.sess.get(f'https://api.worldquantbrain.com/alphas/{alpha_id}/recordsets/turnover')
            if turnover_obj.content:
                turnover = turnover_obj.json()
                turnover = turnover.get('records')
                turnover_df = pd.DataFrame(turnover, columns=['date', 'turnover'])
                turnover_df.dropna(inplace=True)
                return turnover_df
