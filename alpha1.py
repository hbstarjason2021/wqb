import os

# from time import sleep
import requests

# import json
# from os.path import expanduser
from requests.auth import HTTPBasicAuth

# # 加载凭据文件
# with open(expanduser('brain_credentials.txt')) as f:
#     credentials = json.load(f)

# 从列表中提取用户名和密码
# username, password = credentials
username = os.getenv("username")
password = os.getenv("password")
print("username:", username)
print("password:", password)

# 创建会话对象
sess = requests.Session()

# 设置基本身份验证
sess.auth = HTTPBasicAuth(username, password)

# 向API发送POST请求进行身份验证
response = sess.post("https://api.worldquantbrain.com/authentication")

# 打印响应状态和内容以调试
print("response.status_code:", response.status_code)
print("response.json:", response.json())


sim_resp = sess.get("https://api.worldquantbrain.com/alphas/w3o2Z8l")

print("sim_resp.status_code:", sim_resp.status_code)
print("sim_resp.json:", sim_resp.json())
# {
#     'id': 'w3o2Z8l',
#     'type': 'REGULAR',
#     'author': 'ZH14406',
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
#         'maxTrade': 'OFF',
#         'language': 'FASTEXPR',
#         'visualization': False
#     },
#     'regular': {
#         'code': 'group_rank((fnd6_dilavx)/cap, subindustry)', 'description': None, 'operatorCount': 2
#     },
#     'dateCreated': '2025-06-03T13:49:01-04:00',
#     'dateSubmitted': None,
#     'dateModified': '2025-06-03T13:49:02-04:00',
#     'name': None,
#     'favorite': False,
#     'hidden': False,
#     'color': None,
#     'category': None,
#     'tags': [],
#     'classifications': [],
#     'grade': 'INFERIOR',
#     'stage': 'IS',
#     'status': 'UNSUBMITTED',
#     'is': {
#         'pnl': 2235675,
#         'bookSize': 20000000,
#         'longCount': 1495,
#         'shortCount': 1562,
#         'turnover': 0.0217,
#         'returns': 0.0452,
#         'drawdown': 0.212,
#         'margin': 0.004161,
#         'sharpe': 0.54,
#         'fitness': 0.32,
#         'startDate': '2018-01-20',
#         'checks': [
#             {'name': 'LOW_SHARPE', 'result': 'FAIL', 'limit': 1.25, 'value': 0.54},
#             {'name': 'LOW_FITNESS', 'result': 'FAIL', 'limit': 1.0, 'value': 0.32},
#             {'name': 'LOW_TURNOVER', 'result': 'PASS', 'limit': 0.01, 'value': 0.0217},
#             {'name': 'HIGH_TURNOVER', 'result': 'PASS', 'limit': 0.7, 'value': 0.0217},
#             {'name': 'CONCENTRATED_WEIGHT', 'result': 'PASS'},
#             {'name': 'LOW_SUB_UNIVERSE_SHARPE', 'result': 'PASS', 'limit': 0.23, 'value': 0.44},
#             {'name': 'SELF_CORRELATION', 'result': 'PENDING'},
#             {
#                 'name': 'MATCHES_COMPETITION',
#                 'result': 'PASS',
#                 'competitions': [
#                     {'id': 'challenge', 'name': 'Challenge'}
#                 ]
#             }
#         ]
#     },
#     'os': None,
#     'train': None,
#     'test': None,
#     'prod': None,
#     'competitions': None,
#     'themes': None,
#     'pyramids': None,
#     'pyramidThemes': None,
#     'team': None
# }
