import os
from time import sleep
import requests
from requests.auth import HTTPBasicAuth

# 加载凭据文件
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

# 向 API 发送 POST 请求进行身份验证
response = sess.post("https://api.worldquantbrain.com/authentication")

# 打印响应状态和内容以调试
print("response.status_code:", response.status_code)
print("response.json:", response.json())

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
    "regular": "liabilities/assets",  # alphas 表达式
}
print("simulation_data:", simulation_data)

sim_resp = sess.post(
    "https://api.worldquantbrain.com/simulations",
    json=simulation_data,
)
print("sim_resp.headers:", sim_resp.headers)
# sim_resp.headers: {
#     'Date': 'Tue, 03 Jun 2025 07:28:30 GMT',
#     'Content-Type': 'text/html; charset=UTF-8',
#     'Content-Length': '0',
#     'Connection': 'keep-alive',
#     'Retry-After': '2.5',
#     'Location': 'https://api.worldquantbrain.com/simulations/4bwfuq11x4GL9NTMruMHAO1',
#     'Allow': 'POST, OPTIONS',
#     'X-Request-Id': '9891fe734d05476ab504b72535c04d3f',
#     'X-Frame-Options': 'SAMEORIGIN',
#     'Vary': 'Accept-Language, Cookie, Origin',
#     'Content-Language': 'en',
#     'Access-Control-Allow-Origin': 'https://platform.worldquantbrain.com',
#     'Access-Control-Allow-Credentials': 'true',
#     'Access-Control-Expose-Headers': 'Location,Retry-After',
#     'Strict-Transport-Security': 'max-age=31536000; includeSubDomains'
# }
sim_progress_url = sim_resp.headers["Location"]

while True:
    sim_progress_resp = sess.get(sim_progress_url)
    print("sim_progress_resp:", sim_progress_resp)

    retry_after_sec = float(sim_progress_resp.headers.get("Retry-After", 0))
    print("retry_after_sec:", retry_after_sec)

    if retry_after_sec == 0:  # simulation done!
        break
    sleep(retry_after_sec)

print("sim_progress_resp.json:", sim_progress_resp.json())
# sim_progress_resp.json: {
#     'id': '4bwfuq11x4GL9NTMruMHAO1',
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
#         'maxTrade': 'OFF',
#         'language': 'FASTEXPR',
#         'visualization': False
#     },
#     'regular': 'liabilities/assets',
#     'status': 'COMPLETE',
#     'alpha': 'EN6YodG'
# }

# the final simulation result
alpha_id = sim_progress_resp.json()["alpha"]

print("alpha_id:", alpha_id)
