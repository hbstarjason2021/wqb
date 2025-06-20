import os
from time import sleep
import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import JSONDecodeError
import pandas as pd


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
    print("authentication response.status_code:", response.status_code)
    print("authentication response.json:", response.json())
    return sess


def get_alphas(sess, limit, offset):
    # limit = 100
    # offset = 2900
    url = f"https://api.worldquantbrain.com/users/self/alphas?limit={limit}&offset={offset}&status=UNSUBMITTED"
    print("get_alphas url:", url)
    sim_resp = sess.get(url)
    print("get_alphas sim_resp.status_code:", sim_resp.status_code)
    # print("sim_resp.json:", sim_resp.json())
    # {
    #     'count': 571,
    #     'next': 'http://api.worldquantbrain.com:443/users/self/alphas?limit=50&offset=50&status=UNSUBMITTED',
    #     'previous': None,
    #     'results': [
    #         {'id': 'xVWpJpp', 'type': 'REGULAR', 'author': 'ZH14406', 'settings': {'instrumentType': 'EQUITY', 'region': 'USA', 'universe': 'TOP3000', 'delay': 1, 'decay': 6, 'neutralization': 'SUBINDUSTRY', 'truncation': 0.01, 'pasteurization': 'ON', 'unitHandling': 'VERIFY', 'nanHandling': 'ON', 'maxTrade': 'OFF', 'language': 'FASTEXPR', 'visualization': False}, 'regular': {'code': 'group_neutralize(ts_rank(rank(debt_st)/rank(enterprise_value), 10), industry)', 'description': None, 'operatorCount': 5}, 'dateCreated': '2025-06-05T04:10:29-04:00', 'dateSubmitted': None, 'dateModified': '2025-06-05T04:10:30-04:00', 'name': None, 'favorite': False, 'hidden': False, 'color': None, 'category': None, 'tags': [], 'classifications': [{'id': 'DATA_USAGE:SINGLE_DATA_SET', 'name': 'Single Data Set Alpha'}], 'grade': 'INFERIOR', 'stage': 'IS', 'status': 'UNSUBMITTED', 'is': {'pnl': 4532492, 'bookSize': 20000000, 'longCount': 1053, 'shortCount': 1058, 'turnover': 0.3298, 'returns': 0.0916, 'drawdown': 0.0853, 'margin': 0.000555, 'sharpe': 1.16, 'fitness': 0.61, 'startDate': '2018-01-20', 'checks': [{'name': 'LOW_SHARPE', 'result': 'FAIL', 'limit': 1.25, 'value': 1.16}, {'name': 'LOW_FITNESS', 'result': 'FAIL', 'limit': 1.0, 'value': 0.61}, {'name': 'LOW_TURNOVER', 'result': 'PASS', 'limit': 0.01, 'value': 0.3298}, {'name': 'HIGH_TURNOVER', 'result': 'PASS', 'limit': 0.7, 'value': 0.3298}, {'name': 'CONCENTRATED_WEIGHT', 'result': 'PASS'}, {'name': 'LOW_SUB_UNIVERSE_SHARPE', 'result': 'PASS', 'limit': 0.5, 'value': 0.78}, {'name': 'SELF_CORRELATION', 'result': 'PENDING'}, {'name': 'MATCHES_COMPETITION', 'result': 'PASS', 'competitions': [{'id': 'challenge', 'name': 'Challenge'}]}]}, 'os': None, 'train': None, 'test': None, 'prod': None, 'competitions': None, 'themes': None, 'pyramids': None, 'pyramidThemes': None, 'team': None},
    #         {'id': 'gXdVnrJ', 'type': 'REGULAR', 'author': 'ZH14406', 'settings': {'instrumentType': 'EQUITY', 'region': 'USA', 'universe': 'TOP3000', 'delay': 1, 'decay': 6, 'neutralization': 'SUBINDUSTRY', 'truncation': 0.08, 'pasteurization': 'ON', 'unitHandling': 'VERIFY', 'nanHandling': 'ON', 'maxTrade': 'OFF', 'language': 'FASTEXPR', 'visualization': False}, 'regular': {'code': 'group_rank((fnd6_pnrsho)/cap, subindustry)', 'description': None, 'operatorCount': 2}, 'dateCreated': '2025-06-05T04:10:27-04:00', 'dateSubmitted': None, 'dateModified': '2025-06-05T04:10:27-04:00', 'name': None, 'favorite': False, 'hidden': False, 'color': None, 'category': None, 'tags': [], 'classifications': [], 'grade': 'AVERAGE', 'stage': 'IS', 'status': 'UNSUBMITTED', 'is': {'pnl': 3351787, 'bookSize': 20000000, 'longCount': 1361, 'shortCount': 1514, 'turnover': 0.0203, 'returns': 0.0677, 'drawdown': 0.0405, 'margin': 0.006678, 'sharpe': 1.64, 'fitness': 1.21, 'startDate': '2018-01-20', 'checks': [{'name': 'LOW_SHARPE', 'result': 'PASS', 'limit': 1.25, 'value': 1.64}, {'name': 'LOW_FITNESS', 'result': 'PASS', 'limit': 1.0, 'value': 1.21}, {'name': 'LOW_TURNOVER', 'result': 'PASS', 'limit': 0.01, 'value': 0.0203}, {'name': 'HIGH_TURNOVER', 'result': 'PASS', 'limit': 0.7, 'value': 0.0203}, {'name': 'CONCENTRATED_WEIGHT', 'result': 'PASS'}, {'name': 'LOW_SUB_UNIVERSE_SHARPE', 'result': 'FAIL', 'limit': 0.71, 'value': -0.12}, {'name': 'SELF_CORRELATION', 'result': 'PENDING'}, {'name': 'MATCHES_COMPETITION', 'result': 'PASS', 'competitions': [{'id': 'challenge', 'name': 'Challenge'}]}]}, 'os': None, 'train': None, 'test': None, 'prod': None, 'competitions': None, 'themes': None, 'pyramids': None, 'pyramidThemes': None, 'team': None},
    #         ...
    #         {'id': '31GdoJ0', 'type': 'REGULAR', 'author': 'ZH14406', 'settings': {'instrumentType': 'EQUITY', 'region': 'USA', 'universe': 'TOP3000', 'delay': 1, 'decay': 6, 'neutralization': 'SUBINDUSTRY', 'truncation': 0.08, 'pasteurization': 'ON', 'unitHandling': 'VERIFY', 'nanHandling': 'ON', 'maxTrade': 'OFF', 'language': 'FASTEXPR', 'visualization': False}, 'regular': {'code': 'group_rank((fnd6_newa2v1300_txach)/cap, subindustry)', 'description': None, 'operatorCount': 2}, 'dateCreated': '2025-06-03T17:02:26-04:00', 'dateSubmitted': None, 'dateModified': '2025-06-03T17:02:26-04:00', 'name': None, 'favorite': False, 'hidden': False, 'color': None, 'category': None, 'tags': [], 'classifications': [], 'grade': 'INFERIOR', 'stage': 'IS', 'status': 'UNSUBMITTED', 'is': {'pnl': -353342, 'bookSize': 20000000, 'longCount': 1409, 'shortCount': 1610, 'turnover': 0.0153, 'returns': -0.0071, 'drawdown': 0.1253, 'margin': -0.000933, 'sharpe': -0.2, 'fitness': -0.05, 'startDate': '2018-01-20', 'checks': [{'name': 'LOW_SHARPE', 'result': 'FAIL', 'limit': 1.25, 'value': -0.2}, {'name': 'LOW_FITNESS', 'result': 'FAIL', 'limit': 1.0, 'value': -0.05}, {'name': 'LOW_TURNOVER', 'result': 'PASS', 'limit': 0.01, 'value': 0.0153}, {'name': 'HIGH_TURNOVER', 'result': 'PASS', 'limit': 0.7, 'value': 0.0153}, {'name': 'CONCENTRATED_WEIGHT', 'result': 'PASS'}, {'name': 'LOW_SUB_UNIVERSE_SHARPE', 'result': 'FAIL', 'limit': -0.09, 'value': -0.51}, {'name': 'SELF_CORRELATION', 'result': 'PENDING'}, {'name': 'MATCHES_COMPETITION', 'result': 'PASS', 'competitions': [{'id': 'challenge', 'name': 'Challenge'}]}]}, 'os': None, 'train': None, 'test': None, 'prod': None, 'competitions': None, 'themes': None, 'pyramids': None, 'pyramidThemes': None, 'team': None},
    #         {'id': 'bJxp99N', 'type': 'REGULAR', 'author': 'ZH14406', 'settings': {'instrumentType': 'EQUITY', 'region': 'USA', 'universe': 'TOP3000', 'delay': 1, 'decay': 6, 'neutralization': 'SUBINDUSTRY', 'truncation': 0.08, 'pasteurization': 'ON', 'unitHandling': 'VERIFY', 'nanHandling': 'ON', 'maxTrade': 'OFF', 'language': 'FASTEXPR', 'visualization': False}, 'regular': {'code': 'group_rank((fnd6_newa2v1300_tstkn)/cap, subindustry)', 'description': None, 'operatorCount': 2}, 'dateCreated': '2025-06-03T17:00:03-04:00', 'dateSubmitted': None, 'dateModified': '2025-06-03T17:00:03-04:00', 'name': None, 'favorite': False, 'hidden': False, 'color': None, 'category': None, 'tags': [], 'classifications': [], 'grade': 'INFERIOR', 'stage': 'IS', 'status': 'UNSUBMITTED', 'is': {'pnl': 3072925, 'bookSize': 20000000, 'longCount': 1174, 'shortCount': 1868, 'turnover': 0.0176, 'returns': 0.0621, 'drawdown': 0.0658, 'margin': 0.007038, 'sharpe': 1.23, 'fitness': 0.87, 'startDate': '2018-01-20', 'checks': [{'name': 'LOW_SHARPE', 'result': 'FAIL', 'limit': 1.25, 'value': 1.23}, {'name': 'LOW_FITNESS', 'result': 'FAIL', 'limit': 1.0, 'value': 0.87}, {'name': 'LOW_TURNOVER', 'result': 'PASS', 'limit': 0.01, 'value': 0.0176}, {'name': 'HIGH_TURNOVER', 'result': 'PASS', 'limit': 0.7, 'value': 0.0176}, {'name': 'CONCENTRATED_WEIGHT', 'result': 'PASS'}, {'name': 'LOW_SUB_UNIVERSE_SHARPE', 'result': 'PASS', 'limit': 0.53, 'value': 0.65}, {'name': 'SELF_CORRELATION', 'result': 'PENDING'}, {'name': 'MATCHES_COMPETITION', 'result': 'PASS', 'competitions': [{'id': 'challenge', 'name': 'Challenge'}]}]}, 'os': None, 'train': None, 'test': None, 'prod': None, 'competitions': None, 'themes': None, 'pyramids': None, 'pyramidThemes': None, 'team': None}
    #     ]
    # }
    if sim_resp.status_code == 200:
        return sim_resp.json()["results"]
    return []


if __name__ == "__main__":
    df = pd.read_csv("file1.csv")
    limit = 100
    offset = 0
    count = 40000
    while True:
        print("offset = ", offset)
        if offset > count:
            break
        sess = sign_in()
        unsubmitted_alphas = get_alphas(sess, limit, offset)
        print("len(unsubmitted_alphas):", len(unsubmitted_alphas))
        if len(unsubmitted_alphas) == 0:
            sleep(15)
            continue

        for unsubmitted_alpha in unsubmitted_alphas:
            unsubmitted_success = 1
            for unsubmitted_check in unsubmitted_alpha["is"]["checks"]:
                if unsubmitted_check["result"] == "FAIL":
                    unsubmitted_success = 0
                    break
            if unsubmitted_success == 1:
                unsubmitted_alpha_id = unsubmitted_alpha["id"]
                print("unsubmitted success alpha id is:", unsubmitted_alpha_id)
                if unsubmitted_alpha_id in df["alpha_id"].values:
                    print("存在 CSV")
                else:
                    flag = 1
                    while flag:
                        try:
                            print("不存在 CSV")
                            check_result = sess.get(
                                f"https://api.worldquantbrain.com/alphas/{unsubmitted_alpha_id}/check",
                                timeout=300,
                            )
                            print("check result status_code:", check_result.status_code)
                            submit_sucess = 1
                            for submit_check in check_result.json()["is"]["checks"]:
                                if submit_check["result"] != "PASS":
                                    submit_sucess = 0
                                    break
                            if submit_sucess == 1:
                                print(
                                    "submit sucess alpha id is :", unsubmitted_alpha_id
                                )
                                df = df._append(
                                    pd.DataFrame(
                                        {
                                            "alpha_id": [unsubmitted_alpha_id],
                                            "status": [True],
                                        }
                                    ),
                                    ignore_index=False,
                                )
                            else:
                                df = df._append(
                                    pd.DataFrame(
                                        {
                                            "alpha_id": [unsubmitted_alpha_id],
                                            "status": [False],
                                        }
                                    ),
                                    ignore_index=False,
                                )
                            df.to_csv("file1.csv", index=False)
                            sleep(15)
                            flag = 0
                        except JSONDecodeError:
                            sleep(15)
                            continue
        offset += limit
        print("***" * 20)
        sleep(15)

    # for offset in range(900, 2000, limit):
    #     sess = sign_in()
    #     unsubmitted_alphas = get_alphas(sess, limit, offset)
    #     print("len(unsubmitted_alphas):", len(unsubmitted_alphas))
    #     for unsubmitted_alpha in unsubmitted_alphas:
    #         unsubmitted_success = 1
    #         for unsubmitted_check in unsubmitted_alpha["is"]["checks"]:
    #             if unsubmitted_check["result"] == "FAIL":
    #                 unsubmitted_success = 0
    #                 break
    #         if unsubmitted_success == 1:
    #             unsubmitted_alpha_id = unsubmitted_alpha['id']
    #             print("unsubmitted success alpha id is:", unsubmitted_alpha_id)
    #             if unsubmitted_alpha_id in df["alpha_id"].values:
    #                 print("存在 CSV")
    #             else:
    #                 print("不存在 CSV")
    #                 check_result = sess.get(f"https://api.worldquantbrain.com/alphas/{unsubmitted_alpha_id}/check", timeout=300)
    #                 print("check result status_code:", check_result.status_code)
    #                 submit_sucess = 1
    #                 for submit_check in check_result.json()['is']['checks']:
    #                     if submit_check['result'] != 'PASS':
    #                         submit_sucess = 0
    #                         break
    #                 if submit_sucess == 1:
    #                     print("submit sucess alpha id is :", unsubmitted_alpha_id)
    #                     df = df._append(pd.DataFrame({'alpha_id': [unsubmitted_alpha_id], 'status': [True]}), ignore_index = False)
    #                 else:
    #                     df = df._append(pd.DataFrame({'alpha_id': [unsubmitted_alpha_id], 'status': [False]}), ignore_index = False)
    #                 df.to_csv('file1.csv', index=False)
    #                 sleep(15)
    #     print("***" * 20)
    #     sleep(15)


# df = pd.read_csv("file1.csv")

# alphas = sim_resp.json()["results"]
# print("len(alphas):", len(alphas))
# for alpha in alphas:
#     success = 1
#     for check in alpha["is"]["checks"]:
#         if check["result"] == "FAIL":
#             success = 0
#             break
#     if success == 1:
#         print(alpha["id"])
#         if alpha["id"] in df["alpha_id"].values:
#             print("存在")
#         else:
#             print("不存在")
#             result = sess.get(f"https://api.worldquantbrain.com/alphas/{alpha["id"]}/check", timeout=300)
#             # print(result.json())
#             senond_sucess = 1
#             for second in result.json()['is']['checks']:
#                 if second['result'] != 'PASS':
#                     senond_sucess = 0
#                     print(second)
#                     break
#             if senond_sucess == 1:
#                 print("success:", alpha["id"])
#                 df = df._append(pd.DataFrame({'alpha_id': [alpha["id"]], 'status': [True]}), ignore_index = False)
#             else:
#                 df = df._append(pd.DataFrame({'alpha_id': [alpha["id"]], 'status': [False]}), ignore_index = False)
#             df.to_csv('file1.csv', index=False)
#             sleep(15)


# {
#     'is': {
#         'checks': [
#             {'name': 'LOW_SHARPE', 'result': 'PASS', 'limit': 1.25, 'value': 1.64},
#             {'name': 'LOW_FITNESS', 'result': 'PASS', 'limit': 1.0, 'value': 1.03},
#             {'name': 'LOW_TURNOVER', 'result': 'PASS', 'limit': 0.01, 'value': 0.3699},
#             {'name': 'HIGH_TURNOVER', 'result': 'PASS', 'limit': 0.7, 'value': 0.3699},
#             {'name': 'CONCENTRATED_WEIGHT', 'result': 'PASS'},
#             {'name': 'LOW_SUB_UNIVERSE_SHARPE', 'result': 'PASS', 'limit': 0.71, 'value': 1.16},
#             {'name': 'SELF_CORRELATION', 'result': 'FAIL', 'limit': 0.7, 'value': 0.9706},
#             {
#                 'name': 'MATCHES_COMPETITION',
#                 'result': 'PASS',
#                 'competitions': [
#                     {'id': 'challenge', 'name': 'Challenge'}
#                 ]
#             }
#         ],
#         'selfCorrelated': {
#             'schema': {
#                 'name': 'selfCorrelation',
#                 'title': 'Self Correlated',
#                 'properties': [
#                     {'name': 'id', 'title': 'Id', 'type': 'string'},
#                     {'name': 'name', 'title': '名字', 'type': 'string'},
#                     {'name': 'instrumentType', 'title': 'Instrument Type', 'type': 'string'},
#                     {'name': 'region', 'title': 'Region', 'type': 'string'},
#                     {'name': 'universe', 'title': 'Universe', 'type': 'string'},
#                     {'name': 'correlation', 'title': 'Correlation', 'type': 'decimal'},
#                     {'name': 'sharpe', 'title': 'Sharpe', 'type': 'decimal'},
#                     {'name': 'returns', 'title': 'Returns', 'type': 'percent'},
#                     {'name': 'turnover', 'title': 'Turnover', 'type': 'percent'},
#                     {'name': 'fitness', 'title': 'Fitness', 'type': 'decimal'},
#                     {'name': 'margin', 'title': 'Margin', 'type': 'permyriad'}
#                 ]
#             },
#             'records': [
#                 ['r9xYJKa', None, 'EQUITY', 'USA', 'TOP3000', 0.9706, 1.8, 0.1601, 0.3668, 1.19, 0.000873],
#                 ['GX9MW05', None, 'EQUITY', 'USA', 'TOP3000', 0.9382, 1.6, 0.1354, 0.3413, 1.01, 0.000793]
#             ],
#             'min': None,
#             'max': 0.9706
#         }
#     }
# }
