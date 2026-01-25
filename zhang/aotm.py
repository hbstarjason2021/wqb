import logging
import time
from machine_lib import login

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('check_ATOM_alpha.log')
    ]
)

def get_alphas(session, start_date, end_date, alpha_num, sharpe_th, fitness_th):
    for i in range(0, alpha_num, 100):
        print(i)
        ## 大于正值来筛选
        url = "https://api.worldquantbrain.com/users/self/alphas?limit=100&offset=%d"%(i) \
            + "&status=UNSUBMITTED%1FIS_FAIL&dateCreated%3E=" + start_date  \
            + "T00:00:00-05:00&dateCreated%3C" + end_date \
            + "T00:00:00-05:00&is.fitness%3E" + str(fitness_th) \
            + "&is.sharpe%3E" + str(sharpe_th) \
            + "&order=-is.sharpe&hidden=false&type!=SUPER"
   
        resp = session.get(url)
        resp.raise_for_status()
        print(resp.json())
       
    return resp.json()['results']  # 假设返回的是列表

def get_single_dataset_alphas(session, alphas):
    filtered = []
    for alpha in alphas:
        alpha_id = alpha[0] if isinstance(alpha, (list, tuple)) else alpha.get('id')
        url = f"https://api.worldquantbrain.com/alphas/{alpha_id}"
        try:
            resp = session.get(url)
            data = resp.json()
            classifications = data.get('classifications', [])
            has_single_dataset = any(
                c.get('id') == 'DATA_USAGE:SINGLE_DATA_SET' for c in classifications
            )
            checks = data.get('is', {}).get('checks', [])
            has_fail = any(chk.get('result') == 'FAIL' for chk in checks)
            if has_single_dataset and not has_fail:
                logging.info(f"找到 ATOM alpha: {alpha_id}")
                filtered.append(data)
        except Exception as e:
            logging.error(f"获取alpha详情失败: {alpha_id}, {e}")
    return filtered

def print_alpha_info(alpha):
    alpha_id = alpha.get('id')
    code = alpha.get('regular', {}).get('code', 'N/A')
    is_data = alpha.get('is', {})
    sharpe = is_data.get('sharpe', 'N/A')
    fitness = is_data.get('fitness', 'N/A')
    turnover = is_data.get('turnover', 'N/A')
    margin = is_data.get('margin', 'N/A')
    classifications = [c.get('name', '') for c in alpha.get('classifications', [])]
    check_results = [f"{chk.get('name', 'UNKNOWN')}: {chk.get('result', 'UNKNOWN')}" for chk in is_data.get('checks', [])]
    logging.info(f"Alpha ID: {alpha_id}")
    logging.info(f"代码: {code}")
    logging.info(f"Sharpe: {sharpe}, Fitness: {fitness}, Turnover: {turnover}, Margin: {margin}")
    logging.info(f"分类: {', '.join(classifications)}")
    logging.info(f"检查结果: {'; '.join(check_results)}")
    logging.info("-" * 80)

def main():
    logging.info("脚本开始执行...")
    # 直接用 machine_lib.login() 获取 session，不需要 token 检查
    session = login()
    logging.info("正在获取alpha列表...")
    try:
        alphas = get_alphas(
            session,
            start_date="2026-01-01",
            end_date="2026-01-25",
            alpha_num=1000, # 假设你要获取100个alpha
            sharpe_th=1,
            fitness_th=0.3
        )
    except Exception as e:
        logging.error(f"获取alpha列表失败: {e}")
        return

    filtered_alphas = get_single_dataset_alphas(session, alphas)
    logging.info(f"总共找到 {len(filtered_alphas)} 个符合条件的单数据集且无FAIL检查的alpha")

    for idx, alpha in enumerate(filtered_alphas):
        alpha_id = alpha['id']
        logging.info(f"checking index {idx} alpha_id {alpha_id}")
        # 检查prod相关性
        while True:
            result = session.get(f"https://api.worldquantbrain.com/alphas/{alpha_id}/correlations/prod")
            if "retry-after" in result.headers:
                time.sleep(float(result.headers.get("retry-after", "1")))
            else:
                break
        try:
            if result.json().get("max", 0) < 0.7:
                print_alpha_info(alpha)
               
        except Exception as e:
            logging.error(f"catch: {alpha_id}, {str(e)}")

if __name__ == '__main__':
    main()
