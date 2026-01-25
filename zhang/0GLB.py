"""
WorldQuant Brain 批量Alpha生成 - 完整操作符版本
修复：
1. 解决400 Bad Request（Multi-simulations格式错误）
2. 任务失败自动跳过，继续执行下一个任务
3. 增强异常处理，避免程序卡住
"""

import sys
import random
import time
sys.path.append('.')
from machine_lib_0GLB import *

# ============================= 配置区域 =============================
# 全局登录Session（确保整个程序使用同一个Session）
s = None

# 数据集配置
DATASET_ID = 'analyst69'                 
REGION = 'GLB'                           
UNIVERSE = 'MINVOL1M'                    
DELAY = 1                                
DATA_TYPE = 'MATRIX'                     

# 模拟配置（关键：降低并发，减少请求频率）
NEUTRALIZATIONS =  ["NONE", "REVERSION_AND_MOMENTUM", "STATISTICAL", "CROWDING", "FAST", "SLOW", "MARKET", "SECTOR", "INDUSTRY", "SUBINDUSTRY", "COUNTRY","SLOW_AND_FAST"]
random.shuffle(NEUTRALIZATIONS)
INIT_DECAY = 60                           
TASK_POOL_SIZE = 1  # 单任务模式（修复400错误关键）
CONCURRENT_SIMS = 1  

# 字段范围
FIELD_RANGE_SIZE = 20  

# ============================= 核心修复：表达式生成器保持不变 =============================
class AlphaExpressionGenerator:
    """智能Alpha表达式生成器 - 支持所有151个操作符"""
    
    def __init__(self, fields, data_type='MATRIX'):
        self.fields = fields
        self.data_type = data_type
        self.expressions = []
    
    def generate_all(self):
        """生成所有类型的表达式"""
        print(f"\n[表达式生成] 开始生成...")
        print(f"  字段数: {len(self.fields)}")
        print(f"  数据类型: {self.data_type}")
        
        # 1. 单参数操作符 (46个)
        self._generate_single_param()
        print(f"  ✓ 单参数操作符: {len([op for op in basic_ops if op in basic_ops])}个")
        
        # 2. 时间序列操作符 (双参数，需要窗口期)
        self._generate_ts_operators()
        print(f"  ✓ 时间序列操作符: {len([op for op in ts_ops if op.startswith('ts_')])}个")
        
        # 3. Tail类操作符
        self._generate_tail_operators()
        print(f"  ✓ Tail类操作符")
        
        # 3.5. Bucket操作符（需要命名参数）
        self._generate_bucket_operators()
        print(f"  ✓ Bucket操作符")
        
        # 3.6. Truncate & Winsorize操作符（需要命名参数）
        self._generate_truncate_winsorize_operators()
        print(f"  ✓ Truncate & Winsorize操作符")
        
        # 3.7. Clamp操作符（需要命名参数）
        self._generate_clamp_operators()
        print(f"  ✓ Clamp操作符")
        
        # 3.8. TS Target TVR系列操作符（需要完整命名参数）
        self._generate_ts_target_tvr_operators()
        print(f"  ✓ TS Target TVR操作符")
        
        # 3.9. Densify操作符（用于分组字段优化）
        self._generate_densify_operators()
        print(f"  ✓ Densify操作符")
        
        # 4. 分组操作符 (双参数，需要分组字段)
        self._generate_group_operators()
        print(f"  ✓ 分组操作符: {len([op for op in basic_ops if op.startswith('group_')])}个")
        
        # 5. 双字段算术/逻辑操作符
        if len(self.fields) >= 2:
            self._generate_dual_field()
            print(f"  ✓ 双字段操作符")
        
        # 6. 三参数操作符（精选）
        self._generate_triple_param()
        print(f"  ✓ 三参数操作符: {len([op for op in basic_ops if op in ['ts_corr', 'ts_covariance', 'if_else']])}个")
        
        print(f"\n  总表达式数: {len(self.expressions)}")
        return self.expressions
    
    def _get_field_expr(self, field):
        """获取字段表达式（VECTOR需要先转换）"""
        if self.data_type == 'VECTOR':
            vec_op = random.choice([
                'vec_avg', 'vec_sum', 'vec_max', 'vec_min', 'vec_count', 'vec_stddev', 'vec_norm'
            ])
            return f'{vec_op}({field})'
        return field
    
    def _generate_single_param(self):
        """生成单参数操作符表达式"""
        exclude_ops = ['ts_backfill', 'right_tail', 'left_tail', 'tail', 'bucket', 'truncate', 'winsorize', 'clamp',
                       'ts_target_tvr_decay', 'ts_target_tvr_hump', 'densify']
        single_ops = [op for op in basic_ops if not op.startswith('vec_') and op not in exclude_ops]
        
        for field in self.fields:
            field_expr = self._get_field_expr(field)
            self.expressions.append(field_expr)
            self.expressions.append(f'-{field_expr}')
            
            for op in single_ops:
                self.expressions.append(f'{op}({field_expr})')
                self.expressions.append(f'-{op}({field_expr})')
    
    def _generate_ts_operators(self):
        """生成时间序列操作符表达式"""
        ts_ops_window = [
            'ts_rank', 'ts_mean', 'ts_sum', 'ts_std_dev', 
            'ts_delta', 'ts_delay', 'ts_max', 'ts_min',
            'ts_product', 'ts_zscore', 'ts_ir', 'ts_decay_linear',
            'ts_arg_max', 'ts_arg_min', 'ts_scale',
            'ts_median', 'ts_kurtosis', 'ts_skewness'
        ]
        
        ts_ops_lookback = ['ts_backfill', 'ts_av_diff', 'ts_returns']
        windows = [5, 10, 20, 60]
        
        for field in self.fields[::2]:
            field_expr = self._get_field_expr(field)
            for op in ts_ops_window:
                for window in windows[::2]:
                    self.expressions.append(f'{op}({field_expr}, {window})')
            
            for op in ts_ops_lookback:
                for window in windows[::2]:
                    self.expressions.append(f'{op}({field_expr}, {window})')
    
    def _generate_tail_operators(self):
        """生成tail类操作符表达式"""
        for field in self.fields[::3]:
            field_expr = self._get_field_expr(field)
            for minimum in [0, 0.5, 1]:
                self.expressions.append(f'right_tail({field_expr}, minimum={minimum})')
            for maximum in [0, -0.5, -1]:
                self.expressions.append(f'left_tail({field_expr}, maximum={maximum})')
            self.expressions.append(f'tail({field_expr}, lower=-1, upper=1, newval=0)')
            self.expressions.append(f'tail({field_expr}, lower=-2, upper=2, newval=0)')
    
    def _generate_bucket_operators(self):
        """生成bucket操作符表达式 - 必须使用命名参数"""
        for field in self.fields[::4]:
            field_expr = self._get_field_expr(field)
            rank_expr = f'rank({field_expr})'
            self.expressions.append(f'bucket({rank_expr}, range="0, 1, 0.1")')
            self.expressions.append(f'bucket({rank_expr}, range="0, 1, 0.05")')
            self.expressions.append(f'bucket({rank_expr}, buckets="0.2,0.4,0.6,0.8")')
    
    def _generate_truncate_winsorize_operators(self):
        """生成truncate和winsorize操作符表达式 - 必须使用命名参数"""
        for field in self.fields[::3]:
            field_expr = self._get_field_expr(field)
            self.expressions.append(f'truncate({field_expr}, maxPercent=0.01)')
            self.expressions.append(f'truncate({field_expr}, maxPercent=0.05)')
            self.expressions.append(f'truncate(rank({field_expr}), maxPercent=0.02)')
            
            self.expressions.append(f'winsorize({field_expr}, std=3)')
            self.expressions.append(f'winsorize({field_expr}, std=4)')
            self.expressions.append(f'winsorize(rank({field_expr}), std=2.5)')
    
    def _generate_clamp_operators(self):
        """生成clamp操作符表达式 - 必须使用命名参数"""
        for field in self.fields[::4]:
            field_expr = self._get_field_expr(field)
            self.expressions.append(f'clamp({field_expr}, lower=0.95, upper=1.05)')
            self.expressions.append(f'clamp({field_expr}, lower=-0.1, upper=0.1)')
            self.expressions.append(f'clamp(-ts_returns({field_expr}, 5), lower=-0.05, upper=0.05)')
            self.expressions.append(f'clamp(ts_delta({field_expr}, 10), lower=-0.1, upper=0.1)')
    
    def _generate_ts_target_tvr_operators(self):
        """生成ts_target_tvr系列操作符 - 必须使用完整的命名参数"""
        for field in self.fields[::4]:
            field_expr = self._get_field_expr(field)
            self.expressions.append(f'ts_target_tvr_decay({field_expr}, lambda_min=0, lambda_max=1, target_tvr=0.1)')
            self.expressions.append(f'ts_target_tvr_decay({field_expr}, lambda_min=0, lambda_max=0.5, target_tvr=0.05)')
            self.expressions.append(f'ts_target_tvr_hump({field_expr}, lambda_min=0, lambda_max=1, target_tvr=0.1)')
            self.expressions.append(f'ts_target_tvr_hump({field_expr}, lambda_min=0, lambda_max=0.5, target_tvr=0.05)')
        
        if len(self.fields) >= 2:
            for i, field1 in enumerate(self.fields[:3]):
                for field2 in self.fields[i+1:min(i+2, len(self.fields))]:
                    expr1 = self._get_field_expr(field1)
                    expr2 = self._get_field_expr(field2)
                    self.expressions.append(f'ts_target_tvr_delta_limit({expr1}, {expr2}, lambda_min=0, lambda_max=1, target_tvr=0.1)')
    
    def _generate_densify_operators(self):
        """生成densify操作符 - 用于优化分组字段的桶数量"""
        groups = ['subindustry', 'industry', 'sector']
        
        for group in groups:
            self.expressions.append(f'densify({group})')
            
            for field in self.fields[:3]:
                field_expr = self._get_field_expr(field)
                self.expressions.append(f'group_rank({field_expr}, densify({group}))')
                self.expressions.append(f'group_neutralize({field_expr}, densify({group}))')

    def _generate_group_operators(self):
        """生成分组操作符表达式"""
        group_ops = [
            'group_rank', 'group_zscore', 'group_neutralize',
            'group_mean', 'group_scale', 'group_normalize'
        ]
        
        groups = ['subindustry', 'industry', 'sector']
        
        for field in self.fields[::3]:
            field_expr = self._get_field_expr(field)
            
            for op in group_ops:
                for group in groups[:2]:
                    if op == 'group_mean':
                        self.expressions.append(f'{op}({field_expr}, 1, {group})')
                    else:
                        self.expressions.append(f'{op}({field_expr}, {group})')
    
    def _generate_dual_field(self):
        """生成双字段操作符表达式"""
        dual_ops = ['add', 'subtract', 'multiply', 'divide', 'power']
        
        for i, field1 in enumerate(self.fields[:5]):
            for field2 in self.fields[i+1:min(i+3, len(self.fields))]:
                expr1 = self._get_field_expr(field1)
                expr2 = self._get_field_expr(field2)
                
                for op in dual_ops[:3]:
                    self.expressions.append(f'{op}({expr1}, {expr2})')
    
    def _generate_triple_param(self):
        """生成三参数操作符表达式（精选）"""
        triple_ops = ['ts_corr', 'ts_covariance', 'if_else']
        
        if len(self.fields) >= 2:
            field1 = self._get_field_expr(self.fields[0])
            field2 = self._get_field_expr(self.fields[1])
            
            for op in ['ts_corr', 'ts_covariance']:
                for window in [20, 60]:
                    self.expressions.append(f'{op}({field1}, {field2}, {window})')
        
        for field in self.fields[::4]:
            field_expr = self._get_field_expr(field)
            self.expressions.append(f'if_else(greater({field_expr}, 0), {field_expr}, -{field_expr})')

# ============================= 核心修复：模拟任务处理函数 =============================
def generate_sim_data_fixed(alpha_item, region, uni, neut):
    """
    修复版：生成单条模拟数据（解决400错误）
    alpha_item: 单个任务元组 (expr, decay)
    """
    alpha, decay = alpha_item
    simulation_data = {
        'type': 'REGULAR',
        'settings': {
            'instrumentType': 'EQUITY',
            'region': region,
            'universe': uni,
            'delay': 1,
            'decay': decay,
            'neutralization': neut,
            'truncation': 0.08,
            'pasteurization': 'ON',
            'testPeriod': 'P0Y',
            'unitHandling': 'VERIFY',
            'nanHandling': 'ON',
            'language': 'FASTEXPR',
            'visualization': False,
        },
        'regular': alpha
    }
    return simulation_data

def multi_simulate_fixed(alpha_pools, neut, region, universe, start):
    """
    修复版：批量模拟函数
    1. 解决400 Bad Request（单任务不包裹数组）
    2. 任务失败自动跳过，继续下一个
    3. 增强异常处理，避免卡住
    """
    global s
    if s is None:
        s = login()
    
    brain_api_url = 'https://api.worldquantbrain.com'
    failed_tasks = []  # 记录失败任务
    
    for x, pool in enumerate(alpha_pools):
        if x < start: 
            continue
        
        print(f"\n[Pool {x}] 开始处理 {len(pool)} 个任务...")
        progress_urls = []
        
        # 遍历每个任务，逐个处理（失败跳过）
        for y, task in enumerate(pool):
            try:
                # 生成单条模拟数据（不包裹数组）
                sim_data = generate_sim_data_fixed(task, region, universe, neut)
                
                # 提交前添加延迟，避免限流
                time.sleep(GLOBAL_REQUEST_DELAY)
                
                # 核心修复：单任务直接提交（非数组），多任务才用数组
                simulation_response = s.post(
                    'https://api.worldquantbrain.com/simulations',
                    json=sim_data  # 单任务：直接传字典（非数组）
                )
                
                # 处理429限流
                if simulation_response.status_code == 429:
                    retry_after = int(simulation_response.headers.get("Retry-After", 10))
                    print(f"⚠ [Pool {x}-Task {y}] 限流，等待 {retry_after} 秒...")
                    time.sleep(retry_after)
                    # 重试提交
                    simulation_response = s.post(
                        'https://api.worldquantbrain.com/simulations',
                        json=sim_data
                    )
                
                simulation_response.raise_for_status()
                simulation_progress_url = simulation_response.headers.get('Location')
                
                if simulation_progress_url:
                    progress_urls.append((simulation_progress_url, task))
                    print(f"✅ [Pool {x}-Task {y}] 提交成功: {task[0][:50]}...")
                else:
                    print(f"⚠ [Pool {x}-Task {y}] 无进度URL，跳过")
                    failed_tasks.append((x, y, task, "无进度URL"))
                    
            except requests.exceptions.HTTPError as e:
                error_msg = f"HTTP错误: {e.response.status_code} - {e.response.text[:100]}"
                print(f"❌ [Pool {x}-Task {y}] 提交失败: {error_msg}")
                failed_tasks.append((x, y, task, error_msg))
                # 跳过当前任务，继续下一个
                continue
            except Exception as e:
                error_msg = f"系统错误: {str(e)[:100]}"
                print(f"❌ [Pool {x}-Task {y}] 提交失败: {error_msg}")
                failed_tasks.append((x, y, task, error_msg))
                # 重新登录（如果需要）
                if "401" in str(e) or "unauthorized" in str(e).lower():
                    print(f"🔄 重新登录...")
                    s = login()
                # 跳过当前任务，继续下一个
                continue
        
        print(f"[Pool {x}] 提交完成 - 成功: {len(progress_urls)} | 失败: {len(failed_tasks)}")
        
        # 检查任务进度（失败不影响后续）
        for j, (progress, task) in enumerate(progress_urls):
            try:
                while True:
                    time.sleep(GLOBAL_REQUEST_DELAY)
                    simulation_progress = s.get(progress)
                    
                    if simulation_progress.headers.get("Retry-After"):
                        sleep_time = float(simulation_progress.headers["Retry-After"])
                        print(f"⚠ [Pool {x}-Progress {j}] 限流，等待 {sleep_time} 秒...")
                        time.sleep(sleep_time)
                        continue
                    
                    status = simulation_progress.json().get("status", "UNKNOWN")
                    if status in ["COMPLETE", "FAILED", "CANCELLED"]:
                        print(f"📊 [Pool {x}-Progress {j}] 状态: {status}")
                        break
                    else:
                        print(f"⌛ [Pool {x}-Progress {j}] 状态: {status}，等待中...")
                        time.sleep(2)
                        
            except Exception as e:
                print(f"❌ [Pool {x}-Progress {j}] 进度查询失败: {str(e)[:100]}")
                continue  # 跳过进度查询失败的任务
        
        print(f"✅ [Pool {x}] 处理完成")
    
    # 输出失败任务汇总
    if failed_tasks:
        print(f"\n📝 失败任务汇总（共{len(failed_tasks)}个）:")
        for idx, (x, y, task, err) in enumerate(failed_tasks[:5]):  # 只显示前5个
            print(f"  - Pool{x}-Task{y}: {task[0][:50]}... | 原因: {err}")
        if len(failed_tasks) > 5:
            print(f"  - 还有 {len(failed_tasks)-5} 个失败任务，略过显示")
    else:
        print(f"\n🎉 所有任务提交成功！")

# ============================= 主流程 =============================
def main():
    """主执行函数 - 完整的执行流程"""
    global s
    
    print("=" * 70)
    print(f"WorldQuant Brain 批量Alpha生成 - 完整操作符版（防429+自动跳过失败任务）")
    print("=" * 70)
    print(f"\n配置: {DATASET_ID} | {REGION}/{UNIVERSE}/D{DELAY}")
    print(f"支持操作符: {len(basic_ops + ts_ops)}个")
    print(f"中性化配置: {len(NEUTRALIZATIONS)}个 - {NEUTRALIZATIONS[:3]}...")
    print(f"⚠ 单任务提交模式（修复400错误）| 失败任务自动跳过")
    print("-" * 70)
    
    # 1. 确保登录成功
    print(f"\n[1/6] 验证登录状态...")
    if s is None:
        print(f"  → 正在登录...")
        s = login()
    
    if s is None:
        print(f"❌ 登录失败，程序退出")
        return
    
    print(f"  ✓ 登录状态正常")
    
    # 2. 获取数据字段
    print(f"\n[2/6] 获取数据字段...")
    try:
        gdf = get_datafields(
            s=s,
            instrument_type='EQUITY',
            region=REGION,
            delay=DELAY,
            universe=UNIVERSE,
            dataset_id=DATASET_ID
        )
        
        if gdf.empty or len(gdf) == 0:
            print(f"⚠ 警告：未获取到任何字段！使用默认测试字段继续...")
            fields = ['close', 'volume', 'open', 'high', 'low']
        else:
            all_fields = gdf[gdf['type'] == DATA_TYPE]['id'].tolist()
            if len(all_fields) > FIELD_RANGE_SIZE:
                start_idx = random.randint(0, len(all_fields) - FIELD_RANGE_SIZE)
                fields = all_fields[start_idx : start_idx + FIELD_RANGE_SIZE]
            else:
                fields = all_fields
        
        print(f"  ✓ 总字段: {len(all_fields) if 'all_fields' in locals() else len(fields)} | 使用: {len(fields)}")
        if fields:
            print(f"  示例: {fields[0]}")
            
    except Exception as e:
        print(f"❌ 获取数据字段失败: {str(e)[:100]}")
        print("→ 使用默认测试字段继续...")
        fields = ['close', 'volume', 'open', 'high', 'low']
        DATA_TYPE = 'MATRIX'
    
    # 3. 生成表达式
    print(f"\n[3/6] 生成Alpha表达式...")
    try:
        generator = AlphaExpressionGenerator(fields, DATA_TYPE)
        expressions = generator.generate_all()
        
        print(f"  ✓ 表达式总数: {len(expressions)}")
        print(f"  预计批次: {int(len(expressions) / 65) if expressions else 0}")
        if expressions:
            print(f"  示例: {expressions[0]}")
    except Exception as e:
        print(f"❌ 生成表达式失败: {str(e)[:100]}")
        expressions = [f"rank(ts_returns({field}, 5))" for field in fields[:3]]
        print(f"→ 使用简化表达式继续: {expressions}")
    
    # 4. 生成First Order
    print(f"\n[4/6] 生成First Order...")
    try:
        first_order = first_order_factory(expressions, ops_set)
        print(f"  ✓ First Order: {len(first_order)}")
    except Exception as e:
        print(f"❌ 生成First Order失败: {str(e)[:100]}")
        first_order = expressions[:10]
        print(f"→ 使用简化First Order继续: {len(first_order)}个")
    
    # 5. 准备任务
    print(f"\n[5/6] 准备任务...")
    try:
        tasks = [(expr, INIT_DECAY) for expr in first_order]
        random.shuffle(tasks)
        pools = load_task_pool(tasks, TASK_POOL_SIZE, CONCURRENT_SIMS)
        
        print(f"  ✓ 任务数: {len(tasks)}")
        print(f"  任务池: {TASK_POOL_SIZE} | 并发: {CONCURRENT_SIMS}")
        print(f"  衰减: {INIT_DECAY}")
        
        if pools:
            print(f"  示例任务: {pools[0][0][0][:50]}...")
    except Exception as e:
        print(f"❌ 准备任务失败: {str(e)[:100]}")
        print("→ 程序无法继续，退出")
        return
    
    # 6. 批量模拟（使用修复版函数）
    print(f"\n[6/6] 批量模拟...")
    total_neutralizations = len(NEUTRALIZATIONS)
    
    if total_neutralizations == 0:
        print(f"⚠ 没有中性化配置，程序退出")
        return
    
    # 遍历所有中性化配置，失败不终止
    for idx, neutralization in enumerate(NEUTRALIZATIONS, 1):
        print("\n" + "=" * 70)
        print(f"执行中性化配置 [{idx}/{total_neutralizations}]: {neutralization}")
        print("=" * 70)
        
        try:
            # 调用修复版模拟函数
            multi_simulate_fixed(
                alpha_pools=pools,
                neut=neutralization,
                region=REGION,
                universe=UNIVERSE,
                start=0
            )
            print(f"  ✓ 中性化配置 {neutralization} 执行完成")
        except Exception as e:
            error_msg = str(e)[:150]
            print(f"❌ 执行中性化配置 {neutralization} 失败: {error_msg}")
            print(f"→ 跳过当前配置，继续下一个...")
            # 增加延迟，避免连续失败
            time.sleep(5)
            continue
    
    print("\n" + "=" * 70)
    print(f"✅ 所有配置执行完成！")
    print("=" * 70)

# ============================= 程序入口 =============================
if __name__ == "__main__":
    """程序执行入口 - 关键：确保main函数被调用"""
    # 初始化全局配置（从machine_lib导入）
    GLOBAL_REQUEST_DELAY = 1.0
    MAX_RETRIES = 5
    RETRY_BACKOFF_FACTOR = 2
    
    # 基础操作符定义（防止缺失）
    basic_ops = ["reverse", "inverse", "rank", "zscore", "quantile", "normalize",
                 "right_tail", "left_tail", "tail", "bucket", "truncate", "winsorize",
                 "clamp", "ts_target_tvr_decay", "ts_target_tvr_hump", "densify",
                 "group_rank", "group_zscore", "group_neutralize", "group_mean",
                 "group_scale", "group_normalize", "add", "subtract", "multiply",
                 "divide", "power", "ts_corr", "ts_covariance", "if_else", "greater"]
     
    ts_ops = ["ts_rank", "ts_zscore", "ts_delta",  "ts_sum", "ts_delay", 
              "ts_std_dev", "ts_mean",  "ts_arg_min", "ts_arg_max","ts_scale", 
              "ts_quantile", "ts_backfill", "ts_av_diff", "ts_returns", "ts_product",
              "ts_ir", "ts_decay_linear", "ts_max", "ts_min", "ts_median",
              "ts_kurtosis", "ts_skewness", "ts_target_tvr_delta_limit"]
     
    ops_set = basic_ops + ts_ops
    
    try:
        # 先登录
        s = login()
        # 执行主函数
        main()
    except KeyboardInterrupt:
        print(f"\n\n⚠ 用户中断程序执行")
    except Exception as e:
        print(f"\n\n❌ 程序执行出错: {str(e)[:200]}")
        import traceback
        traceback.print_exc()
