"""
WorldQuant Brain 批量Alpha生成 - 完整操作符版本
修复：降低并发数，增加任务间隔，避免429错误
"""

import sys
import random
sys.path.append('.')
from machine_lib_0GLB import *

# ============================= 配置区域 =============================
s = login()

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
TASK_POOL_SIZE = 1  # 从2降低到1，减少并发
CONCURRENT_SIMS = 1  # 从2降低到1，减少并发

# 字段范围
FIELD_RANGE_SIZE = 20  # 从30降低到20，减少字段数量和表达式数量

# ============================= 表达式生成器 =============================
# （以下代码与之前修复版一致，无需修改）
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
            # 使用全部7个向量操作符
            vec_op = random.choice([
                'vec_avg',      # 平均值
                'vec_sum',      # 总和
                'vec_max',      # 最大值
                'vec_min',      # 最小值
                'vec_count',    # 元素数量
                'vec_stddev',   # 标准差
                'vec_norm'      # 绝对值之和
            ])
            return f'{vec_op}({field})'
        return field
    
    def _generate_single_param(self):
        """生成单参数操作符表达式"""
        # 排除需要额外参数的操作符
        exclude_ops = ['ts_backfill', 'right_tail', 'left_tail', 'tail', 'bucket', 'truncate', 'winsorize', 'clamp',
                       'ts_target_tvr_decay', 'ts_target_tvr_hump', 'densify']
        single_ops = [op for op in basic_ops if not op.startswith('vec_') and op not in exclude_ops]
        
        for field in self.fields:
            field_expr = self._get_field_expr(field)
            
            # 原始字段
            self.expressions.append(field_expr)
            self.expressions.append(f'-{field_expr}')
            
            # 应用单参数操作符
            for op in single_ops:
                self.expressions.append(f'{op}({field_expr})')
                self.expressions.append(f'-{op}({field_expr})')
    
    def _generate_ts_operators(self):
        """生成时间序列操作符表达式"""
        # 标准时间序列操作符
        ts_ops_window = [
            'ts_rank', 'ts_mean', 'ts_sum', 'ts_std_dev', 
            'ts_delta', 'ts_delay', 'ts_max', 'ts_min',
            'ts_product', 'ts_zscore', 'ts_ir', 'ts_decay_linear',
            'ts_arg_max', 'ts_arg_min', 'ts_scale',
            'ts_median', 'ts_kurtosis', 'ts_skewness'
        ]
        
        # 需要lookback参数的操作符
        ts_ops_lookback = ['ts_backfill', 'ts_av_diff', 'ts_returns']
        
        windows = [5, 10, 20, 60]  # 常用窗口期
        
        for field in self.fields[::2]:  # 每隔一个字段，减少数量
            field_expr = self._get_field_expr(field)
            
            # 生成标准时间序列表达式
            for op in ts_ops_window:
                for window in windows[::2]:  # 使用部分窗口期
                    self.expressions.append(f'{op}({field_expr}, {window})')
            
            # 生成lookback表达式
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
        for field in self.fields[::4]:  # 每隔3个字段
            field_expr = self._get_field_expr(field)
            # bucket需要先rank，然后使用命名参数
            rank_expr = f'rank({field_expr})'
            
            # 使用range参数 (起始, 结束, 步长)
            self.expressions.append(f'bucket({rank_expr}, range="0, 1, 0.1")')
            self.expressions.append(f'bucket({rank_expr}, range="0, 1, 0.05")')
            
            # 使用buckets参数 (桶边界)
            self.expressions.append(f'bucket({rank_expr}, buckets="0.2,0.4,0.6,0.8")')
    
    def _generate_truncate_winsorize_operators(self):
        """生成truncate和winsorize操作符表达式 - 必须使用命名参数"""
        for field in self.fields[::3]:  # 每隔两个字段
            field_expr = self._get_field_expr(field)
            
            # truncate(x, maxPercent) - 截断极端值
            self.expressions.append(f'truncate({field_expr}, maxPercent=0.01)')
            self.expressions.append(f'truncate({field_expr}, maxPercent=0.05)')
            self.expressions.append(f'truncate(rank({field_expr}), maxPercent=0.02)')
            
            # winsorize(x, std) - 温莎化处理
            self.expressions.append(f'winsorize({field_expr}, std=3)')
            self.expressions.append(f'winsorize({field_expr}, std=4)')
            self.expressions.append(f'winsorize(rank({field_expr}), std=2.5)')
    
    def _generate_clamp_operators(self):
        """生成clamp操作符表达式 - 必须使用命名参数"""
        for field in self.fields[::4]:  # 每隔3个字段
            field_expr = self._get_field_expr(field)
            
            # clamp(x, lower, upper) - 限制值在范围内
            self.expressions.append(f'clamp({field_expr}, lower=0.95, upper=1.05)')
            self.expressions.append(f'clamp({field_expr}, lower=-0.1, upper=0.1)')
            
            # 对时间序列返回值使用clamp
            self.expressions.append(f'clamp(-ts_returns({field_expr}, 5), lower=-0.05, upper=0.05)')
            self.expressions.append(f'clamp(ts_delta({field_expr}, 10), lower=-0.1, upper=0.1)')
    
    def _generate_ts_target_tvr_operators(self):
        """生成ts_target_tvr系列操作符 - 必须使用完整的命名参数"""
        for field in self.fields[::4]:  # 每隔3个字段
            field_expr = self._get_field_expr(field)
            
            # ts_target_tvr_decay(x, lambda_min, lambda_max, target_tvr)
            self.expressions.append(f'ts_target_tvr_decay({field_expr}, lambda_min=0, lambda_max=1, target_tvr=0.1)')
            self.expressions.append(f'ts_target_tvr_decay({field_expr}, lambda_min=0, lambda_max=0.5, target_tvr=0.05)')
            
            # ts_target_tvr_hump(x, lambda_min, lambda_max, target_tvr)
            self.expressions.append(f'ts_target_tvr_hump({field_expr}, lambda_min=0, lambda_max=1, target_tvr=0.1)')
            self.expressions.append(f'ts_target_tvr_hump({field_expr}, lambda_min=0, lambda_max=0.5, target_tvr=0.05)')
        
        # ts_target_tvr_delta_limit(x, y, lambda_min, lambda_max, target_tvr) - 需要两个字段
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
        
        for field in self.fields[::3]:  # 每隔两个字段
            field_expr = self._get_field_expr(field)
            
            for op in group_ops:
                for group in groups[:2]:  # 只用前2个分组
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
                
                for op in dual_ops[:3]:  # 只用前3个操作符
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

# ============================= 主流程 =============================
def main():
    """主执行函数"""
    print("=" * 70)
    print(f"WorldQuant Brain 批量Alpha生成 - 完整操作符版（防429限流）")
    print("=" * 70)
    print(f"\n配置: {DATASET_ID} | {REGION}/{UNIVERSE}/D{DELAY}")
    print(f"支持操作符: {len(basic_ops + ts_ops)}个")
    print(f"中性化配置: {len(NEUTRALIZATIONS)}个 - {NEUTRALIZATIONS}")
    print(f"⚠ 已降低并发和字段数量，避免429错误")
    print("-" * 70)
    
    # 1. 获取数据字段（增加容错处理）
    print(f"\n[1/5] 获取数据字段...")
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
        print(f"❌ 获取数据字段失败: {str(e)}")
        print("→ 使用默认测试字段继续...")
        fields = ['close', 'volume', 'open', 'high', 'low']
        DATA_TYPE = 'MATRIX'
    
    # 2. 生成表达式
    print(f"\n[2/5] 生成Alpha表达式...")
    generator = AlphaExpressionGenerator(fields, DATA_TYPE)
    expressions = generator.generate_all()
    
    print(f"\n  表达式总数: {len(expressions)}")
    print(f"  预计批次: {int(len(expressions) / 65) if expressions else 0}")
    print(f"  示例: {expressions[0] if expressions else 'N/A'}")
    
    # 3. 生成First Order
    print(f"\n[3/5] 生成First Order...")
    first_order = first_order_factory(expressions, ops_set)
    print(f"  ✓ First Order: {len(first_order)}")
    
    # 4. 准备任务
    print(f"\n[4/5] 准备任务...")
    tasks = [(expr, INIT_DECAY) for expr in first_order]
    random.shuffle(tasks)
    pools = load_task_pool(tasks, TASK_POOL_SIZE, CONCURRENT_SIMS)
    
    print(f"  ✓ 任务数: {len(tasks)}")
    print(f"  任务池: {TASK_POOL_SIZE} | 并发: {CONCURRENT_SIMS}")
    print(f"  衰减: {INIT_DECAY}")
    
    # 5. 批量模拟 - 循环执行每个中性化配置
    print(f"\n[5/5] 批量模拟...")
    print(f"  示例任务: {pools[0][0] if pools else 'N/A'}")
    
    total_neutralizations = len(NEUTRALIZATIONS)
    
    for idx, neutralization in enumerate(NEUTRALIZATIONS, 1):
        print("\n" + "=" * 70)
        print(f"执行中性化配置 [{idx}/{total_neutralizations}]: {neutralization}")
        print("=" * 70)
        
        #
