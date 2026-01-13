'''
WorldQuant Brain 完整操作符配置
自动生成于operators提取脚本
包含所有151个操作符的详细分类
'''

# ==================== 按参数数量分类 ====================

# 0 Param: 8个
PARAM_0 = ['equal', 'greater', 'greater_equal', 'in', 'less', 'less_equal', 'not_equal', 'universe_size']

# 1 Param: 46个 - 可直接应用于单字段
PARAM_1 = ['abs', 'arc_tan', 'bucket', 'clamp', 'days_from_last_change', 'densify', 'hump', 'hump_decay', 'inverse', 'is_nan', 'is_not_finite', 'left_tail', 'log', 'normalize', 'not', 'pasteurize', 'purify', 'quantile', 'rank', 'reverse', 'right_tail', 'round', 'round_down', 's_log_1p', 'scale', 'scale_down', 'sigmoid', 'sign', 'sqrt', 'tail', 'tanh', 'to_nan', 'truncate', 'ts_backfill', 'ts_step', 'ts_target_tvr_decay', 'ts_target_tvr_hump', 'vec_avg', 'vec_count', 'vec_max', 'vec_min', 'vec_norm', 'vec_stddev', 'vec_sum', 'winsorize', 'zscore']

# 2 Param: 59个 - 需要2个参数（字段+参数，或字段+字段）
PARAM_2 = ['add', 'and', 'divide', 'group_cartesian_product', 'group_count', 'group_max', 'group_median', 'group_min', 'group_neutralize', 'group_normalize', 'group_rank', 'group_scale', 'group_std_dev', 'group_sum', 'group_zscore', 'inst_tvr', 'jump_decay', 'keep', 'last_diff_value', 'min', 'nan_mask', 'or', 'power', 'regression_neut', 'regression_proj', 'signed_power', 'subtract', 'ts_arg_max', 'ts_arg_min', 'ts_av_diff', 'ts_count_nans', 'ts_decay_exp_window', 'ts_decay_linear', 'ts_delay', 'ts_delta', 'ts_delta_limit', 'ts_entropy', 'ts_ir', 'ts_kurtosis', 'ts_max', 'ts_max_diff', 'ts_mean', 'ts_median', 'ts_min', 'ts_min_diff', 'ts_min_max_cps', 'ts_min_max_diff', 'ts_product', 'ts_quantile', 'ts_rank', 'ts_returns', 'ts_scale', 'ts_skewness', 'ts_std_dev', 'ts_sum', 'ts_target_tvr_delta_limit', 'ts_zscore', 'vector_neut', 'vector_proj']

# 3 Param: 15个
PARAM_3 = ['group_backfill', 'group_extra', 'group_mean', 'group_vector_neut', 'if_else', 'kth_element', 'max', 'multiply', 'trade_when', 'ts_co_skewness', 'ts_corr', 'ts_covariance', 'ts_regression', 'ts_vector_neut', 'ts_vector_proj']

# 4 Param: 5个
PARAM_4 = ['filter', 'group_multi_regression', 'multi_regression', 'ts_rank_gmean_amean_diff', 'ts_triple_corr']

# Special: 18个 - COMBO专用或特殊用途
SPECIAL = ['combo_a', 'generate_stats', 'inst_pnl', 'reduce_avg', 'reduce_choose', 'reduce_count', 'reduce_ir', 'reduce_kurtosis', 'reduce_max', 'reduce_min', 'reduce_norm', 'reduce_percentage', 'reduce_powersum', 'reduce_range', 'reduce_skewness', 'reduce_stddev', 'reduce_sum', 'self_corr']


# ==================== 按功能类别分类 ====================

# Arithmetic Category: 25个
CATEGORY_ARITHMETIC = ['abs', 'add', 'arc_tan', 'densify', 'divide', 'inverse', 'log', 'max', 'min', 'multiply', 'nan_mask', 'pasteurize', 'power', 'purify', 'reverse', 'round', 'round_down', 's_log_1p', 'sigmoid', 'sign', 'signed_power', 'sqrt', 'subtract', 'tanh', 'to_nan']

# Cross Sectional Category: 13个
CATEGORY_CROSS_SECTIONAL = ['multi_regression', 'normalize', 'quantile', 'rank', 'regression_neut', 'regression_proj', 'scale', 'scale_down', 'truncate', 'vector_neut', 'vector_proj', 'winsorize', 'zscore']

# Group Category: 18个
CATEGORY_GROUP = ['combo_a', 'group_backfill', 'group_cartesian_product', 'group_count', 'group_extra', 'group_max', 'group_mean', 'group_median', 'group_min', 'group_multi_regression', 'group_neutralize', 'group_normalize', 'group_rank', 'group_scale', 'group_std_dev', 'group_sum', 'group_vector_neut', 'group_zscore']

# Logical Category: 12个
CATEGORY_LOGICAL = ['and', 'equal', 'greater', 'greater_equal', 'if_else', 'is_nan', 'is_not_finite', 'less', 'less_equal', 'not', 'not_equal', 'or']

# Reduce Category: 14个
CATEGORY_REDUCE = ['reduce_avg', 'reduce_choose', 'reduce_count', 'reduce_ir', 'reduce_kurtosis', 'reduce_max', 'reduce_min', 'reduce_norm', 'reduce_percentage', 'reduce_powersum', 'reduce_range', 'reduce_skewness', 'reduce_stddev', 'reduce_sum']

# Special Category: 4个
CATEGORY_SPECIAL = ['in', 'inst_pnl', 'self_corr', 'universe_size']

# Time Series Category: 49个
CATEGORY_TIME_SERIES = ['days_from_last_change', 'hump', 'hump_decay', 'inst_tvr', 'jump_decay', 'kth_element', 'last_diff_value', 'ts_arg_max', 'ts_arg_min', 'ts_av_diff', 'ts_backfill', 'ts_co_skewness', 'ts_corr', 'ts_count_nans', 'ts_covariance', 'ts_decay_exp_window', 'ts_decay_linear', 'ts_delay', 'ts_delta', 'ts_delta_limit', 'ts_entropy', 'ts_ir', 'ts_kurtosis', 'ts_max', 'ts_max_diff', 'ts_mean', 'ts_median', 'ts_min', 'ts_min_diff', 'ts_min_max_cps', 'ts_min_max_diff', 'ts_product', 'ts_quantile', 'ts_rank', 'ts_rank_gmean_amean_diff', 'ts_regression', 'ts_returns', 'ts_scale', 'ts_skewness', 'ts_std_dev', 'ts_step', 'ts_sum', 'ts_target_tvr_decay', 'ts_target_tvr_delta_limit', 'ts_target_tvr_hump', 'ts_triple_corr', 'ts_vector_neut', 'ts_vector_proj', 'ts_zscore']

# Transformational Category: 9个
CATEGORY_TRANSFORMATIONAL = ['bucket', 'clamp', 'filter', 'generate_stats', 'keep', 'left_tail', 'right_tail', 'tail', 'trade_when']

# Vector Category: 7个
CATEGORY_VECTOR = ['vec_avg', 'vec_count', 'vec_max', 'vec_min', 'vec_norm', 'vec_stddev', 'vec_sum']


# ==================== 常用操作符组合 ====================

# 适合单字段直接应用（1个参数）
OPERATORS_SINGLE_FIELD = PARAM_1

# 适合双字段组合（2个参数）
OPERATORS_DUAL_FIELD = PARAM_2

# 时间序列操作符（需要指定窗口期）
OPERATORS_TIME_SERIES = [op for op in CATEGORY_TIME_SERIES if op in PARAM_2 or op in PARAM_1]

# 横截面操作符
OPERATORS_CROSS_SECTIONAL = [op for op in CATEGORY_CROSS_SECTIONAL if op in PARAM_1]

# 算术操作符  
OPERATORS_ARITHMETIC = [op for op in CATEGORY_ARITHMETIC if op in PARAM_1]

# 逻辑操作符
OPERATORS_LOGICAL = [op for op in CATEGORY_LOGICAL if op in PARAM_1 or op in PARAM_2]

# 分组操作符（需要group参数）
OPERATORS_GROUP = CATEGORY_GROUP

# 向量操作符
OPERATORS_VECTOR = CATEGORY_VECTOR

# 全部REGULAR可用操作符（排除COMBO专用和SELECTION专用）
OPERATORS_ALL_REGULAR = PARAM_1 + PARAM_2 + PARAM_3 + PARAM_4

# 时间序列窗口参数配置
TS_WINDOWS = [5, 10, 20, 60, 120, 252]

# 分组字段配置
GROUP_FIELDS = ['subindustry', 'industry', 'sector']

if __name__ == "__main__":
    print(f"✓ 配置已加载")
    print(f"  - PARAM_1 (单参数): {len(PARAM_1)}个")
    print(f"  - PARAM_2 (双参数): {len(PARAM_2)}个")
    print(f"  - PARAM_3 (三参数): {len(PARAM_3)}个")
    print(f"  - PARAM_4 (四参数): {len(PARAM_4)}个")
    print(f"  - SPECIAL (特殊): {len(SPECIAL)}个")
    print(f"  - 总计REGULAR可用: {len(OPERATORS_ALL_REGULAR)}个")
