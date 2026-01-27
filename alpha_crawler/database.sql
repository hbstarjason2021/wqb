-- WorldQuant Alpha数据爬虫数据库表结构（简化版）
-- 创建数据库和单一总表结构

CREATE DATABASE IF NOT EXISTS template_and_inventory_entry CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE template_and_inventory_entry;

-- Alpha数据总表（简化版）
CREATE TABLE IF NOT EXISTS alphas (
    -- ==================== 基础信息字段 ====================
    id VARCHAR(20) PRIMARY KEY COMMENT 'Alpha唯一标识符(如:USA_HBAJSSG)',
    type VARCHAR(20) NOT NULL COMMENT 'Alpha类型(REGULAR:常规/SUPER:超级组合)',
    author VARCHAR(20) NOT NULL COMMENT 'Alpha作者用户名',
    date_created DATETIME NOT NULL COMMENT 'Alpha创建时间(UTC)',
    date_submitted DATETIME COMMENT 'Alpha提交时间(UTC,未提交为NULL)',
    date_modified DATETIME NOT NULL COMMENT 'Alpha最后修改时间(UTC)',
    name VARCHAR(255) COMMENT 'Alpha名称(用户自定义)',
    favorite BOOLEAN DEFAULT FALSE COMMENT '是否收藏(true:已收藏/false:未收藏)',
    hidden BOOLEAN DEFAULT FALSE COMMENT '是否隐藏(true:已隐藏/false:显示)',
    color VARCHAR(20) COMMENT 'Alpha标记颜色(如:red/blue/green)',
    category VARCHAR(100) COMMENT 'Alpha分类(用户自定义分类)',
    stage VARCHAR(20) COMMENT 'Alpha阶段(IS:样本内/OS:样本外)',
    status VARCHAR(20) COMMENT 'Alpha状态(ACTIVE:活跃/INACTIVE:未激活)',
    grade VARCHAR(20) COMMENT 'Alpha评级(S/A/B/C/D/F)',
    
    -- ==================== 设置信息字段 ====================
    instrument_type VARCHAR(20) COMMENT '交易工具类型(EQUITY:股票)',
    region VARCHAR(10) COMMENT '交易区域(USA:美国/CHN:中国/JPN:日本等)',
    universe VARCHAR(50) COMMENT '股票池(TOP3000:前3000只/TOP500:前500只等)',
    delay INT COMMENT '信号延迟(0:当天/1:隔天)',
    decay INT COMMENT '信号衰减期(天数)',
    neutralization VARCHAR(50) COMMENT '中性化方式(SUBINDUSTRY:子行业/INDUSTRY:行业/SECTOR:板块)',
    truncation DECIMAL(5,3) COMMENT '截断值(0.01-0.10,超出部分截断)',
    pasteurization VARCHAR(10) COMMENT '巴氏消毒(ON:开启/OFF:关闭,去除极端值)',
    unit_handling VARCHAR(20) COMMENT '单位处理方式(VERIFY:验证/CONFLICT:冲突处理)',
    nan_handling VARCHAR(20) COMMENT 'NaN值处理(SKIP:跳过/ZERO:填充0)',
    selection_handling VARCHAR(20) COMMENT '选股处理方式(SUPER类型特有:TOP/BOTTOM)',
    selection_limit INT COMMENT '选股数量限制(SUPER类型特有)',
    max_trade VARCHAR(10) COMMENT '最大交易百分比',
    language VARCHAR(20) COMMENT 'Alpha表达式语言(REGULAR/FAST)',
    visualization BOOLEAN COMMENT '是否启用可视化(true:启用/false:禁用)',
    start_date DATE COMMENT '回测开始日期',
    end_date DATE COMMENT '回测结束日期',
    component_activation VARCHAR(10) COMMENT '组件激活方式(SUPER类型特有)',
    test_period VARCHAR(10) COMMENT '测试周期(SUPER类型特有)',
    
    -- ==================== Alpha代码和描述 ====================
    code TEXT NOT NULL COMMENT 'Alpha表达式代码(完整的计算公式)',
    description TEXT COMMENT 'Alpha描述(用户添加的说明文档)',
    operator_count INT COMMENT '算子数量(表达式中使用的算子个数)',
    
    -- ==================== 代码解析字段(仅REGULAR类型有效) ====================
    template_expression TEXT COMMENT '模版表达式(将数据字段替换为占位符[vec]后的表达式,如:ts_kurtosis(ts_arg_max(winsorize(ts_backfill(vec_sum([vec]), 120), std=4), 504), 5))',
    operators_list JSON COMMENT '操作符列表(JSON数组,表达式中使用的所有操作符,如:["ts_kurtosis","ts_arg_max","winsorize","ts_backfill","vec_sum"])',
    data_fields_list JSON COMMENT '数据字段列表(JSON数组,表达式中使用的所有数据字段,如:["oth553_sal_yearspeakcnt"])',
    data_fields_type_list JSON COMMENT '数据字段类型列表(JSON数组,与data_fields_list一一对应,如:["MATRIX","VECTOR","GROUP"])',
    datasets_list JSON COMMENT '数据集列表(JSON数组,表达式中使用的所有数据集ID,从data_fields中提取,如:["oth553","fnd14"])',
    
    -- ==================== SUPER类型特有字段 - Combo组合信息 ====================
    combo_code TEXT COMMENT 'Combo代码(SUPER类型:组合多个Alpha的代码)',
    combo_description TEXT COMMENT 'Combo描述(SUPER类型:组合策略说明)',
    combo_operator_count INT COMMENT 'Combo算子数量(SUPER类型:组合表达式算子数)',
    
    -- ==================== SUPER类型特有字段 - Selection选股信息 ====================
    selection_code TEXT COMMENT 'Selection代码(SUPER类型:选股逻辑代码)',
    selection_description TEXT COMMENT 'Selection描述(SUPER类型:选股策略说明)',
    selection_operator_count INT COMMENT 'Selection算子数量(SUPER类型:选股表达式算子数)',
    
    -- ==================== 标签和分类信息(JSON) ====================
    tags JSON COMMENT '标签列表(JSON数组,如:["momentum","value","quality"])',
    classifications JSON COMMENT '分类信息(JSON对象,包含行业/风格等分类)',
    
    -- ==================== IS阶段(样本内)性能指标 ====================
    pnl DECIMAL(15,2) COMMENT 'IS阶段PnL(损益,单位:美元)',
    book_size DECIMAL(15,2) COMMENT 'IS阶段账面规模(单位:美元)',
    long_count INT COMMENT 'IS阶段多头持仓数量(平均值)',
    short_count INT COMMENT 'IS阶段空头持仓数量(平均值)',
    turnover DECIMAL(10,4) COMMENT 'IS阶段换手率(日均交易占比)',
    returns DECIMAL(10,4) COMMENT 'IS阶段收益率(年化)',
    drawdown DECIMAL(10,4) COMMENT 'IS阶段最大回撤(负值表示亏损)',
    margin DECIMAL(10,6) COMMENT 'IS阶段平均日收益(PnL/BookSize/交易天数)',
    sharpe DECIMAL(10,4) COMMENT 'IS阶段夏普比率(收益/风险,越高越好)',
    fitness DECIMAL(10,4) COMMENT 'IS阶段Fitness(收益/最大回撤,类似Calmar比率)',
    is_start_date DATE COMMENT 'IS阶段开始日期',
    
    -- ==================== 投资约束性能指标(考虑实际可投资性) ====================
    investability_constrained_pnl DECIMAL(15,2) COMMENT '投资约束PnL(考虑流动性等实际约束后的损益)',
    investability_constrained_book_size DECIMAL(15,2) COMMENT '投资约束账面规模',
    investability_constrained_long_count INT COMMENT '投资约束多头持仓数',
    investability_constrained_short_count INT COMMENT '投资约束空头持仓数',
    investability_constrained_turnover DECIMAL(10,4) COMMENT '投资约束换手率',
    investability_constrained_returns DECIMAL(10,4) COMMENT '投资约束收益率',
    investability_constrained_drawdown DECIMAL(10,4) COMMENT '投资约束最大回撤',
    investability_constrained_margin DECIMAL(10,6) COMMENT '投资约束平均日收益',
    investability_constrained_fitness DECIMAL(10,4) COMMENT '投资约束Fitness',
    investability_constrained_sharpe DECIMAL(10,4) COMMENT '投资约束夏普比率',
    
    -- ==================== 风险中性化性能指标(去除市场/行业风险) ====================
    risk_neutralized_pnl DECIMAL(15,2) COMMENT '风险中性化PnL(去除系统性风险后的损益)',
    risk_neutralized_book_size DECIMAL(15,2) COMMENT '风险中性化账面规模',
    risk_neutralized_long_count INT COMMENT '风险中性化多头持仓数',
    risk_neutralized_short_count INT COMMENT '风险中性化空头持仓数',
    risk_neutralized_turnover DECIMAL(10,4) COMMENT '风险中性化换手率',
    risk_neutralized_returns DECIMAL(10,4) COMMENT '风险中性化收益率',
    risk_neutralized_drawdown DECIMAL(10,4) COMMENT '风险中性化最大回撤',
    risk_neutralized_margin DECIMAL(10,6) COMMENT '风险中性化平均日收益',
    risk_neutralized_fitness DECIMAL(10,4) COMMENT '风险中性化Fitness',
    risk_neutralized_sharpe DECIMAL(10,4) COMMENT '风险中性化夏普比率',
    
    -- ==================== 相关性指标 ====================
    pc_value DECIMAL(10,6) COMMENT 'PC值(Production Correlation,与生产环境Alpha的相关性,越低越好)',
    
    -- ==================== 时间序列数据(JSON格式) ====================
    pnl_data JSON COMMENT 'PnL时间序列(JSON数组,包含每日PnL数据点)',
    
    -- 年度统计数据（JSON格式存储yearly-stats数据）
    yearly_stats_data JSON COMMENT '年度统计数据（包含每年的sharpe/fitness/margin等指标）',
    
    -- 提交评分相关字段（因子提交前的综合质量评估，包含7大维度的详细评分数据）
    submission_scores JSON COMMENT '提交评分总分数据：基础分(200分)、PC加分(80分)、总分(280/200分)、评级(S/A/B/C/D/F)、各项加权分值',
    stability_scores JSON COMMENT '稳定性评分(35分满分)：稳健性总分、5个维度分数(多空比/夏普标准差/收益标准差/夏普>1占比/收益回撤比)、数据覆盖年限',
    recent_performance_scores JSON COMMENT '近期表现评分(55分满分)：Sharpe分(权重50%)、Fitness分(权重30%)、Margin分(权重20%)、总体和近期3年指标值',
    turnover_drawdown_scores JSON COMMENT '换手率和回撤评分(30分满分)：换手率分(25%)、回撤分(25%)、收益分(25%)、覆盖率分(25%)、总体和近期3年指标值',
    position_quality_scores JSON COMMENT '多空质量评分(25分满分)：覆盖率分(权重52%)、平衡性分(权重48%)、多空持仓数、股票池、最小要求',
    trend_scores JSON COMMENT '趋势评分(40分满分)：PnL趋势分(权重50%)、Sharpe趋势分(权重37.5%)、波动性分(权重12.5%)、斜率/R²/趋势描述',
    sc_ppac_scores JSON COMMENT 'SC&PPAC相关性评分(15分满分)：SC分(权重50%)、PPAC分(权重50%)、SC值、PPAC值',
    pc_correlation_scores JSON COMMENT 'PC相关性评分(80分加分项)：PC分(80分制)、换算百分制、PC值、是否有PC数据标识(无PC数据时为null)',
    submission_scored_at TIMESTAMP NULL COMMENT '提交评分完成时间(UTC时间戳)',
    
    -- ==================== 相关性分析字段 ====================
    self_correlation DECIMAL(10,6) COMMENT 'SC值(Self Correlation,自相关性,Alpha与自身历史的相关性)',
    ppa_correlation DECIMAL(10,6) COMMENT 'PPAC值(Prior Period Alpha Correlation,前期Alpha相关性)',
    
    -- ==================== 稳健性评分字段(用于评估Alpha的稳定性) ====================
    robustness_score DECIMAL(10,4) COMMENT '总体稳健性评分(0-1分值,越高越稳定)',
    robustness_long_short_ratio DECIMAL(10,4) COMMENT '多空比率稳健性得分(多空持仓比例的稳定性)',
    robustness_sharpe_std DECIMAL(10,4) COMMENT '夏普稳健性得分(Sharpe标准差的稳定性)',
    robustness_returns_std DECIMAL(10,4) COMMENT '收益稳健性得分(Returns标准差的稳定性)',
    robustness_sharpe_gt1_ratio DECIMAL(10,4) COMMENT '夏普>1占比得分(Sharpe>1的年份占比)',
    robustness_return_drawdown_ratio DECIMAL(10,4) COMMENT '收益回撤比稳健性得分(Returns/Drawdown比例的稳定性)',
    robustness_raw_data JSON COMMENT '稳健性分析原始数据(JSON对象,包含详细的分年度数据)',
    robustness_updated_at TIMESTAMP NULL COMMENT '稳健性数据更新时间(UTC时间戳)',
    
    -- ==================== 时间序列数据(JSON格式) ====================
    sharpe_data JSON COMMENT 'Sharpe时间序列(JSON数组,包含每日或每期Sharpe值)',
    
    -- ==================== 验证和检查结果(JSON格式) ====================
    checks JSON COMMENT 'Alpha检查结果(JSON数组,包含各项验证检查的结果)',
 
    -- ==================== 竞争和排名信息(JSON格式) ====================
    competitions JSON COMMENT '竞赛信息(JSON数组,包含Alpha参加的各项竞赛及排名)',
    
    -- ==================== 金字塔和主题信息(JSON格式) ====================
    pyramids JSON COMMENT '金字塔信息(JSON数组,Alpha所属的策略金字塔)',
    themes JSON COMMENT '主题信息(JSON数组,Alpha所属的投资主题)',
    
    -- ==================== 系统管理字段 ====================
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间(本地数据库时间)',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间(本地数据库时间)',
    
    -- ==================== 索引定义(加速常用查询) ====================
    INDEX idx_author (author) COMMENT '作者索引(用于按作者查询)',
    INDEX idx_date_created (date_created) COMMENT '创建日期索引(用于按时间排序)',
    INDEX idx_status (status) COMMENT '状态索引(用于筛选活跃/未激活Alpha)',
    INDEX idx_stage (stage) COMMENT '阶段索引(用于筛选IS/OS阶段)',
    INDEX idx_region (region) COMMENT '区域索引(用于筛选不同市场)',
    INDEX idx_instrument_type (instrument_type) COMMENT '工具类型索引(用于筛选股票/期货等)',
    INDEX idx_sharpe (sharpe) COMMENT '夏普索引(用于按Sharpe排序)',
    INDEX idx_fitness (fitness) COMMENT 'Fitness索引(用于按Fitness排序)',
    INDEX idx_robustness_score (robustness_score) COMMENT '稳健性评分索引(用于按稳健性排序)'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Alpha因子数据主表(存储所有Alpha的完整信息和性能数据)';

-- ==================== 已提交Alpha数据表(专门存储已提交到生产环境的Alpha) ====================
CREATE TABLE IF NOT EXISTS submitted_alphas (
    -- ==================== 基础信息字段 ====================
    id VARCHAR(20) PRIMARY KEY COMMENT 'Alpha唯一标识符(如:USA_HBAJSSG)',
    type VARCHAR(20) NOT NULL COMMENT 'Alpha类型(REGULAR:常规/SUPER:超级组合)',
    author VARCHAR(20) NOT NULL COMMENT 'Alpha作者用户名',
    date_created DATETIME NOT NULL COMMENT 'Alpha创建时间(UTC)',
    date_submitted DATETIME COMMENT 'Alpha提交时间(UTC,已提交必有值)',
    date_modified DATETIME NOT NULL COMMENT 'Alpha最后修改时间(UTC)',
    name VARCHAR(255) COMMENT 'Alpha名称(用户自定义)',
    favorite BOOLEAN DEFAULT FALSE COMMENT '是否收藏(true:已收藏/false:未收藏)',
    hidden BOOLEAN DEFAULT FALSE COMMENT '是否隐藏(true:已隐藏/false:显示)',
    color VARCHAR(20) COMMENT 'Alpha标记颜色(如:red/blue/green)',
    category VARCHAR(100) COMMENT 'Alpha分类(用户自定义分类)',
    stage VARCHAR(20) COMMENT 'Alpha阶段(IS:样本内/OS:样本外)',
    status VARCHAR(20) COMMENT 'Alpha状态(ACTIVE:活跃/INACTIVE:未激活)',
    grade VARCHAR(20) COMMENT 'Alpha评级(S/A/B/C/D/F)',
    
    -- ==================== 设置信息字段 ====================
    instrument_type VARCHAR(20) COMMENT '交易工具类型(EQUITY:股票)',
    region VARCHAR(10) COMMENT '交易区域(USA:美国/CHN:中国/JPN:日本等)',
    universe VARCHAR(50) COMMENT '股票池(TOP3000:前3000只/TOP500:前500只等)',
    delay INT COMMENT '信号延迟(0:当天/1:隔天)',
    decay INT COMMENT '信号衰减期(天数)',
    neutralization VARCHAR(50) COMMENT '中性化方式(SUBINDUSTRY:子行业/INDUSTRY:行业/SECTOR:板块)',
    truncation DECIMAL(5,3) COMMENT '截断值(0.01-0.10,超出部分截断)',
    pasteurization VARCHAR(10) COMMENT '巴氏消毒(ON:开启/OFF:关闭,去除极端值)',
    unit_handling VARCHAR(20) COMMENT '单位处理方式(VERIFY:验证/CONFLICT:冲突处理)',
    nan_handling VARCHAR(20) COMMENT 'NaN值处理(SKIP:跳过/ZERO:填充0)',
    selection_handling VARCHAR(20) COMMENT '选股处理方式(SUPER类型特有:TOP/BOTTOM)',
    selection_limit INT COMMENT '选股数量限制(SUPER类型特有)',
    max_trade VARCHAR(10) COMMENT '最大交易百分比',
    language VARCHAR(20) COMMENT 'Alpha表达式语言(REGULAR/FAST)',
    visualization BOOLEAN COMMENT '是否启用可视化(true:启用/false:禁用)',
    start_date DATE COMMENT '回测开始日期',
    end_date DATE COMMENT '回测结束日期',
    component_activation VARCHAR(10) COMMENT '组件激活方式(SUPER类型特有)',
    test_period VARCHAR(10) COMMENT '测试周期(SUPER类型特有)',
    
    -- ==================== Alpha代码和描述 ====================
    code TEXT NOT NULL COMMENT 'Alpha表达式代码(完整的计算公式)',
    description TEXT COMMENT 'Alpha描述(用户添加的说明文档)',
    operator_count INT COMMENT '算子数量(表达式中使用的算子个数)',
    
    -- ==================== 代码解析字段(仅REGULAR类型有效) ====================
    template_expression TEXT COMMENT '模版表达式(将数据字段替换为占位符[vec]后的表达式,如:ts_kurtosis(ts_arg_max(winsorize(ts_backfill(vec_sum([vec]), 120), std=4), 504), 5))',
    operators_list JSON COMMENT '操作符列表(JSON数组,表达式中使用的所有操作符,如:["ts_kurtosis","ts_arg_max","winsorize","ts_backfill","vec_sum"])',
    data_fields_list JSON COMMENT '数据字段列表(JSON数组,表达式中使用的所有数据字段,如:["oth553_sal_yearspeakcnt"])',
    data_fields_type_list JSON COMMENT '数据字段类型列表(JSON数组,与data_fields_list一一对应,如:["MATRIX","VECTOR","GROUP"])',
    datasets_list JSON COMMENT '数据集列表(JSON数组,表达式中使用的所有数据集ID,从data_fields中提取,如:["oth553","fnd14"])',
    
    -- ==================== SUPER类型特有字段 - Combo组合信息 ====================
    combo_code TEXT COMMENT 'Combo代码(SUPER类型:组合多个Alpha的代码)',
    combo_description TEXT COMMENT 'Combo描述(SUPER类型:组合策略说明)',
    combo_operator_count INT COMMENT 'Combo算子数量(SUPER类型:组合表达式算子数)',
    
    -- ==================== SUPER类型特有字段 - Selection选股信息 ====================
    selection_code TEXT COMMENT 'Selection代码(SUPER类型:选股逻辑代码)',
    selection_description TEXT COMMENT 'Selection描述(SUPER类型:选股策略说明)',
    selection_operator_count INT COMMENT 'Selection算子数量(SUPER类型:选股表达式算子数)',
    
    -- ==================== 标签和分类信息(JSON) ====================
    tags JSON COMMENT '标签列表(JSON数组,如:["momentum","value","quality"])',
    classifications JSON COMMENT '分类信息(JSON对象,包含行业/风格等分类)',
    
    -- ==================== IS阶段(样本内)性能指标 ====================
    pnl DECIMAL(15,2) COMMENT 'IS阶段PnL(损益,单位:美元)',
    book_size DECIMAL(15,2) COMMENT 'IS阶段账面规模(单位:美元)',
    long_count INT COMMENT 'IS阶段多头持仓数量(平均值)',
    short_count INT COMMENT 'IS阶段空头持仓数量(平均值)',
    turnover DECIMAL(10,4) COMMENT 'IS阶段换手率(日均交易占比)',
    returns DECIMAL(10,4) COMMENT 'IS阶段收益率(年化)',
    drawdown DECIMAL(10,4) COMMENT 'IS阶段最大回撤(负值表示亏损)',
    margin DECIMAL(10,6) COMMENT 'IS阶段平均日收益(PnL/BookSize/交易天数)',
    sharpe DECIMAL(10,4) COMMENT 'IS阶段夏普比率(收益/风险,越高越好)',
    fitness DECIMAL(10,4) COMMENT 'IS阶段Fitness(收益/最大回撤,类似Calmar比率)',
    is_start_date DATE COMMENT 'IS阶段开始日期',
    
    -- ==================== OS阶段(样本外)性能指标 ====================
    os_start_date DATE COMMENT 'OS阶段开始日期(仅已提交Alpha有此数据)',
    
    -- ==================== 生产环境相关性指标 ====================
    prod_correlation DECIMAL(10,6) COMMENT '生产相关性(与生产环境Alpha池的相关性)',
    
    -- ==================== 相关性分析字段 ====================
    self_correlation DECIMAL(10,6) COMMENT 'SC值(Self Correlation,自相关性,Alpha与自身历史的相关性)',
    ppa_correlation DECIMAL(10,6) COMMENT 'PPAC值(Prior Period Alpha Correlation,前期Alpha相关性)',
    
    -- ==================== 金字塔主题信息(已提交Alpha特有) ====================
    pyramid_themes JSON COMMENT '金字塔主题(JSON数组,Alpha在生产环境中所属的主题分类)',
    
    -- ==================== OS阶段特殊指标(已提交Alpha特有) ====================
    os_is_sharpe_ratio DECIMAL(10,6) COMMENT 'OS阶段的IS夏普比率(OS阶段开始时的IS Sharpe)',
    pre_close_sharpe_ratio DECIMAL(10,6) COMMENT '预关闭夏普比率(Alpha关闭前的最后Sharpe值)',
    
    -- ==================== 投资约束性能指标(考虑实际可投资性) ====================
    investability_constrained_pnl DECIMAL(15,2) COMMENT '投资约束PnL(考虑流动性等实际约束后的损益)',
    investability_constrained_book_size DECIMAL(15,2) COMMENT '投资约束账面规模',
    investability_constrained_long_count INT COMMENT '投资约束多头持仓数',
    investability_constrained_short_count INT COMMENT '投资约束空头持仓数',
    investability_constrained_turnover DECIMAL(10,4) COMMENT '投资约束换手率',
    investability_constrained_returns DECIMAL(10,4) COMMENT '投资约束收益率',
    investability_constrained_drawdown DECIMAL(10,4) COMMENT '投资约束最大回撤',
    investability_constrained_margin DECIMAL(10,6) COMMENT '投资约束平均日收益',
    investability_constrained_fitness DECIMAL(10,4) COMMENT '投资约束Fitness',
    investability_constrained_sharpe DECIMAL(10,4) COMMENT '投资约束夏普比率',
    
    -- ==================== 风险中性化性能指标(去除市场/行业风险) ====================
    risk_neutralized_pnl DECIMAL(15,2) COMMENT '风险中性化PnL(去除系统性风险后的损益)',
    risk_neutralized_book_size DECIMAL(15,2) COMMENT '风险中性化账面规模',
    risk_neutralized_long_count INT COMMENT '风险中性化多头持仓数',
    risk_neutralized_short_count INT COMMENT '风险中性化空头持仓数',
    risk_neutralized_turnover DECIMAL(10,4) COMMENT '风险中性化换手率',
    risk_neutralized_returns DECIMAL(10,4) COMMENT '风险中性化收益率',
    risk_neutralized_drawdown DECIMAL(10,4) COMMENT '风险中性化最大回撤',
    risk_neutralized_margin DECIMAL(10,6) COMMENT '风险中性化平均日收益',
    risk_neutralized_fitness DECIMAL(10,4) COMMENT '风险中性化Fitness',
    risk_neutralized_sharpe DECIMAL(10,4) COMMENT '风险中性化夏普比率',
       
    -- ==================== 时间序列数据(JSON格式) ====================
    pnl_data JSON COMMENT 'PnL时间序列(JSON数组,包含每日PnL数据点)',
    
    -- 年度统计数据（JSON格式存储yearly-stats数据）
    yearly_stats_data JSON COMMENT '年度统计数据（包含每年的sharpe/fitness/margin等指标）',
    
    -- 提交评分相关字段（因子提交前的综合质量评估，包含7大维度的详细评分数据）
    submission_scores JSON COMMENT '提交评分总分数据：基础分(200分)、PC加分(80分)、总分(280/200分)、评级(S/A/B/C/D/F)、各项加权分值',
    stability_scores JSON COMMENT '稳定性评分(35分满分)：稳健性总分、5个维度分数(多空比/夏普标准差/收益标准差/夏普>1占比/收益回撤比)、数据覆盖年限',
    recent_performance_scores JSON COMMENT '近期表现评分(55分满分)：Sharpe分(权重50%)、Fitness分(权重30%)、Margin分(权重20%)、总体和近期3年指标值',
    turnover_drawdown_scores JSON COMMENT '换手率和回撤评分(30分满分)：换手率分(25%)、回撤分(25%)、收益分(25%)、覆盖率分(25%)、总体和近期3年指标值',
    position_quality_scores JSON COMMENT '多空质量评分(25分满分)：覆盖率分(权重52%)、平衡性分(权重48%)、多空持仓数、股票池、最小要求',
    trend_scores JSON COMMENT '趋势评分(40分满分)：PnL趋势分(权重50%)、Sharpe趋势分(权重37.5%)、波动性分(权重12.5%)、斜率/R²/趋势描述',
    sc_ppac_scores JSON COMMENT 'SC&PPAC相关性评分(15分满分)：SC分(权重50%)、PPAC分(权重50%)、SC值、PPAC值',
    pc_correlation_scores JSON COMMENT 'PC相关性评分(80分加分项)：PC分(80分制)、换算百分制、PC值、是否有PC数据标识(无PC数据时为null)',
    submission_scored_at TIMESTAMP NULL COMMENT '提交评分完成时间(UTC时间戳)',
    
    -- ==================== 稳健性评分字段(用于评估Alpha的稳定性) ====================
    robustness_score DECIMAL(10,4) COMMENT '总体稳健性评分(0-1分值,越高越稳定)',
    robustness_long_short_ratio DECIMAL(10,4) COMMENT '多空比率稳健性得分(多空持仓比例的稳定性)',
    robustness_sharpe_std DECIMAL(10,4) COMMENT '夏普稳健性得分(Sharpe标准差的稳定性)',
    robustness_returns_std DECIMAL(10,4) COMMENT '收益稳健性得分(Returns标准差的稳定性)',
    robustness_sharpe_gt1_ratio DECIMAL(10,4) COMMENT '夏普>1占比得分(Sharpe>1的年份占比)',
    robustness_return_drawdown_ratio DECIMAL(10,4) COMMENT '收益回撤比稳健性得分(Returns/Drawdown比例的稳定性)',
    robustness_raw_data JSON COMMENT '稳健性分析原始数据(JSON对象,包含详细的分年度数据)',
    robustness_updated_at TIMESTAMP NULL COMMENT '稳健性数据更新时间(UTC时间戳)',
    
    -- ==================== 时间序列数据(JSON格式) ====================
    sharpe_data JSON COMMENT 'Sharpe时间序列(JSON数组,包含每日或每期Sharpe值)',
    
    -- ==================== 验证和检查结果(JSON格式) ====================
    checks JSON COMMENT 'Alpha检查结果(JSON数组,包含各项验证检查的结果)',
    
    -- ==================== 竞争和排名信息(JSON格式) ====================
    competitions JSON COMMENT '竞赛信息(JSON数组,包含Alpha参加的各项竞赛及排名)',
    
    -- ==================== 金字塔和主题信息(JSON格式) ====================
    pyramids JSON COMMENT '金字塔信息(JSON数组,Alpha所属的策略金字塔)',
    themes JSON COMMENT '主题信息(JSON数组,Alpha所属的投资主题)',
    
    -- ==================== 系统管理字段 ====================
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间(本地数据库时间)',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间(本地数据库时间)',
    
    -- ==================== 索引定义(加速常用查询) ====================
    INDEX idx_author (author) COMMENT '作者索引(用于按作者查询)',
    INDEX idx_date_created (date_created) COMMENT '创建日期索引(用于按时间排序)',
    INDEX idx_date_submitted (date_submitted) COMMENT '提交日期索引(用于筛选和排序已提交Alpha)',
    INDEX idx_status (status) COMMENT '状态索引(用于筛选活跃/未激活Alpha)',
    INDEX idx_stage (stage) COMMENT '阶段索引(用于筛选IS/OS阶段)',
    INDEX idx_region (region) COMMENT '区域索引(用于筛选不同市场)',
    INDEX idx_instrument_type (instrument_type) COMMENT '工具类型索引(用于筛选股票/期货等)',
    INDEX idx_sharpe (sharpe) COMMENT '夏普索引(用于按Sharpe排序)',
    INDEX idx_fitness (fitness) COMMENT 'Fitness索引(用于按Fitness排序)',
    INDEX idx_robustness_score (robustness_score) COMMENT '稳健性评分索引(用于按稳健性排序)'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='已提交Alpha数据表(专门存储已提交到生产环境的Alpha,包含OS阶段数据)';

-- ==================== 爬虫状态记录表(记录数据爬取任务的执行状态) ====================
CREATE TABLE IF NOT EXISTS crawl_status (
    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    task_id VARCHAR(50) NOT NULL DEFAULT 'default' COMMENT '任务标识符(用于区分不同的爬虫任务实例)',
    task_type VARCHAR(50) DEFAULT 'alpha_crawl' COMMENT '任务类型(alpha_crawl:Alpha爬取/robustness_analysis:稳健性分析等)',
    task_params JSON COMMENT '任务参数(JSON对象,包含任务的配置参数如limit/offset等)',
    crawl_date DATE NOT NULL COMMENT '爬取日期(任务执行的目标日期)',
    start_time DATETIME COMMENT '任务开始时间(实际爬取开始的时间戳)',
    end_time DATETIME COMMENT '任务结束时间(任务完成或失败的时间戳)',
    duration_seconds INT DEFAULT 0 COMMENT '任务持续时间(秒,end_time-start_time)',
    total_count INT DEFAULT 0 COMMENT '总数量(计划处理的Alpha总数)',
    success_count INT DEFAULT 0 COMMENT '成功数量(成功处理的Alpha数量)',
    error_count INT DEFAULT 0 COMMENT '失败数量(处理失败的Alpha数量)',
    last_offset INT DEFAULT 0 COMMENT '最后偏移量(用于断点续传,记录最后处理的位置)',
    status VARCHAR(30) DEFAULT 'running' COMMENT '任务状态(running:运行中/completed:已完成/failed:失败/paused:暂停)',
    batch_info JSON COMMENT '批次信息(JSON对象,记录各批次的处理详情)',
    error_message TEXT COMMENT '错误信息(任务失败时的详细错误描述)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
    INDEX idx_task_id (task_id) COMMENT '任务ID索引',
    INDEX idx_task_type (task_type) COMMENT '任务类型索引',
    INDEX idx_crawl_date (crawl_date) COMMENT '爬取日期索引',
    INDEX idx_status (status) COMMENT '状态索引',
    INDEX idx_start_time (start_time) COMMENT '开始时间索引'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='爬虫任务状态跟踪表(记录所有数据爬取任务的执行情况)';
