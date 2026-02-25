"""
QRAI-Trader 配置管理
从 .env 文件或环境变量读取配置
"""
import os
from dotenv import load_dotenv

# 加载 .env 文件（项目根目录）
_project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
load_dotenv(os.path.join(_project_root, '.env'))

# ============================================================
# Tushare 多 Token（逗号分隔）
# ============================================================
TUSHARE_TOKENS = [
    t.strip() for t in os.getenv('TUSHARE_TOKENS', '').split(',') if t.strip()
]

# ============================================================
# 数据库配置（3 个 TiDB Serverless 实例）
# 优先读取 DB1_URL / DB2_URL / DB3_URL（URI 格式，推荐用于 GitHub Secrets）
# 格式：mysql+pymysql://user:password@host:port/database
# 若 URL 未设置，回退到独立字段 DB1_HOST / DB1_PORT / ... 兼容旧配置
# ============================================================
def _db_config(prefix: str) -> dict:
    url = os.getenv(f'{prefix}_URL')
    if url:
        return {'url': url}
    return {
        'host': os.getenv(f'{prefix}_HOST'),
        'port': int(os.getenv(f'{prefix}_PORT') or 4000),
        'user': os.getenv(f'{prefix}_USER'),
        'password': os.getenv(f'{prefix}_PASSWORD'),
        'database': os.getenv(f'{prefix}_DATABASE', 'test'),
    }

DB_CONFIGS = {
    'db1': _db_config('DB1'),  # 核心行情
    'db2': _db_config('DB2'),  # 资金与筹码
    'db3': _db_config('DB3'),  # 策略与辅助
}

# ============================================================
# Notion
# ============================================================
NOTION_TOKEN = os.getenv('NOTION_TOKEN')
NOTION_PARENT_PAGE_ID = os.getenv('NOTION_PARENT_PAGE_ID')

# ============================================================
# 数据回补参数
# ============================================================
BACKFILL_START_DATE = '20210101'  # 5年回补起始日期
BACKFILL_END_DATE = '20260210'    # 回补截止日期

# ============================================================
# API 频率控制
# ============================================================
API_CALL_INTERVAL = 0.15  # 秒，单次 Tushare 调用最小间隔
TOKEN_COOLDOWN_SECONDS = int(os.getenv('TOKEN_COOLDOWN_SECONDS') or 60)

# ============================================================
# Phase 3: 筹码胜率并发参数
# ============================================================
CYQ_PERF_WORKERS = int(os.getenv('CYQ_PERF_WORKERS') or 8)
CYQ_PERF_BATCH = int(os.getenv('CYQ_PERF_BATCH') or 30)

# ============================================================
# 量化阈值基准表（来自策略.md）
# ============================================================
QUANT_THRESHOLDS = {
    # 基础过滤
    # 注意单位：circ_mv 数据单位为万元，amount 数据单位为千元
    'circ_mv_min': 50 * 1e4,            # 流通市值下限 50亿元 → 500,000 万元
    'amount_min': 2 * 1e5,              # 当日成交额 > 2亿元 → 200,000 千元
    'avg_amount_20d_min': 2 * 1e5,      # 20日均成交额 > 2亿元 → 200,000 千元
    'turnover_rate_min': 1.2,           # 换手率 > 1.2%（数据已是百分比，无需转换）
    'turnover_amount_or_min': 4 * 1e5,  # 或成交额 > 4亿元 → 400,000 千元
    # PE 过滤：
    #   pe_theme_split_enabled=False 时，所有股票统一使用 pe_ttm_max（当前默认 100）
    #   pe_theme_split_enabled=True  时，命中十五五主题用 pe_ttm_max，非主题用 pe_ttm_strict
    # 当 policy_theme_stocks 概念库完善后，可将 pe_theme_split_enabled 置为 True，
    # 同时把 pe_ttm_strict 恢复为 60，实现精细化管控。
    'pe_ttm_max': 100,              # PE TTM 宽松上限（十五五主题 / 开关关闭时全体适用）
    'pe_ttm_strict': 60,            # PE TTM 严格上限（仅 pe_theme_split_enabled=True 时对非主题股生效）
    'pe_theme_split_enabled': False,  # False=全体用宽松限制；True=主题宽松/非主题严格

    # 每日复盘推送数量（DB落库 + Markdown报告 + Notion推送共用此值）
    # 可通过环境变量 DAILY_TOP_N 覆盖，未设置时默认 100
    'daily_top_n': int(os.getenv('DAILY_TOP_N') or 100),

    # 动量 / RPS
    'rps_20_min': 90,               # 20日RPS > 90
    'rps_60_min': 85,               # 60日RPS > 85

    # 形态识别
    'g_point_vol_ratio': 3.0,       # G点倍量系数 (vs MA(Vol,30))
    'ma_convergence_pct': 5.0,      # 均线粘连度 < 5%
    'l_shape_vol_ratio': 0.2,       # 缩量L型比例 < 20%

    # 博弈与风控
    'auction_vol_ratio': 0.1,       # 竞价量比 > 10%
    'auction_gap_min': 1.0,         # 竞价高开下限 1%
    'auction_gap_max': 3.5,         # 竞价高开上限 3.5%
    'auction_gap_penalty_start': 4.5,  # 竞价高开降分起点 > 4.5%
    'intraday_deviation_pct': 3.0,  # 分时做T乖离率 ±3%
    'skyline_lookback': 3,          # 天际线回溯 K 线根数
    'winner_rate_high': 90.0,       # 获利盘高占比 > 90%
}

# ============================================================
# 策略权重配置
# ============================================================
STRATEGY_WEIGHTS = {
    'macro_calendar': 0.30,   # 天时（宏观/日历）
    'pattern_quant':  0.30,   # 地利（形态/量化）
    'sentiment_rps':  0.20,   # 人和（情绪/筹码）
    'intraday_game':  0.20,   # 弈  （日内博弈）
}

# 夜间复盘对弈维度折算权重（文档定义 5%-10%）
NIGHTLY_INTRADAY_WEIGHT = 0.08

# 全局熔断器：若跌停家数过高或上涨家数占比过低，则评分整体折减
GLOBAL_MELTDOWN_DOWN_LIMIT_COUNT = int(os.getenv('GLOBAL_MELTDOWN_DOWN_LIMIT_COUNT') or 30)
GLOBAL_MELTDOWN_UP_RATIO = float(os.getenv('GLOBAL_MELTDOWN_UP_RATIO') or 0.20)
GLOBAL_MELTDOWN_SCORE_MULTIPLIER = float(os.getenv('GLOBAL_MELTDOWN_SCORE_MULTIPLIER') or 0.2)

# 总分入选阈值
STRATEGY_SCORE_THRESHOLD = 80

# ============================================================
# 宏观策略（S11/S12）参数
# ============================================================
POLICY_THEME_KEYWORDS = [
    '人工智能', 'AI', '算力', '数据中心', '机器人', '自动化',
    '低空', '无人机', '半导体', '芯片', '信创', '网络安全',
    '新能源', '储能', '新材料',
]

# 示例："20260110,20260305"，表示政策催化日；命中后窗口期加分
POLICY_CATALYST_DATES = [
    d.strip() for d in os.getenv('POLICY_CATALYST_DATES', '').split(',') if d.strip()
]
POLICY_CATALYST_WINDOW_DAYS = int(os.getenv('POLICY_CATALYST_WINDOW_DAYS') or 5)

CALENDAR_BIAS_MAP = {
    1: 'AGGRESSIVE',
    4: 'DEFENSIVE',
    10: 'VALUE',
}

# ============================================================
# 核心监控指数列表
# ============================================================
CORE_INDEXES = [
    '000001.SH',  # 上证指数
    '399001.SZ',  # 深证成指
    '399006.SZ',  # 创业板指
    '000300.SH',  # 沪深300
    '000905.SH',  # 中证500
    '000852.SH',  # 中证1000
    '399303.SZ',  # 国证2000
]

INDEX_DAILYBASIC_CODES = [
    '000001.SH',  # 上证综指
    '399001.SZ',  # 深证成指
    '000016.SH',  # 上证50
    '000905.SH',  # 中证500
    '399005.SZ',  # 中小板指
    '399006.SZ',  # 创业板指
    '000300.SH',  # 沪深300
]
