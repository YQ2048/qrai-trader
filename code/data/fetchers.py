"""
QRAI-Trader 数据拉取器
负责从 Tushare 拉取数据并存入 TiDB
支持多 Token 轮询：当某个 Token 调用失败时自动切换到下一个
"""
import time
import threading
import tushare as ts
import pandas as pd
import numpy as np
from sqlalchemy import text
from code.core.config import (
    TUSHARE_TOKENS,
    API_CALL_INTERVAL,
    TOKEN_COOLDOWN_SECONDS,
    BACKFILL_START_DATE,
    BACKFILL_END_DATE,
)
from code.core.db_manager import get_engine


# ============================================================
# 多 Token 轮询管理（线程安全）
# ============================================================
class TushareClient:
    """管理多个 Tushare Token，失败时自动轮询，支持多线程并发调用"""

    def __init__(self, tokens: list):
        if not tokens:
            raise ValueError("至少需要一个 Tushare Token")
        self._states = []
        for token in tokens:
            try:
                pro = ts.pro_api(token)
            except TypeError:
                ts.set_token(token)
                pro = ts.pro_api()
            self._states.append({
                'token': token,
                'pro': pro,
                'lock': threading.Lock(),
                'last_call': 0.0,
                'cooldown_until': 0.0,
            })
        self._rr_idx = 0
        self._rr_lock = threading.Lock()
        preview = ", ".join([f"#{i+1}" for i in range(min(3, len(self._states)))])
        print(f"  Tushare 已加载 {len(self._states)} 个 Token ({preview}{'...' if len(self._states) > 3 else ''})")

    def _next_index(self) -> int:
        with self._rr_lock:
            idx = self._rr_idx
            self._rr_idx = (self._rr_idx + 1) % len(self._states)
            return idx

    def _throttle(self, state: dict):
        """按 Token 进行频率控制，只限制调用间隔，不限制并发"""
        with state['lock']:
            now = time.time()
            wait = API_CALL_INTERVAL - (now - state['last_call'])
            if wait > 0:
                time.sleep(wait)
            state['last_call'] = time.time()

    def call(self, method_name: str, max_retries: int = 3, call_timeout: int = 60, **kwargs):
        """
        调用 Tushare API，失败时自动轮询所有 Token（线程安全）
        method_name: pro 上的方法名，如 'daily', 'stock_basic'
        max_retries: 网络/超时错误的最大重试次数
        call_timeout: 单次 API 调用超时秒数
        """
        from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

        last_error = None
        for retry in range(max_retries):
            attempted = False
            cooldowns = []

            for _ in range(len(self._states)):
                idx = self._next_index()
                state = self._states[idx]
                now = time.time()
                if state['cooldown_until'] > now:
                    cooldowns.append(state['cooldown_until'])
                    continue

                attempted = True
                self._throttle(state)

                try:
                    func = getattr(state['pro'], method_name)
                    # 在子线程中执行 API 调用，带超时保护
                    with ThreadPoolExecutor(max_workers=1) as _pool:
                        future = _pool.submit(func, **kwargs)
                        return future.result(timeout=call_timeout)
                except FuturesTimeout:
                    last_error = TimeoutError(
                        f"Tushare API '{method_name}' 调用超时({call_timeout}s)")
                    break
                except Exception as e:
                    last_error = e
                    err_msg = str(e)
                    # Token/频率限制类错误：进入冷却，尝试下一个 Token
                    if any(kw in err_msg for kw in ['抱歉', '权限', '积分', '频率', 'limit', '403', 'token']):
                        state['cooldown_until'] = time.time() + TOKEN_COOLDOWN_SECONDS
                        continue
                    # 网络类错误，进入重试
                    if any(kw in err_msg.lower() for kw in ['timeout', 'timed out', 'connection', 'reset', 'broken pipe', 'eof']):
                        break
                    raise

            if not attempted and cooldowns:
                wait = max(0.5, min(cooldowns) - time.time())
                time.sleep(wait)
                continue

            if retry < max_retries - 1:
                wait = min(2 ** retry * 2, 15)
                if last_error is not None:
                    print(f"    ⚠ {method_name} 第{retry+1}次失败({str(last_error)[:60]})，{wait}s后重试...")
                time.sleep(wait)

        raise last_error


# 全局客户端实例
client = TushareClient(TUSHARE_TOKENS)


# ============================================================
# DB 列宽度映射（自动截断超长字符串防止入库报错）
# ============================================================
_COLUMN_MAX_LENGTHS = {
    'name': 20, 'industry': 30, 'area': 20, 'cnspell': 20, 'market': 10,
    'exchange': 10, 'fullname': 100, 'act_name': 100, 'act_ent_type': 100,
    'exalter': 200, 'reason': 100, 'buyer': 200, 'seller': 200,
    'holder_name': 100, 'share_type': 30, 'type_name': 20, 'type': 10,
    'summary': 65000, 'change_reason': 65000, 'perf_summary': 65000, 'remark': 65000,
    'up_stat': 20, 'first_time': 20, 'last_time': 20, 'modify_date': 20,
    'industry_name': 30, 'l1_name': 30, 'l2_name': 30, 'l3_name': 30,
    'side': 2, 'market_type': 2, 'limit_type': 2, 'limit': 2,
}


def _clean_df_for_db(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """入库前清洗 DataFrame：截断超长字符串、处理 NaN/Inf、统一类型"""
    df = df.copy()
    for col in df.columns:
        # 1. 截断超长字符串
        max_len = _COLUMN_MAX_LENGTHS.get(col)
        if max_len and df[col].dtype == object:
            df[col] = df[col].apply(
                lambda x: str(x)[:max_len] if isinstance(x, str) and len(str(x)) > max_len else x
            )
        # 2. 将 numpy int64/float64 转为 Python 原生类型（防止 pymysql 不识别）
        if df[col].dtype in ['int64', 'Int64']:
            df[col] = df[col].where(df[col].notna(), None)
        elif df[col].dtype == 'float64':
            # 将 inf/-inf 替换为 None
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)
    # 3. 删除完全重复的行
    df = df.drop_duplicates()
    return df


def _save_to_db(df: pd.DataFrame, table_name: str, db_key: str, if_exists: str = 'append', use_replace: bool = False):
    """将 DataFrame 存入指定数据库表，主键冲突时自动用批量 REPLACE"""
    if df is None or df.empty:
        return 0
    df = _clean_df_for_db(df, table_name)
    if use_replace:
        _upsert_to_db(df, table_name, db_key)
        return len(df)
    engine = get_engine(db_key)
    try:
        df.to_sql(table_name, engine, if_exists=if_exists, index=False, method='multi', chunksize=200)
        return len(df)
    except Exception as e:
        if 'Duplicate entry' in str(e):
            print(f"    [{table_name}] 主键冲突，切换为批量 REPLACE...")
        else:
            print(f"    [{table_name}] to_sql 失败({str(e)[:80]})，回退到 REPLACE...")
        _upsert_to_db(df, table_name, db_key)
        return len(df)
    finally:
        engine.dispose()


def _upsert_to_db(df: pd.DataFrame, table_name: str, db_key: str):
    """使用 pymysql executemany 做批量 REPLACE INTO（带重试）"""
    import pymysql
    import certifi
    from code.core.config import DB_CONFIGS

    cfg = DB_CONFIGS[db_key]
    conn = pymysql.connect(
        host=cfg['host'], port=cfg['port'],
        user=cfg['user'], password=cfg['password'],
        database=cfg['database'],
        ssl={'ca': certifi.where()},
        connect_timeout=30,
        read_timeout=60,
        write_timeout=60,
    )
    try:
        cols = ', '.join([f'`{c}`' for c in df.columns])
        placeholders = ', '.join(['%s'] * len(df.columns))
        sql = f"REPLACE INTO `{table_name}` ({cols}) VALUES ({placeholders})"

        cursor = conn.cursor()
        rows = []
        for _, row in df.iterrows():
            cleaned = []
            for v in row:
                if v is None or (isinstance(v, float) and (pd.isna(v) or np.isinf(v))):
                    cleaned.append(None)
                elif isinstance(v, (np.integer,)):
                    cleaned.append(int(v))
                elif isinstance(v, (np.floating,)):
                    cleaned.append(float(v))
                elif isinstance(v, np.bool_):
                    cleaned.append(bool(v))
                else:
                    cleaned.append(v)
            rows.append(tuple(cleaned))

        batch_size = 500
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            try:
                cursor.executemany(sql, batch)
            except Exception as e:
                # 单批失败时逐条重试，跳过问题行
                err_msg = str(e)
                skipped = 0
                for single_row in batch:
                    try:
                        cursor.execute(sql, single_row)
                    except Exception:
                        skipped += 1
                if skipped > 0:
                    print(f"    [{table_name}] 批次 {i//batch_size}: 跳过 {skipped} 条问题数据")
        conn.commit()
        cursor.close()
    finally:
        conn.close()


def get_trade_dates(start_date: str = None, end_date: str = None) -> list:
    """从数据库获取交易日列表"""
    start_date = start_date or BACKFILL_START_DATE
    end_date = end_date or BACKFILL_END_DATE
    engine = get_engine('db1')
    with engine.connect() as conn:
        result = conn.execute(text(
            f"SELECT cal_date FROM trade_cal WHERE is_open=1 "
            f"AND cal_date >= '{start_date}' AND cal_date <= '{end_date}' "
            f"ORDER BY cal_date"
        ))
        dates = [row[0] for row in result]
    engine.dispose()
    return dates


# ============================================================
# DB1: 核心行情数据拉取
# ============================================================

def fetch_stock_basic():
    """拉取股票基础信息（全量）"""
    print("\n[stock_basic] 拉取股票基础信息...")
    fields = ('ts_code,symbol,name,area,industry,fullname,cnspell,'
              'market,exchange,list_status,list_date,delist_date,is_hs,act_name,act_ent_type')
    df = client.call('stock_basic', exchange='', list_status='L', fields=fields)
    df_d = client.call('stock_basic', exchange='', list_status='D', fields=fields)
    df = pd.concat([df, df_d], ignore_index=True)
    count = _save_to_db(df, 'stock_basic', 'db1', if_exists='replace')
    print(f"  ✓ 共 {count} 条记录")
    return df


def fetch_trade_cal():
    """拉取交易日历"""
    print("\n[trade_cal] 拉取交易日历...")
    df = client.call('trade_cal', exchange='SSE', start_date='20200101', end_date='20261231')
    count = _save_to_db(df, 'trade_cal', 'db1', if_exists='replace')
    print(f"  ✓ 共 {count} 条记录")
    return df


def fetch_daily_by_date(trade_date: str):
    """按日期拉取全市场日线行情"""
    return client.call('daily', trade_date=trade_date)


def fetch_adj_factor_by_date(trade_date: str):
    """按日期拉取全市场复权因子"""
    return client.call('adj_factor', trade_date=trade_date)


def fetch_daily_basic_by_date(trade_date: str):
    """按日期拉取全市场每日指标"""
    return client.call(
        'daily_basic',
        trade_date=trade_date,
        fields='ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,'
               'pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,'
               'total_share,float_share,free_share,total_mv,circ_mv'
    )


def backfill_db1_by_date(trade_dates: list):
    """按日期批量回补 DB1 数据（daily + adj_factor + daily_basic）"""
    total = len(trade_dates)
    print(f"\n[DB1 回补] 共 {total} 个交易日待处理")

    for i, td in enumerate(trade_dates):
        try:
            df_daily = fetch_daily_by_date(td)
            df_adj = fetch_adj_factor_by_date(td)
            df_basic = fetch_daily_basic_by_date(td)

            n1 = _save_to_db(df_daily, 'stock_daily', 'db1')
            n2 = _save_to_db(df_adj, 'adj_factor', 'db1')
            n3 = _save_to_db(df_basic, 'daily_basic', 'db1')

            if (i + 1) % 10 == 0 or i == 0:
                print(f"  [{i+1}/{total}] {td}: daily={n1}, adj={n2}, basic={n3}")

        except Exception as e:
            print(f"  ✗ [{i+1}/{total}] {td}: {e}")
            time.sleep(2)


# ============================================================
# DB2: 资金与筹码数据拉取
# ============================================================

def fetch_moneyflow_by_date(trade_date: str):
    """按日期拉取全市场资金流向"""
    return client.call('moneyflow', trade_date=trade_date)


def fetch_moneyflow_hsgt(start_date: str = None, end_date: str = None):
    """拉取沪深港通资金流向（一次最多300条，按半年分段）"""
    start_date = start_date or BACKFILL_START_DATE
    end_date = end_date or BACKFILL_END_DATE
    print(f"\n[moneyflow_hsgt] 拉取北向资金 {start_date} ~ {end_date}...")

    all_data = []
    # 按半年分段（每段约120个交易日，远小于300条限制）
    from datetime import datetime, timedelta
    dt_start = datetime.strptime(start_date, '%Y%m%d')
    dt_end = datetime.strptime(end_date, '%Y%m%d')

    cur = dt_start
    while cur <= dt_end:
        seg_end = min(cur + timedelta(days=180), dt_end)
        s = cur.strftime('%Y%m%d')
        e = seg_end.strftime('%Y%m%d')
        df = client.call('moneyflow_hsgt', start_date=s, end_date=e)
        if df is not None and not df.empty:
            all_data.append(df)
            print(f"    {s}~{e}: {len(df)} 条")
        cur = seg_end + timedelta(days=1)

    if all_data:
        df_all = pd.concat(all_data, ignore_index=True).drop_duplicates(subset=['trade_date'])
        count = _save_to_db(df_all, 'moneyflow_hsgt', 'db2')
        print(f"  ✓ 共 {count} 条记录")
    else:
        print("  ✗ 无数据")


def fetch_cyq_perf_by_stock(ts_code: str, start_date: str = None, end_date: str = None):
    """按股票拉取筹码胜率数据"""
    from datetime import datetime
    start_date = start_date or BACKFILL_START_DATE
    end_date = end_date or datetime.now().strftime('%Y%m%d')
    return client.call('cyq_perf', ts_code=ts_code, start_date=start_date, end_date=end_date)


def backfill_db2_moneyflow(trade_dates: list):
    """按日期批量回补资金流向"""
    total = len(trade_dates)
    print(f"\n[DB2 moneyflow 回补] 共 {total} 个交易日待处理")

    for i, td in enumerate(trade_dates):
        try:
            df = fetch_moneyflow_by_date(td)
            n = _save_to_db(df, 'moneyflow', 'db2')
            if (i + 1) % 10 == 0 or i == 0:
                print(f"  [{i+1}/{total}] {td}: {n} 条")
        except Exception as e:
            print(f"  ✗ [{i+1}/{total}] {td}: {e}")
            time.sleep(2)


def backfill_db2_cyq_perf(stock_list: list, start_date: str = None, end_date: str = None):
    """按股票批量回补筹码胜率"""
    total = len(stock_list)
    print(f"\n[DB2 cyq_perf 回补] 共 {total} 只股票待处理")

    for i, code in enumerate(stock_list):
        try:
            df = fetch_cyq_perf_by_stock(code, start_date, end_date)
            n = _save_to_db(df, 'cyq_perf', 'db2')
            if (i + 1) % 50 == 0 or i == 0:
                print(f"  [{i+1}/{total}] {code}: {n} 条")
        except Exception as e:
            print(f"  ✗ [{i+1}/{total}] {code}: {e}")
            time.sleep(1)


# ============================================================
# DB3: 策略与辅助数据拉取
# ============================================================

def fetch_stk_limit_by_date(trade_date: str):
    """按日期拉取涨跌停价格"""
    return client.call('stk_limit', trade_date=trade_date)


def fetch_limit_list_d_by_date(trade_date: str):
    """按日期拉取涨跌停和炸板数据"""
    all_data = []
    for limit_type in ['U', 'D', 'Z']:
        df = client.call('limit_list_d', trade_date=trade_date, limit_type=limit_type)
        if df is not None and not df.empty:
            all_data.append(df)
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()


def fetch_stock_st_by_date(trade_date: str):
    """按日期拉取ST股票列表"""
    return client.call('stock_st', trade_date=trade_date)


def fetch_disclosure_date(end_date: str):
    """按报告期拉取财报披露计划"""
    return client.call('disclosure_date', end_date=end_date)


def fetch_forecast_vip(period: str):
    """按报告期拉取全部业绩预告"""
    return client.call(
        'forecast_vip',
        period=period,
        fields='ts_code,ann_date,end_date,type,p_change_min,p_change_max,'
               'net_profit_min,net_profit_max,last_parent_net,first_ann_date,summary,change_reason'
    )


def fetch_express_vip(period: str):
    """按报告期拉取全部业绩快报"""
    return client.call(
        'express_vip',
        period=period,
        fields='ts_code,ann_date,end_date,revenue,operate_profit,total_profit,n_income,'
               'total_assets,total_hldr_eqy_exc_min_int,diluted_eps,diluted_roe,'
               'yoy_net_profit,bps,yoy_sales,yoy_op,yoy_tp,yoy_dedu_np,yoy_eps,yoy_roe,'
               'growth_assets,yoy_equity,growth_bps,or_last_year,op_last_year,tp_last_year,'
               'np_last_year,eps_last_year,open_net_assets,open_bps,perf_summary,is_audit,remark'
    )


def backfill_db3_limits(trade_dates: list):
    """按日期批量回补涨跌停价格"""
    total = len(trade_dates)
    print(f"\n[DB3 stk_limit 回补] 共 {total} 个交易日待处理")

    for i, td in enumerate(trade_dates):
        try:
            df = fetch_stk_limit_by_date(td)
            n = _save_to_db(df, 'stk_limit', 'db3')
            if (i + 1) % 10 == 0 or i == 0:
                print(f"  [{i+1}/{total}] {td}: {n} 条")
        except Exception as e:
            print(f"  ✗ [{i+1}/{total}] {td}: {e}")
            time.sleep(2)


def backfill_db3_forecast():
    """回补全部业绩预告数据"""
    print("\n[DB3 forecast] 回补业绩预告...")
    periods = []
    for year in range(2021, 2027):
        for q in ['0331', '0630', '0930', '1231']:
            periods.append(f'{year}{q}')

    for period in periods:
        try:
            df = fetch_forecast_vip(period)
            n = _save_to_db(df, 'forecast', 'db3')
            print(f"  {period}: {n} 条")
        except Exception as e:
            print(f"  ✗ {period}: {e}")
            time.sleep(1)


def backfill_db3_express():
    """回补全部业绩快报数据"""
    print("\n[DB3 express] 回补业绩快报...")
    periods = []
    for year in range(2021, 2027):
        for q in ['0331', '0630', '0930', '1231']:
            periods.append(f'{year}{q}')

    for period in periods:
        try:
            df = fetch_express_vip(period)
            n = _save_to_db(df, 'express', 'db3')
            print(f"  {period}: {n} 条")
        except Exception as e:
            print(f"  ✗ {period}: {e}")
            time.sleep(1)


# ============================================================
# DB1: 指数行情数据
# ============================================================

# 核心指数列表
CORE_INDEXES = [
    '000001.SH',  # 上证指数
    '399001.SZ',  # 深证成指
    '399006.SZ',  # 创业板指
    '000300.SH',  # 沪深300
    '000905.SH',  # 中证500
    '000852.SH',  # 中证1000
    '399303.SZ',  # 国证2000
]


def fetch_index_daily(ts_code: str, start_date: str = None, end_date: str = None):
    """按指数代码拉取日线行情"""
    start_date = start_date or BACKFILL_START_DATE
    end_date = end_date or BACKFILL_END_DATE
    return client.call(
        'index_daily',
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date,
    )


# ============================================================
# DB2: 融资融券数据
# ============================================================

def fetch_margin_by_date(trade_date: str):
    """按日期拉取融资融券交易汇总"""
    return client.call('margin', trade_date=trade_date)


def fetch_margin_detail_by_date(trade_date: str):
    """按日期拉取融资融券交易明细"""
    return client.call('margin_detail', trade_date=trade_date)


# ============================================================
# 指数每日指标
# ============================================================

INDEX_DAILYBASIC_CODES = [
    '000001.SH',  # 上证综指
    '399001.SZ',  # 深证成指
    '000016.SH',  # 上证50
    '000905.SH',  # 中证500
    '399005.SZ',  # 中小板指
    '399006.SZ',  # 创业板指
    '000300.SH',  # 沪深300
]


def fetch_index_dailybasic(ts_code: str, start_date: str = None, end_date: str = None):
    """按指数代码拉取大盘指数每日指标"""
    start_date = start_date or BACKFILL_START_DATE
    end_date = end_date or BACKFILL_END_DATE
    return client.call(
        'index_dailybasic',
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date,
    )


# ============================================================
# 龙虎榜数据
# ============================================================

def fetch_top_list_by_date(trade_date: str):
    """按日期拉取龙虎榜每日明细"""
    return client.call('top_list', trade_date=trade_date)


def fetch_top_inst_by_date(trade_date: str):
    """按日期拉取龙虎榜机构明细"""
    return client.call('top_inst', trade_date=trade_date)


# ============================================================
# 申万行业数据
# ============================================================

def fetch_sw_daily_by_date(trade_date: str):
    """按日期拉取全部申万行业日线行情"""
    return client.call(
        'sw_daily',
        trade_date=trade_date,
        fields='ts_code,trade_date,name,open,low,high,close,change,pct_change,vol,amount,pe,pb,float_mv,total_mv'
    )


def fetch_index_classify():
    """拉取申万行业分类（一次性）"""
    all_data = []
    for level in ['L1', 'L2', 'L3']:
        df = client.call('index_classify', level=level, src='SW2021')
        if df is not None and not df.empty:
            all_data.append(df)
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


def fetch_index_member_all_by_l1(l1_code: str):
    """按一级行业代码拉取申万行业成分"""
    return client.call('index_member_all', l1_code=l1_code, is_new='Y')


# ============================================================
# 沪深股通十大成交股
# ============================================================

def fetch_hsgt_top10_by_date(trade_date: str):
    """按日期拉取沪深股通十大成交股"""
    return client.call('hsgt_top10', trade_date=trade_date)


# ============================================================
# 股东与大宗交易数据
# ============================================================

def fetch_stk_holdertrade_by_date(ann_date: str):
    """按公告日期拉取股东增减持"""
    return client.call('stk_holdertrade', ann_date=ann_date)


def fetch_block_trade_by_date(trade_date: str):
    """按日期拉取大宗交易"""
    return client.call('block_trade', trade_date=trade_date)


# ============================================================
# 政策主题概念板块数据（用于十五五主题精准股票匹配）
# ============================================================

def fetch_concept_list():
    """拉取 Tushare 全量 A 股概念分类列表（concept_name 用于关键词匹配）。
    Tushare concept 接口实际返回字段名为 name，此处统一重命名为 concept_name。
    """
    df = client.call('concept', src='ts', fields='code,name')
    if df is not None and not df.empty:
        df = df.rename(columns={'name': 'concept_name'})
    return df


def fetch_concept_detail(concept_id: str) -> pd.DataFrame:
    """拉取指定概念 ID 的全部成分股"""
    df = client.call('concept_detail', id=concept_id, fields='ts_code,name')
    if df is not None and not df.empty:
        df['concept_id'] = concept_id
    return df if df is not None else pd.DataFrame()


def fetch_share_float_by_date(ann_date: str):
    """按公告日期拉取限售股解禁"""
    return client.call('share_float', ann_date=ann_date)


def fetch_stk_holdernumber_by_date(ann_date: str):
    """按公告日期拉取股东人数"""
    return client.call('stk_holdernumber', ann_date=ann_date)


# ============================================================
# 财务指标数据
# ============================================================

def fetch_fina_indicator_vip(period: str):
    """按报告期拉取全市场财务指标（需5000积分）"""
    return client.call(
        'fina_indicator_vip',
        period=period,
        fields='ts_code,ann_date,end_date,eps,dt_eps,total_revenue_ps,revenue_ps,'
               'capital_rese_ps,surplus_rese_ps,undist_profit_ps,extra_item,profit_dedt,'
               'gross_margin,current_ratio,quick_ratio,cash_ratio,'
               'ar_turn,ca_turn,fa_turn,assets_turn,op_income,ebit,ebitda,fcff,fcfe,'
               'netdebt,tangible_asset,working_capital,networking_capital,invest_capital,'
               'retained_earnings,bps,ocfps,retainedps,cfps,ebit_ps,fcff_ps,fcfe_ps,'
               'netprofit_margin,grossprofit_margin,roe,roe_waa,roe_dt,roa,npta,roic,'
               'debt_to_assets,assets_to_eqt,netprofit_yoy,dt_netprofit_yoy,'
               'or_yoy,q_sales_yoy,q_op_qoq,equity_yoy,rd_exp,update_flag'
    )


# ============================================================
# 指数成分和权重
# ============================================================

def fetch_index_weight(index_code: str, start_date: str, end_date: str):
    """按指数和日期范围拉取指数成分权重（月度数据）"""
    return client.call(
        'index_weight',
        index_code=index_code,
        start_date=start_date,
        end_date=end_date,
    )
