"""
Phase 3 并行回补脚本: 16个新增 Tushare 接口
- 不同接口任务并行运行（接口速率限制独立）
- 同一接口内并发拉取 + 批量写入
"""
import time
import threading
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import text
from code.core.db_manager import get_engine, create_all_tables
from code.data.fetchers import (
    client, _save_to_db, get_trade_dates,
    fetch_index_daily, CORE_INDEXES,
    fetch_index_dailybasic, INDEX_DAILYBASIC_CODES,
    fetch_margin_by_date, fetch_margin_detail_by_date,
    fetch_cyq_perf_by_stock,
    fetch_top_list_by_date, fetch_top_inst_by_date,
    fetch_sw_daily_by_date, fetch_index_classify, fetch_index_member_all_by_l1,
    fetch_hsgt_top10_by_date,
    fetch_stk_holdertrade_by_date, fetch_block_trade_by_date,
    fetch_share_float_by_date, fetch_stk_holdernumber_by_date,
    fetch_fina_indicator_vip,
    fetch_index_weight,
)
from code.core.config import (
    BACKFILL_START_DATE,
    BACKFILL_END_DATE,
    CYQ_PERF_WORKERS,
    CYQ_PERF_BATCH,
)

# 打印锁，避免多线程输出混乱
_print_lock = threading.Lock()


def safe_print(msg):
    with _print_lock:
        ts = datetime.now().strftime('%H:%M:%S')
        print(f"[{ts}] {msg}", flush=True)


def _format_elapsed(seconds):
    """格式化耗时为可读字符串"""
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m{seconds % 60}s"
    else:
        return f"{seconds // 3600}h{(seconds % 3600) // 60}m"


def get_existing_dates(db_key, table_name):
    engine = get_engine(db_key)
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT DISTINCT trade_date FROM `{table_name}`"))
            return {row[0] for row in result}
    except Exception:
        return set()
    finally:
        engine.dispose()


def get_existing_periods(db_key, table_name, col='end_date'):
    engine = get_engine(db_key)
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT DISTINCT `{col}` FROM `{table_name}`"))
            return {row[0] for row in result}
    except Exception:
        return set()
    finally:
        engine.dispose()


def generate_months(start_date=None, end_date=None):
    """生成月份范围列表 [(start, end), ...]"""
    start = datetime.strptime(start_date or BACKFILL_START_DATE, '%Y%m%d')
    end = datetime.strptime(end_date or BACKFILL_END_DATE, '%Y%m%d')
    months = []
    cur = start.replace(day=1)
    while cur <= end:
        next_month = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
        s = cur.strftime('%Y%m%d')
        e = (next_month - timedelta(days=1)).strftime('%Y%m%d')
        months.append((s, e))
        cur = next_month
    return months


def _dates_to_segments(sorted_dates, max_gap_days=5):
    """
    将有序日期列表合并为连续区间段 [(start, end), ...]
    相邻日期间隔不超过 max_gap_days 天视为连续，合并为一个区间。
    目的：减少 API 调用次数（一次拉整个区间而非逐天拉取）。
    """
    if not sorted_dates:
        return []
    segments = []
    seg_start = sorted_dates[0]
    seg_end = sorted_dates[0]
    for d in sorted_dates[1:]:
        dt_prev = datetime.strptime(seg_end, '%Y%m%d')
        dt_cur = datetime.strptime(d, '%Y%m%d')
        if (dt_cur - dt_prev).days <= max_gap_days:
            seg_end = d
        else:
            segments.append((seg_start, seg_end))
            seg_start = d
            seg_end = d
    segments.append((seg_start, seg_end))
    return segments


def _get_month_record_counts(db_key, table_name, date_col='trade_date'):
    """
    查询表中每个月的记录数 {YYYYMM: count}
    用于判断月粒度数据是否完整（只有0条记录的月才需要补）
    """
    engine = get_engine(db_key)
    try:
        with engine.connect() as conn:
            result = conn.execute(text(
                f"SELECT SUBSTRING(`{date_col}`, 1, 6) AS ym, COUNT(*) AS cnt "
                f"FROM `{table_name}` WHERE `{date_col}` IS NOT NULL "
                f"GROUP BY ym"
            ))
            return {row[0]: row[1] for row in result if row[0]}
    except Exception:
        return {}
    finally:
        engine.dispose()


# ============================================================
# 核心: 并发拉取 + 批量写入
# ============================================================

def parallel_date_task(task_name, fetch_func, table_name, db_key,
                       dates, workers=3, batch_size=10, use_replace=False):
    """
    并发拉取按日期的数据，批量写入 DB
    - workers: 并发线程数（降低到3以减少 API 竟态）
    - batch_size: 每批合并写入的天数（降低到10减少单批失败影响）
    """
    total = len(dates)
    if total == 0:
        safe_print(f"  [{task_name}] ✓ 已全部回补，跳过")
        return

    safe_print(f"  [{task_name}] 待回补: {total}, 并发×{workers}, 批量×{batch_size}")

    success = 0
    errors = 0
    task_start = time.time()
    last_log_time = task_start
    # 根据任务大小动态调整日志频率：小任务每20条，大任务每50条
    log_interval = 20 if total <= 100 else 50

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for chunk_start in range(0, total, batch_size):
            chunk_dates = dates[chunk_start:chunk_start + batch_size]
            futures = [(td, pool.submit(fetch_func, td)) for td in chunk_dates]

            chunk_dfs = []
            for td, future in futures:
                try:
                    df = future.result(timeout=120)
                    if df is not None and not df.empty:
                        chunk_dfs.append(df)
                    success += 1
                except Exception as e:
                    errors += 1
                    if errors <= 5:
                        safe_print(f"  [{task_name}] fetch {td} 失败: {str(e)[:80]}")
                    elif errors == 6:
                        safe_print(f"  [{task_name}] 后续错误将不再逐条打印...")

            if chunk_dfs:
                try:
                    # 确保所有 DataFrame 的列一致再 concat
                    # 取第一个 df 的列作为基准
                    base_cols = list(chunk_dfs[0].columns)
                    aligned_dfs = []
                    for df in chunk_dfs:
                        # 只保留基准列中存在的列，缺失列补 NaN
                        for col in base_cols:
                            if col not in df.columns:
                                df[col] = None
                        aligned_dfs.append(df[base_cols])
                    combined = pd.concat(aligned_dfs, ignore_index=True)
                    _save_to_db(combined, table_name, db_key, use_replace=use_replace)
                except Exception as e:
                    safe_print(f"  [{task_name}] DB写入异常: {str(e)[:120]}")
                    # 降级: 逐个 DataFrame 写入
                    for df in chunk_dfs:
                        try:
                            _save_to_db(df, table_name, db_key, use_replace=use_replace)
                        except Exception:
                            pass

            done = min(chunk_start + batch_size, total)
            now = time.time()
            elapsed = now - task_start
            # 更高频的进度输出：每 log_interval 条 或 每30秒 或 首批/末批
            should_log = (
                done == total or
                chunk_start == 0 or
                done % log_interval == 0 or
                (now - last_log_time) >= 30  # 至少每30秒输出一次
            )
            if should_log:
                elapsed_str = _format_elapsed(elapsed)
                speed = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / speed if speed > 0 else 0
                safe_print(
                    f"  [{task_name}] {done}/{total} (✓{success} ✗{errors})"
                    f"  耗时{elapsed_str}, 约{_format_elapsed(eta)}后完成"
                )
                last_log_time = now

    elapsed_total = _format_elapsed(time.time() - task_start)
    safe_print(f"  [{task_name}] ✓ 完成: {success}/{total}, 总耗时{elapsed_total}")


# ============================================================
# 快速任务 (Batch 1, 串行即可)
# ============================================================

def _get_existing_dates_for_code(db_key, table_name, ts_code):
    """查询某张表中某个 ts_code 已有的 trade_date 集合"""
    engine = get_engine(db_key)
    try:
        with engine.connect() as conn:
            result = conn.execute(text(
                f"SELECT DISTINCT trade_date FROM `{table_name}` WHERE ts_code = '{ts_code}'"
            ))
            return {row[0] for row in result}
    except Exception:
        return set()
    finally:
        engine.dispose()


def task_index_daily():
    safe_print("\n[指数日线] 回补核心指数行情...")
    all_dates = set(get_trade_dates())
    for code in CORE_INDEXES:
        try:
            existing = _get_existing_dates_for_code('db1', 'index_daily', code)
            missing = sorted(all_dates - existing)
            if not missing:
                safe_print(f"  ✓ {code} 已完整，跳过")
                continue
            # 按连续区间分段拉取，减少 API 调用次数
            segments = _dates_to_segments(missing)
            total_fetched = 0
            for seg_start, seg_end in segments:
                df = fetch_index_daily(code, start_date=seg_start, end_date=seg_end)
                _save_to_db(df, 'index_daily', 'db1', use_replace=True)
                total_fetched += len(df) if df is not None else 0
            safe_print(f"  ✓ {code} (缺{len(missing)}天, 补{total_fetched}条)")
        except Exception as e:
            safe_print(f"  ✗ {code}: {e}")
    safe_print("[指数日线] ✓ 完成")


def task_index_dailybasic():
    safe_print("\n[指数指标] 回补...")
    all_dates = set(get_trade_dates())
    for code in INDEX_DAILYBASIC_CODES:
        try:
            existing = _get_existing_dates_for_code('db1', 'index_dailybasic', code)
            missing = sorted(all_dates - existing)
            if not missing:
                safe_print(f"  ✓ {code} 已完整，跳过")
                continue
            segments = _dates_to_segments(missing)
            total_fetched = 0
            for seg_start, seg_end in segments:
                df = fetch_index_dailybasic(code, start_date=seg_start, end_date=seg_end)
                _save_to_db(df, 'index_dailybasic', 'db1', use_replace=True)
                total_fetched += len(df) if df is not None else 0
            safe_print(f"  ✓ {code} (缺{len(missing)}天, 补{total_fetched}条)")
        except Exception as e:
            safe_print(f"  ✗ {code}: {e}")
    safe_print("[指数指标] ✓ 完成")


def task_index_classify():
    safe_print("\n[申万分类] 拉取行业分类...")
    try:
        df = fetch_index_classify()
        n = _save_to_db(df, 'index_classify', 'db3', if_exists='replace')
        safe_print(f"  ✓ {n} 条")
    except Exception as e:
        safe_print(f"  ✗ {e}")
    safe_print("[申万分类] ✓ 完成")


def task_index_member_all():
    safe_print("\n[申万成分] 拉取行业成分...")
    engine = get_engine('db3')
    try:
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT index_code FROM index_classify WHERE level = 'L1'"
            ))
            l1_codes = [row[0] for row in result]
    except Exception:
        l1_codes = [
            '801010.SI', '801030.SI', '801040.SI', '801050.SI', '801080.SI',
            '801110.SI', '801120.SI', '801130.SI', '801140.SI', '801150.SI',
            '801160.SI', '801170.SI', '801180.SI', '801200.SI', '801210.SI',
            '801230.SI', '801710.SI', '801720.SI', '801730.SI', '801740.SI',
            '801750.SI', '801760.SI', '801770.SI', '801780.SI', '801790.SI',
            '801880.SI', '801890.SI', '801950.SI', '801960.SI', '801970.SI',
            '801980.SI',
        ]
    finally:
        engine.dispose()

    total = len(l1_codes)
    for i, code in enumerate(l1_codes):
        try:
            df = fetch_index_member_all_by_l1(code)
            _save_to_db(df, 'index_member_all', 'db3')
            if (i + 1) % 10 == 0 or i == 0 or i == total - 1:
                safe_print(f"  [{i+1}/{total}] {code}")
        except Exception as e:
            safe_print(f"  ✗ {code}: {e}")
    safe_print(f"[申万成分] ✓ 完成")


# ============================================================
# 按日期拉取的任务（使用 parallel_date_task 并发）
# ============================================================

def task_margin():
    all_dates = get_trade_dates()
    existing = get_existing_dates('db2', 'margin')
    remaining = [d for d in all_dates if d not in existing]
    parallel_date_task('融资融券', fetch_margin_by_date, 'margin', 'db2', remaining)


def task_margin_detail():
    all_dates = get_trade_dates()
    existing = get_existing_dates('db2', 'margin_detail')
    remaining = [d for d in all_dates if d not in existing]
    parallel_date_task('融资融券明细', fetch_margin_detail_by_date, 'margin_detail', 'db2',
                       remaining, workers=3, batch_size=10)


def task_hsgt_top10():
    all_dates = get_trade_dates()
    existing = get_existing_dates('db2', 'hsgt_top10')
    remaining = [d for d in all_dates if d not in existing]
    parallel_date_task('沪深股通Top10', fetch_hsgt_top10_by_date, 'hsgt_top10', 'db2', remaining)


def task_top_list():
    all_dates = get_trade_dates()
    existing = get_existing_dates('db3', 'top_list')
    remaining = [d for d in all_dates if d not in existing]
    parallel_date_task('龙虎榜', fetch_top_list_by_date, 'top_list', 'db3',
                       remaining, use_replace=True)


def task_top_inst():
    all_dates = get_trade_dates()
    existing = get_existing_dates('db3', 'top_inst')
    remaining = [d for d in all_dates if d not in existing]
    parallel_date_task('龙虎榜机构', fetch_top_inst_by_date, 'top_inst', 'db3', remaining)


def task_block_trade():
    all_dates = get_trade_dates()
    existing = get_existing_dates('db3', 'block_trade')
    remaining = [d for d in all_dates if d not in existing]
    parallel_date_task('大宗交易', fetch_block_trade_by_date, 'block_trade', 'db3', remaining)


def task_sw_daily():
    all_dates = get_trade_dates()
    existing = get_existing_dates('db3', 'sw_daily')
    remaining = [d for d in all_dates if d not in existing]
    parallel_date_task('申万行业', fetch_sw_daily_by_date, 'sw_daily', 'db3',
                       remaining, workers=3, batch_size=10)


# ============================================================
# 按月份/季度拉取的任务（数量少，串行即可）
# ============================================================


def task_stk_holdertrade():
    safe_print(f"  [股东增减持] 开始...")
    t0 = time.time()
    months = generate_months()
    month_counts = _get_month_record_counts('db3', 'stk_holdertrade', 'ann_date')
    # 只补记录数为 0 的月份
    remaining = [(s, e) for s, e in months if month_counts.get(s[:6], 0) == 0]
    safe_print(f"  [股东增减持] 全部{len(months)}月, 已有{len(months)-len(remaining)}月, 待回补{len(remaining)}月")
    total = len(remaining)
    for i, (s, e) in enumerate(remaining):
        try:
            df = client.call('stk_holdertrade', start_date=s, end_date=e)
            _save_to_db(df, 'stk_holdertrade', 'db3')
            if (i + 1) % 10 == 0 or i == 0 or i == total - 1:
                safe_print(f"  [股东增减持] [{i+1}/{total}] {s}~{e}")
        except Exception as e_err:
            safe_print(f"  [股东增减持] ✗ {s}~{e}: {e_err}")
    safe_print(f"  [股东增减持] ✓ 完成 (耗时{_format_elapsed(time.time() - t0)})")


def task_share_float():
    safe_print(f"  [限售解禁] 开始...")
    t0 = time.time()
    months = generate_months()
    month_counts = _get_month_record_counts('db3', 'share_float', 'float_date')
    remaining = [(s, e) for s, e in months if month_counts.get(s[:6], 0) == 0]
    safe_print(f"  [限售解禁] 全部{len(months)}月, 已有{len(months)-len(remaining)}月, 待回补{len(remaining)}月")
    total = len(remaining)
    for i, (s, e) in enumerate(remaining):
        try:
            df = client.call('share_float', start_date=s, end_date=e)
            _save_to_db(df, 'share_float', 'db3')
            if (i + 1) % 10 == 0 or i == 0 or i == total - 1:
                safe_print(f"  [限售解禁] [{i+1}/{total}] {s}~{e}")
        except Exception as e_err:
            safe_print(f"  [限售解禁] ✗ {s}~{e}: {e_err}")
    safe_print(f"  [限售解禁] ✓ 完成 (耗时{_format_elapsed(time.time() - t0)})")


def task_stk_holdernumber():
    safe_print(f"  [股东人数] 开始...")
    t0 = time.time()
    months = generate_months()
    month_counts = _get_month_record_counts('db3', 'stk_holdernumber', 'end_date')
    remaining = [(s, e) for s, e in months if month_counts.get(s[:6], 0) == 0]
    safe_print(f"  [股东人数] 全部{len(months)}月, 已有{len(months)-len(remaining)}月, 待回补{len(remaining)}月")
    total = len(remaining)
    for i, (s, e) in enumerate(remaining):
        try:
            df = client.call('stk_holdernumber', start_date=s, end_date=e)
            _save_to_db(df, 'stk_holdernumber', 'db3')
            if (i + 1) % 10 == 0 or i == 0 or i == total - 1:
                safe_print(f"  [股东人数] [{i+1}/{total}] {s}~{e}")
        except Exception as e_err:
            safe_print(f"  [股东人数] ✗ {s}~{e}: {e_err}")
    safe_print(f"  [股东人数] ✓ 完成 (耗时{_format_elapsed(time.time() - t0)})")


def task_fina_indicator():
    safe_print(f"  [财务指标] 开始...")
    t0 = time.time()
    periods = []
    for year in range(2021, 2027):
        for q in ['0331', '0630', '0930', '1231']:
            periods.append(f'{year}{q}')

    existing = get_existing_periods('db3', 'fina_indicator', 'end_date')
    remaining = [p for p in periods if p not in existing]
    safe_print(f"  [财务指标] 共 {len(periods)} 季, 待回补: {len(remaining)}")

    for period in remaining:
        try:
            df = fetch_fina_indicator_vip(period)
            n = _save_to_db(df, 'fina_indicator', 'db3', use_replace=True)
            safe_print(f"  [财务指标] {period}: {n} 条")
        except Exception as e:
            safe_print(f"  [财务指标] ✗ {period}: {e}")
    safe_print(f"  [财务指标] ✓ 完成 (耗时{_format_elapsed(time.time() - t0)})")


def task_index_weight():
    safe_print(f"  [指数权重] 开始...")
    t0 = time.time()
    weight_indexes = ['000300.SH', '000905.SH', '000852.SH']

    # 指数权重是月度数据，用月份差集检查
    for idx_code in weight_indexes:
        existing = _get_existing_dates_for_code('db2', 'index_weight', idx_code)
        # 生成所有月份区间
        all_months = generate_months()
        # 找到尚未覆盖的月份（整月无任何数据的月份）
        existing_months = {d[:6] for d in existing}
        remaining_months = [(s, e) for s, e in all_months if s[:6] not in existing_months]
        if not remaining_months:
            safe_print(f"  [指数权重] ✓ {idx_code} 已完整，跳过")
            continue
        for s, e in remaining_months:
            try:
                df = fetch_index_weight(idx_code, s, e)
                _save_to_db(df, 'index_weight', 'db2', use_replace=True)
            except Exception as err:
                safe_print(f"  [指数权重] ✗ {idx_code} {s}~{e}: {err}")
        safe_print(f"  [指数权重] ✓ {idx_code} (补{len(remaining_months)}月)")
    safe_print(f"  [指数权重] ✓ 完成 (耗时{_format_elapsed(time.time() - t0)})")


# ============================================================
# 筹码胜率（最慢，并发按股票拉取）
# ============================================================

def task_cyq_perf():
    safe_print("\n[筹码胜率] 回补...")
    engine = get_engine('db1')
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT ts_code FROM stock_basic WHERE list_status = 'L' ORDER BY ts_code"
        ))
        stock_list = [row[0] for row in result]
    engine.dispose()

    engine = get_engine('db2')
    with engine.connect() as conn:
        result = conn.execute(text("SELECT DISTINCT ts_code FROM cyq_perf"))
        existing = {row[0] for row in result}
    engine.dispose()

    remaining = [s for s in stock_list if s not in existing]
    total = len(remaining)
    safe_print(f"  总: {len(stock_list)}, 已完成: {len(existing)}, 待回补: {total}")
    safe_print(f"  并发×{CYQ_PERF_WORKERS}, 批量×{CYQ_PERF_BATCH}")

    if not remaining:
        safe_print("  ✓ 已全部回补，跳过")
        return

    success = 0
    errors = 0
    task_start = time.time()
    last_log_time = task_start

    def fetch_stock(code):
        try:
            return fetch_cyq_perf_by_stock(code)
        except Exception:
            return None

    workers = CYQ_PERF_WORKERS
    batch_size = CYQ_PERF_BATCH

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for chunk_start in range(0, total, batch_size):
            chunk = remaining[chunk_start:chunk_start + batch_size]
            futures = [(code, pool.submit(fetch_stock, code)) for code in chunk]

            chunk_dfs = []
            for code, future in futures:
                try:
                    df = future.result(timeout=120)
                    if df is not None and not df.empty:
                        chunk_dfs.append(df)
                        success += 1
                    else:
                        errors += 1
                except Exception:
                    errors += 1

            if chunk_dfs:
                try:
                    combined = pd.concat(chunk_dfs, ignore_index=True)
                    _save_to_db(combined, 'cyq_perf', 'db2')
                except Exception as e:
                    safe_print(f"  [筹码胜率] DB写入异常: {str(e)[:80]}")
                    for df in chunk_dfs:
                        try:
                            _save_to_db(df, 'cyq_perf', 'db2')
                        except Exception:
                            pass

            done = min(chunk_start + batch_size, total)
            now = time.time()
            elapsed = now - task_start
            # 每50只或每30秒打印一次进度
            log_interval = 50
            should_log = (
                done == total or
                chunk_start == 0 or
                done % log_interval == 0 or
                (now - last_log_time) >= 30
            )
            if should_log:
                elapsed_str = _format_elapsed(elapsed)
                speed = done / elapsed if elapsed > 0 else 0
                eta = (total - done) / speed if speed > 0 else 0
                safe_print(
                    f"  [筹码胜率] {done}/{total} (✓{success} ✗{errors})"
                    f"  耗时{elapsed_str}, 约{_format_elapsed(eta)}后完成"
                )
                last_log_time = now

    elapsed_total = _format_elapsed(time.time() - task_start)
    safe_print(f"  [筹码胜率] ✓ 完成: {success}/{total}, 总耗时{elapsed_total}")


# ============================================================
# 主流程
# ============================================================

if __name__ == '__main__':
    safe_print("=" * 60)
    safe_print("Phase 3 并行数据补充（16个接口）")
    safe_print(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    safe_print("=" * 60)

    _global_start = time.time()

    create_all_tables()

    # ---- 第一阶段: 快速一次性任务（串行） ----
    task_index_daily()
    task_index_dailybasic()
    task_index_classify()
    task_index_member_all()

    # ---- 第二阶段: 所有剩余任务并行 ----
    safe_print("\n" + "=" * 60)
    safe_print("开始并行回补所有接口...")
    safe_print("=" * 60)

    parallel_tasks = [
        task_margin,
        task_margin_detail,
        task_hsgt_top10,
        task_top_list,
        task_top_inst,
        task_block_trade,
        task_sw_daily,
        task_stk_holdertrade,
        task_share_float,
        task_stk_holdernumber,
        task_fina_indicator,
        task_index_weight,
    ]

    # 限制最多 4 个任务并行（避免 API 速率限制）
    # 心跳线程：定期报告仍在运行的任务
    _active_tasks = {}  # {future: task_name}
    _parallel_done = threading.Event()

    def _heartbeat():
        """每60秒打印一次仍在运行的任务，让用户知道程序没有卡死"""
        while not _parallel_done.wait(timeout=60):
            running = [name for f, name in _active_tasks.items() if not f.done()]
            if running:
                safe_print(f"  ♥ 心跳: 仍在运行的任务 ({len(running)}个): {', '.join(running)}")

    heartbeat_thread = threading.Thread(target=_heartbeat, daemon=True)
    heartbeat_thread.start()

    with ThreadPoolExecutor(max_workers=4) as task_pool:
        for fn in parallel_tasks:
            f = task_pool.submit(fn)
            _active_tasks[f] = fn.__name__

        for future in as_completed(_active_tasks):
            name = _active_tasks[future]
            try:
                future.result()
            except Exception as e:
                safe_print(f"  任务 {name} 异常: {e}")

    _parallel_done.set()

    # ---- 第三阶段: 筹码胜率（最慢，单独跑） ----
    task_cyq_perf()

    total_elapsed = _format_elapsed(time.time() - _global_start)
    safe_print("\n" + "=" * 60)
    safe_print(f"Phase 3 全部完成! 总耗时: {total_elapsed}")
    safe_print("=" * 60)
