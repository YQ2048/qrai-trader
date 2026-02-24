"""
QRAI-Trader 近期数据补齐脚本 (sync_daily.py)
基于差集法检测 DB 中每张表的真实缺失交易日，精确补齐。

与"最新日期"法不同，差集法能发现历史中间任意位置的缺失数据。

使用场景：
    1. 每日策略执行前先调用，确保 DB 数据完整
    2. 中断恢复后补齐缺失数据（含中间漏洞）
    3. 手动触发补齐指定日期范围

用法：
    # 自动检测并补齐（默认扫描最近180天）
    python -m code.data.sync_daily

    # 指定补齐到某日
    python -m code.data.sync_daily --end-date 20260210

    # 全量扫描（从 BACKFILL_START_DATE 开始）
    python -m code.data.sync_daily --full-scan

    # 自定义回溯天数
    python -m code.data.sync_daily --lookback-days 365

    # 只检查不执行（dry run）
    python -m code.data.sync_daily --dry-run
"""
import time
import argparse
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import text
from code.core.db_manager import get_engine
from code.core.logger import get_logger
from code.data.fetchers import (
    client, _save_to_db,
    fetch_stock_basic, fetch_trade_cal,
    fetch_daily_by_date, fetch_adj_factor_by_date, fetch_daily_basic_by_date,
    fetch_moneyflow_by_date,
    fetch_stk_limit_by_date, fetch_limit_list_d_by_date,
    fetch_moneyflow_hsgt,
    fetch_index_daily, CORE_INDEXES,
    fetch_index_dailybasic, INDEX_DAILYBASIC_CODES,
    fetch_margin_by_date, fetch_margin_detail_by_date,
    fetch_top_list_by_date, fetch_top_inst_by_date,
    fetch_sw_daily_by_date,
    fetch_hsgt_top10_by_date,
    fetch_block_trade_by_date,
    fetch_cyq_perf_by_stock,
)
from code.core.config import API_CALL_INTERVAL, BACKFILL_START_DATE, CYQ_PERF_WORKERS

logger = get_logger(__name__)


# 统一的检查清单（避免"检查了但不补齐"）
CHECK_TABLES = [
    ('db1', 'stock_daily',      '日线行情'),
    ('db1', 'adj_factor',       '复权因子'),
    ('db1', 'daily_basic',      '每日指标'),
    ('db1', 'index_daily',      '指数日线'),
    ('db1', 'index_dailybasic', '指数指标'),
    ('db2', 'moneyflow',        '资金流向'),
    ('db2', 'moneyflow_hsgt',   '北向资金'),
    ('db2', 'margin',           '融资融券'),
    ('db2', 'margin_detail',    '融券明细'),
    ('db2', 'hsgt_top10',       '股通Top10'),
    ('db2', 'cyq_perf',         '筹码胜率'),
    ('db3', 'stk_limit',        '涨跌停价'),
    ('db3', 'limit_list_d',     '涨跌停炸板'),
    ('db3', 'top_list',         '龙虎榜明细'),
    ('db3', 'top_inst',         '龙虎榜机构'),
    ('db3', 'sw_daily',         '申万行业'),
    ('db3', 'block_trade',      '大宗交易'),
]


# ============================================================
# 辅助函数
# ============================================================

_ALLOWED_TABLES = {row[1] for row in CHECK_TABLES}


def _validate_table_name(table_name: str) -> str:
    """白名单校验表名，防止 SQL 注入"""
    if table_name not in _ALLOWED_TABLES:
        raise ValueError(f"非法表名: {table_name}")
    return table_name


def get_latest_date_in_db(db_key: str, table_name: str) -> str:
    """查询某张表中最新的 trade_date"""
    _validate_table_name(table_name)
    engine = get_engine(db_key)
    try:
        with engine.connect() as conn:
            result = conn.execute(text(
                f"SELECT MAX(trade_date) FROM `{table_name}`"
            ))
            row = result.fetchone()
            return row[0] if row and row[0] else None
    except Exception as e:
        logger.warning("获取最新交易日失败: %s", e)
        return None


def get_latest_trade_date() -> str:
    """从交易日历获取截至今天的最后一个交易日"""
    today = datetime.now().strftime('%Y%m%d')
    engine = get_engine('db1')
    try:
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT MAX(cal_date) FROM trade_cal "
                "WHERE is_open = 1 AND cal_date <= :today"
            ), {"today": today})
            row = result.fetchone()
            return row[0] if row and row[0] else today
    except Exception as e:
        logger.warning("获取上一交易日失败: %s", e)
        return today


def get_trade_dates_between(start_date: str, end_date: str) -> list:
    """获取两个日期间的交易日列表"""
    engine = get_engine('db1')
    try:
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT cal_date FROM trade_cal WHERE is_open=1 "
                "AND cal_date >= :start_date AND cal_date <= :end_date "
                "ORDER BY cal_date"
            ), {"start_date": start_date, "end_date": end_date})
            return [row[0] for row in result]
    except Exception as e:
        logger.warning("获取交易日列表失败: %s", e)
        return []


def get_missing_dates(db_key: str, table_name: str, all_dates: list) -> list:
    """找出某张表中缺失的交易日"""
    _validate_table_name(table_name)
    engine = get_engine(db_key)
    try:
        with engine.connect() as conn:
            result = conn.execute(text(
                f"SELECT DISTINCT trade_date FROM `{table_name}`"
            ))
            existing = {row[0] for row in result}
        return [d for d in all_dates if d not in existing]
    except Exception as e:
        logger.warning("检测缺失日期失败，返回全部: %s", e)
        return all_dates


def _fetch_with_retry(fetch_func, *args, retries: int = 2, retry_wait: float = 0.8, **kwargs):
    """带重试的拉取包装，处理网络抖动/接口短暂异常"""
    last_error = None
    for i in range(retries + 1):
        try:
            return fetch_func(*args, **kwargs)
        except Exception as e:
            last_error = e
            if i < retries:
                time.sleep(retry_wait * (i + 1))
    raise last_error


def _sync_table_by_dates(task_name: str, fetch_func, table_name: str, db_key: str,
                         missing_dates: list, use_replace: bool = False,
                         workers: int = 4, retries: int = 2):
    """按日期并发补齐单张表"""
    if not missing_dates:
        print(f"  [{task_name}] ✓ 完整")
        return 0, 0

    print(f"  [{task_name}] 缺失 {len(missing_dates)} 天，补齐中... (并发×{workers})")
    success = 0
    errors = 0
    empty = 0

    max_workers = max(1, min(workers, len(missing_dates)))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {
            pool.submit(_fetch_with_retry, fetch_func, td, retries=retries): td
            for td in missing_dates
        }

        done = 0
        for future in as_completed(future_map):
            td = future_map[future]
            done += 1
            try:
                df = future.result()
                inserted = _save_to_db(df, table_name, db_key, use_replace=use_replace)
                if inserted > 0:
                    success += 1
                else:
                    empty += 1
                    if empty <= 5:
                        print(f"    ⚠ [{task_name}] {td}: 接口返回空数据")
            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"    ✗ [{task_name}] {td}: {str(e)[:80]}")

            if done % 20 == 0 or done == len(missing_dates):
                print(f"    [{task_name}] {done}/{len(missing_dates)}")

    if empty > 0:
        print(f"  [{task_name}] ⚠ 成功 {success}/{len(missing_dates)}，空返回 {empty} 天")
    else:
        print(f"  [{task_name}] ✓ {success}/{len(missing_dates)}")
    return success, errors + empty


# ============================================================
# 数据完整性检查
# ============================================================

def check_data_freshness():
    """检查所有核心表的数据新鲜度"""
    latest_trade_date = get_latest_trade_date()

    print(f"\n{'='*60}")
    print(f"数据新鲜度检查 (当前最新交易日: {latest_trade_date})")
    print(f"{'='*60}")
    print(f"{'表名':<18} {'DB最新日期':<12} {'差距':>6}  状态")
    print(f"{'-'*55}")

    stale_tables = []
    for db_key, table, desc in CHECK_TABLES:
        latest = get_latest_date_in_db(db_key, table)
        if latest:
            gap = len(get_trade_dates_between(latest, latest_trade_date)) - 1
            status = '✓' if gap == 0 else f'✗ 缺 {gap} 天'
            print(f"  {desc:<16} {latest:<12} {gap:>4}天  {status}")
            if gap > 0:
                stale_tables.append((db_key, table, desc, latest, gap))
        else:
            print(f"  {desc:<16} {'无数据':<12}   {'—':>4}  ✗ 空表")
            stale_tables.append((db_key, table, desc, None, -1))

    return latest_trade_date, stale_tables


# ============================================================
# 增量补齐
# ============================================================

def sync_core_daily(all_trade_dates: list):
    """
    按表独立检测缺失交易日，精确补齐核心每日数据。
    每张表单独做差集 —— 即使 stock_daily 完整，adj_factor 有洞也会被发现并修复。
    """
    core_defs = [
        ('db1', 'stock_daily',  '日线行情', fetch_daily_by_date),
        ('db1', 'adj_factor',   '复权因子', fetch_adj_factor_by_date),
        ('db1', 'daily_basic',  '每日指标', fetch_daily_basic_by_date),
        ('db2', 'moneyflow',    '资金流向', fetch_moneyflow_by_date),
        ('db3', 'stk_limit',    '涨跌停价', fetch_stk_limit_by_date),
    ]

    total_fixed = 0
    total_errors = 0
    for db_key, table, desc, fetch_func in core_defs:
        missing = get_missing_dates(db_key, table, all_trade_dates)
        success, errors = _sync_table_by_dates(
            task_name=desc,
            fetch_func=fetch_func,
            table_name=table,
            db_key=db_key,
            missing_dates=missing,
            workers=4,
            retries=2,
        )
        total_fixed += success
        total_errors += errors

    return total_fixed, total_errors


def sync_supplementary(all_trade_dates: list, target_date: str):
    """
    补齐辅助数据。每张表独立做差集检测缺失日期。
    涵盖: 涨跌停炸板 / 龙虎榜 / 申万行业 / 融资融券 / 沪深股通 / 大宗交易 / 指数
    """
    if not all_trade_dates:
        return

    print(f"\n[辅助数据] 扫描 {len(all_trade_dates)} 个交易日的辅助数据缺失...")

    # 对指数行情做差集: 以 (trade_date) 为粒度
    idx_missing = get_missing_dates('db1', 'index_daily', all_trade_dates)
    if idx_missing:
        print(f"  [指数日线] 缺失 {len(idx_missing)} 天，补齐中...")
        for code in CORE_INDEXES:
            try:
                df = _fetch_with_retry(
                    fetch_index_daily,
                    code,
                    start_date=idx_missing[0],
                    end_date=idx_missing[-1],
                    retries=2,
                )
                _save_to_db(df, 'index_daily', 'db1')
            except Exception as e:
                print(f"    ✗ index_daily {code}: {e}")
        print(f"  [指数日线] ✓ 已补齐")
    else:
        print(f"  [指数日线] ✓ 完整")

    idx_basic_missing = get_missing_dates('db1', 'index_dailybasic', all_trade_dates)
    if idx_basic_missing:
        print(f"  [指数指标] 缺失 {len(idx_basic_missing)} 天，补齐中...")
        for code in INDEX_DAILYBASIC_CODES:
            try:
                df = _fetch_with_retry(
                    fetch_index_dailybasic,
                    code,
                    start_date=idx_basic_missing[0],
                    end_date=idx_basic_missing[-1],
                    retries=2,
                )
                _save_to_db(df, 'index_dailybasic', 'db1')
            except Exception as e:
                print(f"    ✗ index_dailybasic {code}: {e}")
        print(f"  [指数指标] ✓ 已补齐")
    else:
        print(f"  [指数指标] ✓ 完整")

    # --- 北向资金（按区间拉取）---
    hsgt_missing = get_missing_dates('db2', 'moneyflow_hsgt', all_trade_dates)
    if hsgt_missing:
        print(f"  [北向资金] 缺失 {len(hsgt_missing)} 天，补齐中...")
        try:
            _fetch_with_retry(
                fetch_moneyflow_hsgt,
                start_date=hsgt_missing[0],
                end_date=hsgt_missing[-1],
                retries=2,
            )
        except Exception as e:
            print(f"    ✗ moneyflow_hsgt: {e}")
        remain = get_missing_dates('db2', 'moneyflow_hsgt', all_trade_dates)
        if remain:
            print(f"  [北向资金] ⚠ 仍缺 {len(remain)} 天 ({remain[0]} ~ {remain[-1]})")
        else:
            print(f"  [北向资金] ✓ 已补齐")
    else:
        print(f"  [北向资金] ✓ 完整")

    # --- 筹码胜率（cyq_perf，按股票+区间拉取）---
    cyq_missing = get_missing_dates('db2', 'cyq_perf', all_trade_dates)
    if cyq_missing:
        _beijing_now = datetime.now(timezone(timedelta(hours=8)))
        _today_str = _beijing_now.strftime('%Y%m%d')
        # 如果今天在缺失列表中，先试探一只活跃股确认数据是否已发布
        _has_today = any(str(d).replace('-', '')[:8] == _today_str for d in cyq_missing)
        if _has_today:
            _probe_ok = False
            try:
                _probe_df = fetch_cyq_perf_by_stock(
                    '000001.SZ', start_date=_today_str, end_date=_today_str
                )
                _probe_ok = _probe_df is not None and not _probe_df.empty
            except Exception:
                pass
            if not _probe_ok and _beijing_now.hour < 20:
                cyq_missing = [d for d in cyq_missing if str(d).replace('-', '')[:8] != _today_str]
                print(
                    f"  [筹码胜率] ⏭ 跳过 {_today_str}"
                    f"（000001.SZ 试探无数据 + 北京时间 {_beijing_now.strftime('%H:%M')} < 20:00）"
                )
    if cyq_missing:
        print(f"  [筹码胜率] 缺失 {len(cyq_missing)} 天，补齐中...")
        start_date = cyq_missing[0]
        end_date = cyq_missing[-1]

        engine = get_engine('db1')
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT ts_code FROM stock_basic WHERE list_status = 'L' ORDER BY ts_code"
            ))
            stock_list = [row[0] for row in result]

        success = 0
        errors = 0
        total = len(stock_list)

        def _fetch_cyq(ts_code: str):
            return _fetch_with_retry(
                fetch_cyq_perf_by_stock,
                ts_code,
                start_date=start_date,
                end_date=end_date,
                retries=2,
                retry_wait=max(API_CALL_INTERVAL, 0.2),
            )

        workers = CYQ_PERF_WORKERS
        with ThreadPoolExecutor(max_workers=min(workers, total)) as pool:
            future_map = {pool.submit(_fetch_cyq, ts_code): ts_code for ts_code in stock_list}
            done = 0
            for future in as_completed(future_map):
                ts_code = future_map[future]
                done += 1
                try:
                    df = future.result()
                    _save_to_db(df, 'cyq_perf', 'db2')
                    success += 1
                except Exception as e:
                    errors += 1
                    if errors <= 5:
                        print(f"    ✗ [筹码胜率] {ts_code}: {str(e)[:80]}")

                if done % 200 == 0 or done == total:
                    print(f"    [筹码胜率] {done}/{total}")

        print(f"  [筹码胜率] ✓ 已补齐（股票维度） {success}/{total}")
    else:
        print(f"  [筹码胜率] ✓ 完整")

    # --- 按日期循环拉取的辅助数据（每张表独立差集）---
    date_tasks = [
        ('涨跌停炸板', fetch_limit_list_d_by_date, 'limit_list_d', 'db3'),
        ('龙虎榜明细', fetch_top_list_by_date,     'top_list',     'db3'),
        ('龙虎榜机构', fetch_top_inst_by_date,     'top_inst',     'db3'),
        ('申万行业',   fetch_sw_daily_by_date,     'sw_daily',     'db3'),
        ('融资融券',   fetch_margin_by_date,       'margin',       'db2'),
        ('融券明细',   fetch_margin_detail_by_date, 'margin_detail','db2'),
        ('股通Top10',  fetch_hsgt_top10_by_date,   'hsgt_top10',   'db2'),
        ('大宗交易',   fetch_block_trade_by_date,  'block_trade',  'db3'),
    ]

    for task_name, fetch_func, table_name, db_key in date_tasks:
        actual_missing = get_missing_dates(db_key, table_name, all_trade_dates)
        _sync_table_by_dates(
            task_name=task_name,
            fetch_func=fetch_func,
            table_name=table_name,
            db_key=db_key,
            missing_dates=actual_missing,
            use_replace=(table_name == 'top_list'),
            workers=4,
            retries=2,
        )


def sync_all(end_date: str = None, dry_run: bool = False,
             full_scan: bool = False, lookback_days: int = 180):
    """
    主入口: 基于差集法检查并补齐所有数据。

    工作方式:
        1. 确定扫描范围 (默认最近 lookback_days 天 / full_scan 全量)
        2. 从交易日历取该范围内的所有交易日
        3. 对每张表做 差集 = 应有交易日 - 已有交易日
        4. 精确补齐缺失的日期

    Args:
        end_date:      截止日期，默认自动检测最新交易日
        dry_run:       仅检查不执行
        full_scan:     从 BACKFILL_START_DATE 全量扫描
        lookback_days: 默认回溯天数

    Returns:
        (latest_trade_date, missing_count)
    """
    # Step 0: 更新交易日历
    print("\n[0] 更新交易日历和股票列表...")
    try:
        fetch_trade_cal()
        fetch_stock_basic()
    except Exception as e:
        print(f"  ⚠ 更新失败（将使用已有数据）: {e}")

    # 并行预热 3 个 DB 连接（TiDB Serverless 冷启动约 3-5 分钟，串行则最多等 15 分钟）
    print("[预热] 唤醒 db1 / db2 / db3 连接...")
    def _ping_db(db_key: str):
        try:
            with get_engine(db_key).connect() as conn:
                conn.execute(text("SELECT 1"))
        except Exception as _e:
            print(f"  ⚠ [{db_key}] 预热失败: {_e}")
    with ThreadPoolExecutor(max_workers=3) as _pool:
        list(_pool.map(_ping_db, ['db1', 'db2', 'db3']))
    print("[预热] 完成\n")

    # 确定目标日期
    target_date = end_date or get_latest_trade_date()

    # 确定扫描起始日期
    if full_scan:
        scan_start = BACKFILL_START_DATE
        scan_mode = f"全量扫描 (从 {BACKFILL_START_DATE})"
    else:
        scan_start = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y%m%d')
        scan_mode = f"近 {lookback_days} 天扫描 (从 {scan_start})"

    # 获取扫描范围内的所有交易日
    all_trade_dates = get_trade_dates_between(scan_start, target_date)
    if not all_trade_dates:
        print(f"\n⚠ 扫描范围 {scan_start}~{target_date} 内无交易日（交易日历可能为空）")
        print("  请先运行 init_batch.py 进行历史回补！")
        return target_date, -1

    print(f"\n[扫描模式] {scan_mode}")
    print(f"[扫描范围] {all_trade_dates[0]} ~ {all_trade_dates[-1]}，共 {len(all_trade_dates)} 个交易日")

    # 检查数据新鲜度（仅展示）
    check_data_freshness()

    if dry_run:
        # Dry run: 列出每张表的缺失情况
        print(f"\n{'='*60}")
        print("[DRY RUN] 各表缺失检测")
        print(f"{'='*60}")

        total_gaps = 0
        for db_key, table, desc in CHECK_TABLES:
            missing = get_missing_dates(db_key, table, all_trade_dates)
            total_gaps += len(missing)
            if missing:
                print(f"  {desc:<12} 缺失 {len(missing):>4} 天  (如 {missing[0]}..{missing[-1]})")
            else:
                print(f"  {desc:<12} ✓ 完整")
        print(f"\n总缺失: {total_gaps} (表×天)")
        return target_date, total_gaps

    # 开始补齐
    start_time = time.time()

    # Step 1: 核心每日数据（每表独立差集）
    print(f"\n[1/2] 核心每日数据补齐")
    core_fixed, core_errors = sync_core_daily(all_trade_dates)

    # Step 2: 辅助数据（每表独立差集）
    print(f"\n[2/2] 辅助数据补齐")
    sync_supplementary(all_trade_dates, target_date)

    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"数据补齐完成! 耗时 {elapsed/60:.1f} 分钟")
    if core_errors > 0:
        print(f"核心层补齐存在 {core_errors} 个错误任务（已自动重试）")
    print(f"{'='*60}")

    # 最终验证
    _, remaining_stale = check_data_freshness()
    return target_date, len(remaining_stale)


# ============================================================
# 供策略脚本调用的便捷函数
# ============================================================

def ensure_data_ready(end_date: str = None, full_scan: bool = False,
                      lookback_days: int = 180) -> str:
    """
    策略执行前调用此函数确保数据就绪。
    返回最新可用的交易日期。

    Args:
        end_date:      补齐到的目标日期，默认自动检测
        full_scan:     全量扫描（从 BACKFILL_START_DATE）
        lookback_days: 回溯天数（默认 180 天）

    使用示例:
        from code.data.sync_daily import ensure_data_ready
        latest = ensure_data_ready()
        print(f"数据已就绪，最新交易日: {latest}")
        # ... 继续执行策略逻辑 ...
    """
    print("\n" + "=" * 60)
    print("数据完整性检查与补齐 (差集法)")
    print(f"检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    latest, missing = sync_all(end_date=end_date, full_scan=full_scan,
                               lookback_days=lookback_days)

    # 以 stock_daily 实际有数据的最新日期为准（防止交易日历超前于盘后数据）
    actual_latest = get_latest_date_in_db('db1', 'stock_daily')
    if actual_latest:
        actual_latest = str(actual_latest).replace('-', '')[:8]
    else:
        actual_latest = str(latest).replace('-', '')[:8] if latest else None

    if missing == 0:
        print(f"\n✓ 数据就绪，可执行策略 (最新交易日: {actual_latest})")
    elif missing > 0:
        print(f"\n⚠ 仍有 {missing} 张表数据滞后，建议检查网络/API 限制")
        if actual_latest and actual_latest != str(latest).replace('-', '')[:8]:
            print(f"  → 将使用最近有数据日期: {actual_latest}（交易日历最新: {latest}）")
    else:
        print(f"\n✗ 核心数据缺失，请先运行完整数据回补")

    return actual_latest


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='QRAI-Trader 近期数据补齐 (差集法)')
    parser.add_argument('--end-date', type=str, help='补齐截止日期 (YYYYMMDD)，默认自动检测')
    parser.add_argument('--dry-run', action='store_true', help='仅检查不执行')
    parser.add_argument('--full-scan', action='store_true',
                        help=f'从 {BACKFILL_START_DATE} 全量扫描（默认仅最近180天）')
    parser.add_argument('--lookback-days', type=int, default=180,
                        help='回溯天数（默认 180），与 --full-scan 互斥')
    args = parser.parse_args()

    if args.dry_run:
        sync_all(end_date=args.end_date, dry_run=True,
                 full_scan=args.full_scan, lookback_days=args.lookback_days)
    else:
        ensure_data_ready(end_date=args.end_date, full_scan=args.full_scan,
                          lookback_days=args.lookback_days)
