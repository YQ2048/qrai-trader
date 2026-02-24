"""
独立筹码胜率同步任务（run_sync_cyq.py）

S32"筹码低位锁仓"是唯一依赖 cyq_perf 数据的策略，但 cyq_perf 按股票拉取，
5000+ 只股票耗时较长（单 Token ~32分钟，双 Token ~16分钟），不适合卡在每日
主流程 sync_daily 里阻塞后续策略执行。

本脚本作为独立 job，可单独触发或挂载为"闲暇任务"：
  - 探针验证每个 Token 的有效性，自动跳过失效 Token
  - 多 Token 并行，每 Token 限 170次/分（留 15% 余量，实际上限 200/分）
  - 支持仅同步活跃股（排除 ST、小市值），进一步缩短耗时
  - 支持指定日期 / 自动检测缺失日期

用法：
    # 自动检测缺失日期并补齐（排除 ST）
    python -m code.jobs.run_sync_cyq

    # 指定目标日期
    python -m code.jobs.run_sync_cyq --date 20260224

    # 全量股票（含 ST 和小市值）
    python -m code.jobs.run_sync_cyq --all-stocks

    # 仅显示缺失情况，不执行补齐
    python -m code.jobs.run_sync_cyq --dry-run
"""
import time
import threading
import argparse
from datetime import datetime, timedelta

from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor, as_completed

from code.core.db_manager import get_engine
from code.core.logger import get_logger
from code.data.fetchers import client, _save_to_db
from code.data.sync_daily import get_trade_dates_between, get_missing_dates, get_latest_trade_date

logger = get_logger(__name__)

_PER_TOKEN_INTERVAL = 60.0 / 170   # ≈ 0.353s，单 Token 170次/分（上限 200/分）


# ============================================================
# Token 探针验证
# ============================================================

def validate_tokens(start_date: str) -> list:
    """探针验证每个 Token 是否可用，返回可用 (index, state) 列表"""
    valid = []
    for i, state in enumerate(client._states):
        try:
            df = state['pro'].cyq_perf(
                ts_code='000001.SZ', start_date=start_date, end_date=start_date
            )
            valid.append((i, state))
            print(f"  Token #{i + 1} ✓ 有效")
        except Exception as e:
            print(f"  Token #{i + 1} ✗ 无效（{str(e)[:80]}），已跳过")
    return valid


# ============================================================
# 股票列表获取
# ============================================================

def get_stock_list(all_stocks: bool = False) -> list:
    """
    获取需要同步筹码的股票列表。

    S32"筹码低位锁仓"要求：winner_rate > 90%、chip_peak_cv_10 < 0.05。
    ST 股和极小市值股极少触发该策略，默认过滤以减少请求量。
    - all_stocks=False：排除 ST / *ST（约减少 10-15% 股票数）
    - all_stocks=True ：全量上市股票
    """
    engine = get_engine('db1')
    with engine.connect() as conn:
        if all_stocks:
            sql = "SELECT ts_code FROM stock_basic WHERE list_status = 'L' ORDER BY ts_code"
        else:
            sql = (
                "SELECT ts_code FROM stock_basic "
                "WHERE list_status = 'L' AND name NOT LIKE 'ST%' AND name NOT LIKE '%ST%' "
                "ORDER BY ts_code"
            )
        result = conn.execute(text(sql))
        stocks = [row[0] for row in result]
    return stocks


# ============================================================
# 核心：多 Token 并行拉取
# ============================================================

def sync_cyq_perf(
    start_date: str,
    end_date: str,
    stock_list: list,
    valid_states: list,
) -> tuple[int, int]:
    """
    多 Token 并行拉取 cyq_perf，每个 Token 独立速率锁。

    Returns:
        (success_count, error_count)
    """
    n_tok = len(valid_states)
    total = len(stock_list)
    tok_locks = [threading.Lock() for _ in valid_states]
    tok_last  = [0.0] * n_tok

    def _fetch(ts_code: str, slot: int):
        lock  = tok_locks[slot]
        state = valid_states[slot][1]
        with lock:
            elapsed = time.time() - tok_last[slot]
            if elapsed < _PER_TOKEN_INTERVAL:
                time.sleep(_PER_TOKEN_INTERVAL - elapsed)
            tok_last[slot] = time.time()
        # 锁外发起调用，不阻塞其他 Token 的计时
        for attempt in range(2):
            try:
                return state['pro'].cyq_perf(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                )
            except Exception as e:
                if attempt == 0:
                    time.sleep(5.0)
                else:
                    raise

    success = 0
    errors  = 0
    est_min = total / (170 * n_tok)
    print(f"  开始拉取: {total} 只股票 × {n_tok} Token，预计 {est_min:.0f} 分钟")

    with ThreadPoolExecutor(max_workers=n_tok) as pool:
        future_map = {
            pool.submit(_fetch, ts_code, i % n_tok): ts_code
            for i, ts_code in enumerate(stock_list)
        }
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
                if errors <= 10:
                    print(f"    ✗ {ts_code}: {str(e)[:80]}")

            if done % 500 == 0 or done == total:
                elapsed_s = time.time()
                print(f"    进度: {done}/{total}  成功={success}  失败={errors}")

    return success, errors


# ============================================================
# 主入口
# ============================================================

def run(
    target_date: str = None,
    lookback_days: int = 7,
    all_stocks: bool = False,
    dry_run: bool = False,
):
    print(f"\n{'='*60}")
    print(f"筹码胜率独立同步任务")
    print(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    # 确定扫描范围
    latest = target_date or get_latest_trade_date()
    scan_start = (datetime.strptime(latest, '%Y%m%d') - timedelta(days=lookback_days * 2)).strftime('%Y%m%d')
    all_dates = get_trade_dates_between(scan_start, latest)

    missing = get_missing_dates('db2', 'cyq_perf', all_dates)
    if not missing:
        print(f"✓ cyq_perf 数据完整（最近 {lookback_days} 天扫描范围内无缺失）")
        return

    start_date = missing[0]
    end_date   = missing[-1]
    print(f"检测到缺失 {len(missing)} 个交易日: {start_date} ~ {end_date}")

    stock_list = get_stock_list(all_stocks=all_stocks)
    print(f"股票范围: {'全量' if all_stocks else '非ST'} {len(stock_list)} 只")

    if dry_run:
        est_1tok = len(stock_list) / 170
        print(f"\n[DRY RUN] 无实际拉取")
        print(f"  股票数: {len(stock_list)}")
        print(f"  缺失天数: {len(missing)}")
        print(f"  预计耗时: 1 Token ≈ {est_1tok:.0f}分钟，2 Token ≈ {est_1tok/2:.0f}分钟")
        return

    # Token 探针验证
    print(f"\n验证 Token 有效性...")
    valid_states = validate_tokens(start_date)
    if not valid_states:
        print("✗ 所有 Token 均无效，任务终止")
        return

    # 执行拉取
    t0 = time.time()
    success, errors = sync_cyq_perf(start_date, end_date, stock_list, valid_states)
    elapsed = (time.time() - t0) / 60

    print(f"\n{'='*60}")
    print(f"筹码同步完成  耗时 {elapsed:.1f} 分钟")
    print(f"成功: {success}/{len(stock_list)}  失败: {errors}")
    if errors > 0:
        print(f"⚠ 有 {errors} 只股票拉取失败，下次运行会自动检测并重试")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description='QRAI-Trader 筹码胜率独立同步')
    parser.add_argument('--date', type=str, help='目标交易日 YYYYMMDD，默认自动检测')
    parser.add_argument('--lookback-days', type=int, default=7,
                        help='向前扫描天数（默认 7，即近 7 个自然日）')
    parser.add_argument('--all-stocks', action='store_true',
                        help='全量股票（含 ST），默认排除 ST 以减少请求量')
    parser.add_argument('--dry-run', action='store_true', help='仅估算，不实际拉取')
    args = parser.parse_args()

    run(
        target_date=args.date,
        lookback_days=args.lookback_days,
        all_stocks=args.all_stocks,
        dry_run=args.dry_run,
    )


if __name__ == '__main__':
    main()
