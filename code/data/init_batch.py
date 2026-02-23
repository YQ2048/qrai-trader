"""
QRAI-Trader 历史数据批量回补脚本
支持断点续传：自动跳过已入库的日期
"""
import time
from datetime import datetime

from sqlalchemy import text
from code.core.db_manager import get_engine, create_all_tables
from code.data.fetchers import (
    client, fetch_stock_basic, fetch_trade_cal,
    fetch_daily_by_date, fetch_adj_factor_by_date, fetch_daily_basic_by_date,
    fetch_moneyflow_by_date, fetch_moneyflow_hsgt,
    fetch_stk_limit_by_date, fetch_stock_st_by_date,
    fetch_limit_list_d_by_date,
    fetch_forecast_vip, fetch_express_vip, fetch_disclosure_date,
    _save_to_db, get_trade_dates
)
from code.core.config import BACKFILL_START_DATE, BACKFILL_END_DATE


def get_existing_dates(db_key: str, table_name: str) -> set:
    """查询某张表已有哪些 trade_date"""
    engine = get_engine(db_key)
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT DISTINCT trade_date FROM `{table_name}`"))
        dates = {row[0] for row in result}
    engine.dispose()
    return dates


def backfill_daily_data():
    """
    Phase 1: 按日期回补每日数据
    每个交易日拉取: daily + adj_factor + daily_basic + moneyflow + stk_limit
    """
    print("\n" + "=" * 60)
    print("Phase 1: 每日数据批量回补")
    print(f"范围: {BACKFILL_START_DATE} ~ {BACKFILL_END_DATE}")
    print("=" * 60)

    # 获取全部交易日
    all_dates = get_trade_dates()
    print(f"交易日总数: {len(all_dates)}")

    # 检查已入库的日期（用 stock_daily 作为进度标记）
    existing = get_existing_dates('db1', 'stock_daily')
    remaining = [d for d in all_dates if d not in existing]
    print(f"已入库: {len(existing)} 天, 待回补: {len(remaining)} 天")

    if not remaining:
        print("✓ 所有日期已回补完毕，跳过")
        return

    total = len(remaining)
    start_time = time.time()
    success_count = 0
    error_dates = []

    for i, td in enumerate(remaining):
        try:
            # DB1: daily + adj_factor + daily_basic (3 次 API)
            df_daily = fetch_daily_by_date(td)
            df_adj = fetch_adj_factor_by_date(td)
            df_basic = fetch_daily_basic_by_date(td)

            n1 = _save_to_db(df_daily, 'stock_daily', 'db1')
            n2 = _save_to_db(df_adj, 'adj_factor', 'db1')
            n3 = _save_to_db(df_basic, 'daily_basic', 'db1')

            # DB2: moneyflow (1 次 API)
            df_mf = fetch_moneyflow_by_date(td)
            n4 = _save_to_db(df_mf, 'moneyflow', 'db2')

            # DB3: stk_limit (1 次 API)
            df_lim = fetch_stk_limit_by_date(td)
            n5 = _save_to_db(df_lim, 'stk_limit', 'db3')

            success_count += 1

            # 每 10 天或首尾打印进度
            if (i + 1) % 10 == 0 or i == 0 or i == total - 1:
                elapsed = time.time() - start_time
                speed = success_count / elapsed if elapsed > 0 else 0
                eta = (total - i - 1) / speed if speed > 0 else 0
                eta_min = int(eta // 60)
                eta_sec = int(eta % 60)
                print(
                    f"  [{i+1}/{total}] {td}: "
                    f"daily={n1} adj={n2} basic={n3} mf={n4} limit={n5} | "
                    f"速度: {speed:.1f}天/秒 ETA: {eta_min}分{eta_sec}秒"
                )

        except Exception as e:
            error_dates.append(td)
            print(f"  ✗ [{i+1}/{total}] {td}: {e}")
            time.sleep(2)

    elapsed = time.time() - start_time
    print(f"\nPhase 1 完成: 成功 {success_count}/{total}, 耗时 {elapsed/60:.1f} 分钟")
    if error_dates:
        print(f"  失败日期 ({len(error_dates)}): {error_dates[:20]}...")


def backfill_hsgt():
    """Phase 2a: 北向资金（按半年分段，一次性拉完）"""
    print("\n" + "=" * 60)
    print("Phase 2a: 北向资金回补")
    print("=" * 60)
    fetch_moneyflow_hsgt()


def backfill_limit_list():
    """Phase 2b: 涨跌停和炸板数据（数据从 2020 年开始）"""
    print("\n" + "=" * 60)
    print("Phase 2b: 涨跌停和炸板数据回补")
    print("=" * 60)

    all_dates = get_trade_dates(start_date='20210101')
    existing = get_existing_dates('db3', 'limit_list_d')
    remaining = [d for d in all_dates if d not in existing]
    print(f"待回补: {len(remaining)} 天")

    total = len(remaining)
    for i, td in enumerate(remaining):
        try:
            df = fetch_limit_list_d_by_date(td)
            n = _save_to_db(df, 'limit_list_d', 'db3')
            if (i + 1) % 20 == 0 or i == 0:
                print(f"  [{i+1}/{total}] {td}: {n} 条")
        except Exception as e:
            print(f"  ✗ [{i+1}/{total}] {td}: {e}")
            time.sleep(1)


def backfill_st():
    """Phase 2c: ST 股票列表（每月首个交易日取一次即可）"""
    print("\n" + "=" * 60)
    print("Phase 2c: ST 股票列表回补（月频）")
    print("=" * 60)

    all_dates = get_trade_dates()
    # 每月取第一个交易日
    monthly_dates = []
    seen_months = set()
    for d in all_dates:
        month_key = d[:6]
        if month_key not in seen_months:
            seen_months.add(month_key)
            monthly_dates.append(d)

    existing = get_existing_dates('db3', 'stock_st')
    remaining = [d for d in monthly_dates if d not in existing]
    print(f"待回补: {len(remaining)} 个月")

    for i, td in enumerate(remaining):
        try:
            df = fetch_stock_st_by_date(td)
            n = _save_to_db(df, 'stock_st', 'db3')
            print(f"  [{i+1}/{len(remaining)}] {td}: {n} 条")
        except Exception as e:
            print(f"  ✗ {td}: {e}")
            time.sleep(1)


def backfill_earnings():
    """Phase 2d: 业绩预告 + 业绩快报 + 财报披露计划"""
    print("\n" + "=" * 60)
    print("Phase 2d: 业绩数据回补")
    print("=" * 60)

    periods = []
    for year in range(2021, 2027):
        for q in ['0331', '0630', '0930', '1231']:
            periods.append(f'{year}{q}')

    # 业绩预告
    print("\n[forecast] 业绩预告...")
    for period in periods:
        try:
            df = fetch_forecast_vip(period)
            n = _save_to_db(df, 'forecast', 'db3')
            if n > 0:
                print(f"  {period}: {n} 条")
        except Exception as e:
            print(f"  ✗ {period}: {e}")
            time.sleep(1)

    # 业绩快报
    print("\n[express] 业绩快报...")
    for period in periods:
        try:
            df = fetch_express_vip(period)
            n = _save_to_db(df, 'express', 'db3')
            if n > 0:
                print(f"  {period}: {n} 条")
        except Exception as e:
            print(f"  ✗ {period}: {e}")
            time.sleep(1)

    # 财报披露计划
    print("\n[disclosure_date] 财报披露计划...")
    for period in periods:
        try:
            df = fetch_disclosure_date(period)
            n = _save_to_db(df, 'disclosure_date', 'db3')
            if n > 0:
                print(f"  {period}: {n} 条")
        except Exception as e:
            print(f"  ✗ {period}: {e}")
            time.sleep(1)


if __name__ == '__main__':
    print("=" * 60)
    print(f"QRAI-Trader Phase 1: 核心每日数据回补 ({BACKFILL_START_DATE} ~ {BACKFILL_END_DATE})")
    print(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # 确保表已创建
    create_all_tables()

    # 确保基础数据最新
    fetch_stock_basic()
    fetch_trade_cal()

    # Phase 1: 核心每日数据（daily + adj_factor + daily_basic + moneyflow + stk_limit）
    # 注意: 辅助数据（北向资金/涨跌停炸板/ST/业绩）已移至 init_aux.py
    #       扩展数据（融资融券/龙虎榜/申万行业/筹码等）由 init_extended.py 负责
    backfill_daily_data()

    print("\n" + "=" * 60)
    print(f"Phase 1 完成! {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("提示: 请运行 init_aux.py 补充辅助数据，init_extended.py 补充扩展数据")
    print("=" * 60)
