"""
Phase 2 并行回补脚本
可与 init_batch.py (Phase 1) 同时运行
包含: 北向资金 / 涨跌停炸板 / ST股票 / 业绩数据

两个进程共享同一 Token，Tushare 限制 800次/分钟。
Phase 1 约 100次/分钟，Phase 2 约 60次/分钟，合计远低于限制。
"""
import time
from datetime import datetime

from code.data.fetchers import (
    client, fetch_moneyflow_hsgt,
    fetch_limit_list_d_by_date, fetch_stock_st_by_date,
    fetch_forecast_vip, fetch_express_vip, fetch_disclosure_date,
    fetch_concept_list, fetch_concept_detail,
    _save_to_db, get_trade_dates
)
from code.data.init_batch import get_existing_dates


def task_hsgt():
    """Phase 2a: 北向资金"""
    print("[2a] 北向资金回补...")
    try:
        fetch_moneyflow_hsgt()
        print("[2a] ✓ 北向资金完成")
    except Exception as e:
        print(f"[2a] ✗ 北向资金失败: {e}")


def task_limit_list():
    """Phase 2b: 涨跌停和炸板数据"""
    print("[2b] 涨跌停和炸板数据回补...")
    all_dates = get_trade_dates(start_date='20210101')
    existing = get_existing_dates('db3', 'limit_list_d')
    remaining = [d for d in all_dates if d not in existing]
    print(f"[2b] 待回补: {len(remaining)} 天")

    total = len(remaining)
    success = 0
    for i, td in enumerate(remaining):
        try:
            df = fetch_limit_list_d_by_date(td)
            n = _save_to_db(df, 'limit_list_d', 'db3')
            success += 1
            if (i + 1) % 20 == 0 or i == 0 or i == total - 1:
                print(f"[2b]   [{i+1}/{total}] {td}: {n} 条")
        except Exception as e:
            print(f"[2b]   ✗ [{i+1}/{total}] {td}: {e}")
            time.sleep(1)

    print(f"[2b] ✓ 涨跌停完成: {success}/{total}")


def task_st():
    """Phase 2c: ST 股票列表（月频）"""
    print("[2c] ST 股票列表回补...")
    all_dates = get_trade_dates()
    monthly_dates = []
    seen_months = set()
    for d in all_dates:
        month_key = d[:6]
        if month_key not in seen_months:
            seen_months.add(month_key)
            monthly_dates.append(d)

    existing = get_existing_dates('db3', 'stock_st')
    remaining = [d for d in monthly_dates if d not in existing]
    print(f"[2c] 待回补: {len(remaining)} 个月")

    success = 0
    for i, td in enumerate(remaining):
        try:
            df = fetch_stock_st_by_date(td)
            n = _save_to_db(df, 'stock_st', 'db3')
            success += 1
            print(f"[2c]   [{i+1}/{len(remaining)}] {td}: {n} 条")
        except Exception as e:
            print(f"[2c]   ✗ {td}: {e}")
            time.sleep(1)

    print(f"[2c] ✓ ST列表完成: {success}/{len(remaining)}")


def task_earnings():
    """Phase 2d: 业绩预告 + 业绩快报 + 财报披露计划"""
    print("[2d] 业绩数据回补...")

    periods = []
    for year in range(2021, 2027):
        for q in ['0331', '0630', '0930', '1231']:
            periods.append(f'{year}{q}')

    # 业绩预告
    print("[2d] [forecast] 业绩预告...")
    for period in periods:
        try:
            df = fetch_forecast_vip(period)
            n = _save_to_db(df, 'forecast', 'db3')
            if n > 0:
                print(f"[2d]   forecast {period}: {n} 条")
        except Exception as e:
            print(f"[2d]   ✗ forecast {period}: {e}")
            time.sleep(1)

    # 业绩快报
    print("[2d] [express] 业绩快报...")
    for period in periods:
        try:
            df = fetch_express_vip(period)
            n = _save_to_db(df, 'express', 'db3')
            if n > 0:
                print(f"[2d]   express {period}: {n} 条")
        except Exception as e:
            print(f"[2d]   ✗ express {period}: {e}")
            time.sleep(1)

    # 财报披露计划
    print("[2d] [disclosure_date] 财报披露计划...")
    for period in periods:
        try:
            df = fetch_disclosure_date(period)
            n = _save_to_db(df, 'disclosure_date', 'db3')
            if n > 0:
                print(f"[2d]   disclosure {period}: {n} 条")
        except Exception as e:
            print(f"[2d]   ✗ disclosure {period}: {e}")
            time.sleep(1)

    print("[2d] ✓ 业绩数据完成")


def task_policy_theme():
    """Phase 2e: 十五五政策主题概念成分股（月频）

    通过 Tushare concept + concept_detail 接口，拉取命中政策关键词的
    所有概念板块成分股，写入 DB3.policy_theme_stocks。
    精确度远优于申万行业名称关键词匹配。
    """
    from code.core.config import POLICY_THEME_KEYWORDS
    from code.core.db_manager import get_engine
    from sqlalchemy import text
    from datetime import datetime
    import pandas as pd

    print("[2e] 政策主题概念成分股同步...")
    today_str = datetime.now().strftime("%Y%m%d")

    # Step 1: 拉全量概念列表
    concepts_df = fetch_concept_list()
    if concepts_df is None or concepts_df.empty:
        print("[2e] ✗ 拉取概念列表失败，跳过")
        return

    # Step 2: 过滤命中政策关键词的概念
    def _matches_any_keyword(name: str) -> bool:
        if not isinstance(name, str):
            return False
        name_lower = name.lower()
        return any(kw.lower() in name_lower for kw in POLICY_THEME_KEYWORDS)

    matched = concepts_df[concepts_df['concept_name'].apply(_matches_any_keyword)].copy()
    print(f"[2e] 匹配到 {len(matched)} 个概念板块:")
    for _, r in matched.iterrows():
        print(f"[2e]   {r['concept_name']} (id={r['code']})")

    if matched.empty:
        print("[2e] 警告: 未匹配到任何概念，请检查 POLICY_THEME_KEYWORDS 配置")
        return

    # Step 3: 逐概念拉成分股
    all_members = []
    for _, row in matched.iterrows():
        concept_id   = row['code']
        concept_name = row['concept_name']
        try:
            detail = fetch_concept_detail(concept_id)
            if detail is not None and not detail.empty:
                detail['concept_name'] = concept_name
                detail['updated_date'] = today_str
                all_members.append(detail)
                print(f"[2e]   ✓ {concept_name}: {len(detail)} 只")
            else:
                print(f"[2e]   - {concept_name}: 空数据，跳过")
        except Exception as e:
            print(f"[2e]   ✗ {concept_name} ({concept_id}): {e}")
            time.sleep(1)

    if not all_members:
        print("[2e] ✗ 未拉到任何成分股数据")
        return

    # Step 4: 合并去重，全量覆盖写入 DB3
    result_df = pd.concat(all_members, ignore_index=True)
    result_df = result_df.dropna(subset=['ts_code', 'concept_id'])
    result_df = result_df.drop_duplicates(subset=['ts_code', 'concept_id'])

    engine = get_engine('db3')
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM policy_theme_stocks"))
        conn.commit()

    n = _save_to_db(result_df, 'policy_theme_stocks', 'db3')
    print(f"[2e] ✓ 政策主题同步完成: {n} 条记录，"
          f"{result_df['ts_code'].nunique()} 只不重复股票，"
          f"{result_df['concept_id'].nunique()} 个概念")


if __name__ == '__main__':
    print("=" * 60)
    print("Phase 2 并行回补（与 Phase 1 同时运行）")
    print(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # 2a (北向资金) 和 2d (业绩) 数据量小、调用少，先串行快速完成
    task_hsgt()
    task_earnings()

    # 2c (ST) 数据量小，也先完成
    task_st()

    # 2b (涨跌停) 数据量最大，最后跑
    task_limit_list()

    # 2e (政策主题概念成分股) 月频同步，末尾执行
    task_policy_theme()

    print("\n" + "=" * 60)
    print(f"Phase 2 全部完成! {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
