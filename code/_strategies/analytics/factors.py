from datetime import datetime, timedelta
from functools import lru_cache
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import text

from code.core.config import CALENDAR_BIAS_MAP, POLICY_CATALYST_DATES, POLICY_CATALYST_WINDOW_DAYS, POLICY_THEME_KEYWORDS
from code.core.db_manager import get_engine
from code.core.logger import get_logger

logger = get_logger(__name__)


def _yyyymmdd_to_date(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y%m%d")


def _infer_calendar_bias(trade_date: str) -> str:
    month = datetime.strptime(trade_date, "%Y%m%d").month
    return CALENDAR_BIAS_MAP.get(month, "NEUTRAL")


def _is_policy_catalyst_active(trade_date: str) -> int:
    if not POLICY_CATALYST_DATES:
        return 0

    current = datetime.strptime(trade_date, "%Y%m%d")
    for raw_date in POLICY_CATALYST_DATES:
        try:
            catalyst = datetime.strptime(raw_date, "%Y%m%d")
        except ValueError:
            continue
        delta_days = (current - catalyst).days
        if 0 <= delta_days <= POLICY_CATALYST_WINDOW_DAYS:
            return 1
    return 0


def _is_policy_theme_industry(industry: str) -> int:
    """降级 fallback：对申万行业名做关键词匹配（DB 数据不可用时使用）"""
    if not isinstance(industry, str) or not industry.strip():
        return 0
    text_lower = industry.strip().lower()
    for keyword in POLICY_THEME_KEYWORDS:
        if keyword.lower() in text_lower:
            return 1
    return 0


@lru_cache(maxsize=1)
def _load_policy_theme_stock_set() -> frozenset:
    """从 DB3.policy_theme_stocks 加载精准概念成分股集合（进程内缓存一次）。
    失败时返回空集合，调用方会自动降级为行业关键词匹配。
    """
    try:
        engine = get_engine("db3")
        sql = text("SELECT DISTINCT ts_code FROM policy_theme_stocks")
        with engine.connect() as conn:
            rows = conn.execute(sql).fetchall()
        result = frozenset(r[0] for r in rows)
        if result:
            logger.info("policy_theme_stocks 加载完成: %d 只股票", len(result))
            return result
        logger.warning("policy_theme_stocks 表为空，降级为行业关键词匹配")
    except Exception as e:
        logger.warning("加载 policy_theme_stocks 失败，降级为行业关键词匹配: %s", e)
    return frozenset()


def _apply_policy_theme_hit(df: pd.DataFrame) -> pd.DataFrame:
    """统一计算 policy_theme_hit 列：优先 DB 精准匹配，空集合时降级关键词匹配。"""
    theme_stocks = _load_policy_theme_stock_set()
    if theme_stocks:
        df["policy_theme_hit"] = df["ts_code"].isin(theme_stocks).astype(int)
    else:
        df["policy_theme_hit"] = df["industry"].fillna("").apply(_is_policy_theme_industry)
    return df


def _load_price_panel(start_date: str, end_date: str) -> pd.DataFrame:
    engine = get_engine("db1")
    sql = text(
        """
        SELECT d.ts_code, d.trade_date, d.open, d.high, d.low, d.close, d.pct_chg, d.vol, d.amount,
               a.adj_factor
        FROM stock_daily d
        LEFT JOIN adj_factor a
          ON d.ts_code = a.ts_code AND d.trade_date = a.trade_date
        WHERE d.trade_date BETWEEN :start_date AND :end_date
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"start_date": start_date, "end_date": end_date})
    return df


def _load_daily_basic(trade_date: str) -> pd.DataFrame:
    engine = get_engine("db1")
    with engine.connect() as conn:
        cols = conn.execute(text("SHOW COLUMNS FROM daily_basic")).fetchall()
        col_names = {str(r[0]).lower() for r in cols}

    optional_cols = []
    if "ps_ttm" in col_names:
        optional_cols.append("ps_ttm")
    if "peg" in col_names:
        optional_cols.append("peg")

    select_cols = ["ts_code", "trade_date", "turnover_rate", "pe_ttm", "circ_mv", *optional_cols]
    sql = text(
        f"""
        SELECT {', '.join(select_cols)}
        FROM daily_basic
        WHERE trade_date = :trade_date
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"trade_date": trade_date})

    if "ps_ttm" not in df.columns:
        df["ps_ttm"] = pd.NA
    if "peg" not in df.columns:
        df["peg"] = pd.NA
    return df


def _load_chip_panel(start_date: str, end_date: str) -> pd.DataFrame:
    engine2 = get_engine("db2")

    sql = text(
        """
        SELECT ts_code, trade_date, winner_rate, cost_50pct AS chip_peak_price,
               cost_85pct, cost_15pct
        FROM cyq_perf
        WHERE trade_date BETWEEN :start_date AND :end_date
        """
    )

    try:
        with engine2.connect() as conn:
            df = pd.read_sql(sql, conn, params={"start_date": start_date, "end_date": end_date})
    except Exception as e:
        logger.warning("加载筹码数据失败: %s", e)
        df = pd.DataFrame(columns=["ts_code", "trade_date", "winner_rate", "chip_peak_price", "cost_85pct", "cost_15pct"])

    # cost_band_90: 90%筹码集中度 = (cost_85pct - cost_15pct) / cost_50pct
    if not df.empty and "cost_85pct" in df.columns and "cost_15pct" in df.columns:
        peak = pd.to_numeric(df["chip_peak_price"], errors="coerce")
        df["cost_band_90"] = np.where(
            peak.fillna(0) > 0,
            (pd.to_numeric(df["cost_85pct"], errors="coerce") - pd.to_numeric(df["cost_15pct"], errors="coerce")) / peak,
            np.nan,
        )
    else:
        df["cost_band_90"] = np.nan

    return df


def _load_moneyflow(trade_date: str) -> pd.DataFrame:
    engine2 = get_engine("db2")

    sql = text(
        """
        SELECT ts_code, trade_date, net_mf_amount
        FROM moneyflow
        WHERE trade_date = :trade_date
        """
    )

    with engine2.connect() as conn:
        df = pd.read_sql(sql, conn, params={"trade_date": trade_date})

    return df


def _load_stock_basic() -> pd.DataFrame:
    engine = get_engine("db1")
    sql = text("SELECT ts_code, name, industry, list_date, delist_date FROM stock_basic")
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    return df


def _load_top_inst_agg(trade_date: str) -> pd.DataFrame:
    engine3 = get_engine("db3")
    sql = text(
        """
        SELECT
            ts_code,
            trade_date,
            COUNT(*) AS inst_count,
            SUM(COALESCE(net_buy, 0)) AS inst_net_buy
        FROM top_inst
        WHERE trade_date = :trade_date
        GROUP BY ts_code, trade_date
        """
    )
    with engine3.connect() as conn:
        df = pd.read_sql(sql, conn, params={"trade_date": trade_date})
    return df


def _load_moneyflow_hsgt_panel(start_date: str, end_date: str) -> pd.DataFrame:
    engine2 = get_engine("db2")
    sql = text(
        """
        SELECT trade_date, north_money
        FROM moneyflow_hsgt
        WHERE trade_date BETWEEN :start_date AND :end_date
        ORDER BY trade_date
        """
    )
    with engine2.connect() as conn:
        df = pd.read_sql(sql, conn, params={"start_date": start_date, "end_date": end_date})
    return df


def _load_forecast_latest(trade_date: str) -> pd.DataFrame:
    engine3 = get_engine("db3")
    sql = text(
        """
        SELECT ts_code, forecast_type, ann_date
        FROM (
            SELECT
                ts_code,
                type AS forecast_type,
                ann_date,
                ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY ann_date DESC) AS rn
            FROM forecast
            WHERE ann_date <= :trade_date
        ) t
        WHERE rn = 1
        """
    )
    try:
        with engine3.connect() as conn:
            df = pd.read_sql(sql, conn, params={"trade_date": trade_date})
    except Exception as e:
        logger.warning("加载业绩预告失败: %s", e)
        df = pd.DataFrame(columns=["ts_code", "forecast_type", "ann_date"])
    return df


def _compute_price_factors(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
    df = df.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)

    df["adj_factor"] = df.groupby("ts_code")["adj_factor"].ffill().bfill()
    df["open_adj"] = df["open"] * df["adj_factor"]
    df["high_adj"] = df["high"] * df["adj_factor"]
    df["low_adj"] = df["low"] * df["adj_factor"]
    df["close_adj"] = df["close"] * df["adj_factor"]

    g = df.groupby("ts_code", group_keys=False)

    df["ma20"] = g["close_adj"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    df["ma55"] = g["close_adj"].transform(lambda s: s.rolling(55, min_periods=55).mean())
    df["ma120"] = g["close_adj"].transform(lambda s: s.rolling(120, min_periods=120).mean())
    df["ma250"] = g["close_adj"].transform(lambda s: s.rolling(250, min_periods=250).mean())

    df["vol_ma5_prev"] = g["vol"].transform(lambda s: s.rolling(5, min_periods=5).mean().shift(1))
    df["vol_ma30_prev"] = g["vol"].transform(lambda s: s.rolling(30, min_periods=30).mean().shift(1))
    df["amount_ma20_prev"] = g["amount"].transform(lambda s: s.rolling(20, min_periods=20).mean().shift(1))

    df["pct_chg_20d"] = g["close_adj"].transform(lambda s: s / s.shift(20) - 1)
    df["pct_chg_60d"] = g["close_adj"].transform(lambda s: s / s.shift(60) - 1)
    df["pct_chg_1d"] = g["close_adj"].transform(lambda s: s / s.shift(1) - 1)

    df["rps_20"] = df.groupby("trade_date")["pct_chg_20d"].rank(pct=True) * 100
    df["rps_60"] = df.groupby("trade_date")["pct_chg_60d"].rank(pct=True) * 100

    mean_ma = (df["ma55"] + df["ma120"] + df["ma250"]) / 3
    max_ma = df[["ma55", "ma120", "ma250"]].max(axis=1)
    min_ma = df[["ma55", "ma120", "ma250"]].min(axis=1)

    df["ma_dispersion"] = (max_ma - min_ma) / mean_ma
    df["vol_ratio_5"] = df["vol"] / df["vol_ma5_prev"]
    df["g_point_strength"] = df["vol"] / df["vol_ma30_prev"]
    df["close_to_ma250"] = df["close_adj"] / df["ma250"]
    df["cross3_vol_ratio"] = df["vol_ratio_5"]

    limit_flag = (df["pct_chg"] >= 9.5).astype(int)
    df["limit_flag"] = limit_flag
    df["had_limit_10d"] = g["limit_flag"].transform(lambda s: s.shift(1).rolling(10, min_periods=1).max())

    df["limit_vol_ref"] = df["vol"].where(df["limit_flag"] == 1)
    df["limit_vol_ref"] = g["limit_vol_ref"].transform(lambda s: s.shift(1).ffill())
    df["vol_recent_3d_min"] = g["vol"].transform(lambda s: s.rolling(3, min_periods=3).min())
    df["l_shape_shrink_ratio"] = df["vol_recent_3d_min"] / df["limit_vol_ref"]

    # S22 附加约束：当日成交量是否为近20日最低
    df["vol_llv_20"] = g["vol"].transform(lambda s: s.rolling(20, min_periods=20).min())
    df["vol_at_llv_20"] = (df["vol"] <= df["vol_llv_20"]).astype(int)

    rolling_high_5 = g["high_adj"].transform(lambda s: s.rolling(5, min_periods=5).max())
    rolling_low_5 = g["low_adj"].transform(lambda s: s.rolling(5, min_periods=5).min())
    rolling_close_5 = g["close_adj"].transform(lambda s: s.rolling(5, min_periods=5).mean())
    df["amplitude_5d"] = (rolling_high_5 - rolling_low_5) / rolling_close_5

    max_ma_all = df[["ma55", "ma120", "ma250"]].max(axis=1)
    df["cross3_close_break"] = (df["close_adj"] > max_ma_all).astype(int)
    df["cross3_trend_strength"] = df["pct_chg_1d"]

    # 决策支持置信度：ATR 趋势
    prev_close_atr = g["close_adj"].transform(lambda s: s.shift(1))
    tr_hl = df["high_adj"] - df["low_adj"]
    tr_hc = (df["high_adj"] - prev_close_atr).abs()
    tr_lc = (df["low_adj"] - prev_close_atr).abs()
    df["true_range"] = pd.concat([tr_hl, tr_hc, tr_lc], axis=1).max(axis=1)
    df["atr_14"] = g["true_range"].transform(lambda s: s.rolling(14, min_periods=14).mean())
    df["atr_14_trend_5"] = g["atr_14"].transform(lambda s: s - s.shift(5))

    # S24 仙人指路反包
    prev_high = g["high_adj"].transform(lambda s: s.shift(1))
    prev_close_s24 = g["close_adj"].transform(lambda s: s.shift(1))
    prev_open = g["open_adj"].transform(lambda s: s.shift(1))
    prev_vol = g["vol"].transform(lambda s: s.shift(1))
    df["guide_shadow_pct"] = (prev_high - prev_close_s24) / prev_close_s24
    df["guide_body_pct"] = (prev_open - prev_close_s24).abs() / prev_close_s24
    df["reversal_flag"] = (
        (df["guide_shadow_pct"] > 0.03) &
        (df["guide_body_pct"] < 0.015) &
        (df["close_adj"] > prev_high) &
        (df["vol"] > prev_vol)
    ).astype(int)

    # S25 釜底抽薪
    df["false_break_depth"] = (df["ma20"] - df["low_adj"]) / df["ma20"]
    df["lower_shadow_pct"] = (df[["open_adj", "close_adj"]].min(axis=1) - df["low_adj"]) / df["close_adj"]

    # S26 黄金分割低吸
    swing_high_60 = g["high_adj"].transform(lambda s: s.rolling(60, min_periods=20).max())
    swing_low_60 = g["low_adj"].transform(lambda s: s.rolling(60, min_periods=20).min())
    df["golden_buy_price"] = swing_high_60 - (swing_high_60 - swing_low_60) * 0.618
    df["distance_to_618"] = (df["close_adj"] - df["golden_buy_price"]).abs() / df["golden_buy_price"]

    return df


def load_factor_snapshot(trade_date: str, lookback_days: int = 260) -> pd.DataFrame:
    end_dt = _yyyymmdd_to_date(trade_date)
    start_dt = end_dt - timedelta(days=lookback_days)
    start_date = start_dt.strftime("%Y%m%d")

    price_df = _load_price_panel(start_date=start_date, end_date=trade_date)
    if price_df.empty:
        return pd.DataFrame()

    factor_df = _compute_price_factors(price_df)
    latest_df = factor_df[factor_df["trade_date"] == pd.to_datetime(trade_date, format="%Y%m%d")].copy()
    if latest_df.empty:
        return pd.DataFrame()

    latest_df["trade_date"] = latest_df["trade_date"].dt.strftime("%Y%m%d")

    daily_basic_df = _load_daily_basic(trade_date)
    chip_panel_df = _load_chip_panel(start_date=start_date, end_date=trade_date)
    moneyflow_df = _load_moneyflow(trade_date)
    stock_basic_df = _load_stock_basic()
    top_inst_df = _load_top_inst_agg(trade_date)
    hsgt_panel_df = _load_moneyflow_hsgt_panel(start_date=start_date, end_date=trade_date)
    forecast_df = _load_forecast_latest(trade_date)

    industry_slope_df = pd.DataFrame(columns=["ts_code", "trade_date", "industry_rps_slope_3d"])
    if not factor_df.empty and not stock_basic_df.empty:
        panel_with_industry = factor_df.merge(stock_basic_df[["ts_code", "industry"]], on="ts_code", how="left")
        industry_daily = (
            panel_with_industry
            .dropna(subset=["industry"])
            .groupby(["trade_date", "industry"], as_index=False)["rps_20"]
            .mean()
            .sort_values(["industry", "trade_date"])
        )
        industry_daily["industry_rps_slope_3d"] = (
            industry_daily.groupby("industry")["rps_20"].transform(lambda s: (s - s.shift(2)) / 2)
        )
        latest_industry = industry_daily[
            industry_daily["trade_date"] == pd.to_datetime(trade_date, format="%Y%m%d")
        ][["industry", "industry_rps_slope_3d"]]
        if not latest_industry.empty:
            industry_slope_df = latest_df[["ts_code", "trade_date"]].merge(
                stock_basic_df[["ts_code", "industry"]], on="ts_code", how="left"
            ).merge(latest_industry, on="industry", how="left")[["ts_code", "trade_date", "industry_rps_slope_3d"]]
            industry_slope_df["trade_date"] = industry_slope_df["trade_date"].dt.strftime("%Y%m%d")

    north_context_df = pd.DataFrame(columns=["trade_date", "north_money", "north_consecutive_days", "north_sum_3d"])
    if not hsgt_panel_df.empty:
        hsgt_panel_df = hsgt_panel_df.copy()
        hsgt_panel_df["trade_date"] = pd.to_datetime(hsgt_panel_df["trade_date"], format="%Y%m%d")
        hsgt_panel_df = hsgt_panel_df.sort_values("trade_date").reset_index(drop=True)
        inflow_flag = (hsgt_panel_df["north_money"] > 0).astype(int)
        hsgt_panel_df["north_consecutive_days"] = inflow_flag * (
            inflow_flag.groupby((inflow_flag == 0).cumsum()).cumcount() + 1
        )
        hsgt_panel_df["north_sum_3d"] = hsgt_panel_df["north_money"].rolling(3, min_periods=3).sum()

        north_context_df = hsgt_panel_df[
            hsgt_panel_df["trade_date"] == pd.to_datetime(trade_date, format="%Y%m%d")
        ][["trade_date", "north_money", "north_consecutive_days", "north_sum_3d"]].copy()
        north_context_df["trade_date"] = north_context_df["trade_date"].dt.strftime("%Y%m%d")

    chip_latest_df = pd.DataFrame(columns=["ts_code", "trade_date", "winner_rate", "chip_peak_price", "chip_peak_cv_10", "cost_band_90"])
    if not chip_panel_df.empty:
        chip_panel_df = chip_panel_df.copy()
        chip_panel_df["trade_date"] = pd.to_datetime(chip_panel_df["trade_date"], format="%Y%m%d")
        chip_panel_df = chip_panel_df.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)

        g = chip_panel_df.groupby("ts_code", group_keys=False)
        chip_panel_df["chip_peak_mean_10"] = g["chip_peak_price"].transform(lambda s: s.rolling(10, min_periods=10).mean())
        chip_panel_df["chip_peak_std_10"] = g["chip_peak_price"].transform(lambda s: s.rolling(10, min_periods=10).std())
        chip_panel_df["chip_peak_cv_10"] = chip_panel_df["chip_peak_std_10"] / chip_panel_df["chip_peak_mean_10"]

        chip_latest_df = chip_panel_df[
            chip_panel_df["trade_date"] == pd.to_datetime(trade_date, format="%Y%m%d")
        ][["ts_code", "trade_date", "winner_rate", "chip_peak_price", "chip_peak_cv_10", "cost_band_90"]].copy()
        chip_latest_df["trade_date"] = chip_latest_df["trade_date"].dt.strftime("%Y%m%d")

    out = latest_df.merge(daily_basic_df, on=["ts_code", "trade_date"], how="left")
    out = out.merge(chip_latest_df, on=["ts_code", "trade_date"], how="left")
    out = out.merge(moneyflow_df, on=["ts_code", "trade_date"], how="left")
    out = out.merge(top_inst_df, on=["ts_code", "trade_date"], how="left")
    out = out.merge(stock_basic_df, on="ts_code", how="left")
    out = out.merge(industry_slope_df, on=["ts_code", "trade_date"], how="left")
    out = out.merge(north_context_df, on="trade_date", how="left")
    out = out.merge(forecast_df, on="ts_code", how="left")

    out = _apply_policy_theme_hit(out)
    out["policy_catalyst_active"] = _is_policy_catalyst_active(trade_date)
    out["calendar_bias"] = _infer_calendar_bias(trade_date)

    # 判定是否处于业绩披露窗口：基于 forecast.ann_date 所在月份而非交易日月份
    _disclosure_months = {1, 4, 7, 10}
    if "ann_date" in out.columns:
        out["earnings_disclosure_month"] = out["ann_date"].apply(
            lambda d: int(str(d)[4:6]) in _disclosure_months if pd.notna(d) and len(str(d)) >= 6 else False
        ).astype(int)
    else:
        out["earnings_disclosure_month"] = int(datetime.strptime(trade_date, "%Y%m%d").month in _disclosure_months)

    out["earnings_negative_flag"] = out["forecast_type"].isin(["预减", "首亏", "续亏", "略减"]).astype(int)
    out["earnings_preincrease_flag"] = out["forecast_type"].eq("预增").astype(int)

    keep_cols = [
        "ts_code", "trade_date", "name", "industry",
        "close", "close_adj", "pct_chg", "pct_chg_1d", "pct_chg_20d", "amount", "amount_ma20_prev", "vol",
        "turnover_rate", "pe_ttm", "circ_mv",
        "ps_ttm", "peg",
        "ma20", "ma55", "ma120", "ma250", "ma_dispersion",
        "vol_ratio_5", "g_point_strength", "close_to_ma250",
        "had_limit_10d", "l_shape_shrink_ratio", "amplitude_5d", "vol_at_llv_20",
        "cross3_close_break", "cross3_trend_strength", "cross3_vol_ratio",
        "guide_shadow_pct", "guide_body_pct", "reversal_flag",
        "false_break_depth", "lower_shadow_pct",
        "golden_buy_price", "distance_to_618",
        "atr_14", "atr_14_trend_5",
        "rps_20", "rps_60", "winner_rate", "chip_peak_price", "chip_peak_cv_10", "cost_band_90", "net_mf_amount",
        "inst_count", "inst_net_buy",
        "north_money", "north_consecutive_days", "north_sum_3d", "industry_rps_slope_3d",
        "policy_theme_hit", "policy_catalyst_active", "calendar_bias",
        "forecast_type", "earnings_disclosure_month", "earnings_negative_flag", "earnings_preincrease_flag",
    ]
    out = out[keep_cols]
    out = out.drop_duplicates(subset=["ts_code", "trade_date"])
    return out


def get_latest_trade_date_from_db() -> Optional[str]:
    engine = get_engine("db1")
    sql = text("SELECT MAX(trade_date) AS trade_date FROM stock_daily")
    with engine.connect() as conn:
        row = conn.execute(sql).fetchone()
    if not row or not row[0]:
        return None
    return str(row[0])
