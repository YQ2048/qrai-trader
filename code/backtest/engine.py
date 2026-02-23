import json
from datetime import datetime, timedelta
from typing import Iterable, List, Optional

import numpy as np
import pandas as pd
from sqlalchemy import text

from code.analytics.factors import load_factor_snapshot, _load_price_panel, _compute_price_factors, \
    _load_daily_basic, _load_chip_panel, _load_moneyflow, _load_stock_basic, \
    _load_top_inst_agg, _load_moneyflow_hsgt_panel, _load_forecast_latest, \
    _is_policy_theme_industry, _is_policy_catalyst_active, _infer_calendar_bias
from code.analytics.screener import detect_daily_candidates
from code.core.db_manager import get_engine
from code.core.logger import get_logger

logger = get_logger(__name__)


DEFAULT_BACKTEST_STRATEGIES = [
    "S11", "S12", "S13",
    "S21", "S22", "S23", "S24", "S25", "S26",
    "S31", "S32", "S33", "S34",
]


def make_signal_uid(signal_date: str, ts_code: str, strategy_id: str) -> str:
    return f"{signal_date}|{ts_code}|{strategy_id}"


def ensure_backtest_signals_table(engine) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS backtest_signals (
        id INT AUTO_INCREMENT PRIMARY KEY,
        strategy_id VARCHAR(10) NOT NULL,
        ts_code VARCHAR(10) NOT NULL,
        signal_date VARCHAR(8) NOT NULL,
        signal_uid VARCHAR(64),
        strategy_signal_id INT,
        score_raw DOUBLE,
        close_on_signal DOUBLE,
        return_t1 DOUBLE,
        return_t3 DOUBLE,
        return_t5 DOUBLE,
        return_t10 DOUBLE,
        return_t20 DOUBLE,
        max_gain_20 DOUBLE,
        max_loss_20 DOUBLE,
        skyline_hit TINYINT DEFAULT 0,
        skyline_hit_day INT,
        outcome_label VARCHAR(20),
        data_gap TINYINT DEFAULT 0,
        data_gap_reason VARCHAR(255),
        param_snapshot JSON,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        KEY idx_strategy_date (strategy_id, signal_date),
        KEY idx_code_date (ts_code, signal_date),
        KEY idx_signal_uid (signal_uid),
        KEY idx_strategy_signal_id (strategy_signal_id)
    )
    """
    with engine.connect() as conn:
        conn.execute(text(ddl))
        columns = conn.execute(text("SHOW COLUMNS FROM backtest_signals")).fetchall()
        col_names = {r[0] for r in columns}
        if "signal_uid" not in col_names:
            conn.execute(text("ALTER TABLE backtest_signals ADD COLUMN signal_uid VARCHAR(64)"))
        if "strategy_signal_id" not in col_names:
            conn.execute(text("ALTER TABLE backtest_signals ADD COLUMN strategy_signal_id INT"))
        if "data_gap" not in col_names:
            conn.execute(text("ALTER TABLE backtest_signals ADD COLUMN data_gap TINYINT DEFAULT 0"))
        if "data_gap_reason" not in col_names:
            conn.execute(text("ALTER TABLE backtest_signals ADD COLUMN data_gap_reason VARCHAR(255)"))
        if "param_snapshot" not in col_names:
            conn.execute(text("ALTER TABLE backtest_signals ADD COLUMN param_snapshot JSON"))
        if "max_gain_20" not in col_names:
            conn.execute(text("ALTER TABLE backtest_signals ADD COLUMN max_gain_20 DOUBLE"))
        if "max_loss_20" not in col_names:
            conn.execute(text("ALTER TABLE backtest_signals ADD COLUMN max_loss_20 DOUBLE"))
        if "skyline_hit" not in col_names:
            conn.execute(text("ALTER TABLE backtest_signals ADD COLUMN skyline_hit TINYINT DEFAULT 0"))
        if "skyline_hit_day" not in col_names:
            conn.execute(text("ALTER TABLE backtest_signals ADD COLUMN skyline_hit_day INT"))
        if "outcome_label" not in col_names:
            conn.execute(text("ALTER TABLE backtest_signals ADD COLUMN outcome_label VARCHAR(20)"))
        conn.commit()


def list_trade_dates(start_date: str, end_date: str) -> List[str]:
    engine = get_engine("db1")
    sql = text(
        """
        SELECT cal_date
        FROM trade_cal
        WHERE is_open = 1
          AND cal_date BETWEEN :start_date AND :end_date
        ORDER BY cal_date
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"start_date": start_date, "end_date": end_date}).fetchall()
    return [r[0] for r in rows]


def build_signals_from_factor_history(
    factor_history_df: pd.DataFrame,
    strategy_ids: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    if factor_history_df is None or factor_history_df.empty:
        return pd.DataFrame(columns=["ts_code", "signal_date", "strategy_id", "score_raw"])

    target_ids = set(strategy_ids or DEFAULT_BACKTEST_STRATEGIES)
    rows = []

    for trade_date, snap in factor_history_df.groupby("trade_date"):
        candidates = detect_daily_candidates(snap)
        for candidate in candidates:
            for signal in candidate.signals:
                if signal.strategy_id not in target_ids:
                    continue
                rows.append(
                    {
                        "ts_code": signal.ts_code,
                        "signal_date": signal.trade_date,
                        "strategy_id": signal.strategy_id,
                        "score_raw": float(signal.score_raw),
                        "industry": candidate.industry,
                    }
                )

    if not rows:
        return pd.DataFrame(columns=["ts_code", "signal_date", "strategy_id", "score_raw", "industry"])

    out = pd.DataFrame(rows).drop_duplicates(subset=["ts_code", "signal_date", "strategy_id"])
    out = out.sort_values(["signal_date", "ts_code", "strategy_id"]).reset_index(drop=True)
    return out


def attach_forward_returns(signals_df: pd.DataFrame, price_df: pd.DataFrame) -> pd.DataFrame:
    if signals_df is None or signals_df.empty:
        return pd.DataFrame(
            columns=[
                "ts_code", "signal_date", "strategy_id", "score_raw", "industry",
                "return_t1", "return_t3", "return_t5", "return_t10", "return_t20",
            ]
        )

    if price_df is None or price_df.empty:
        out = signals_df.copy()
        out["return_t1"] = None
        out["return_t3"] = None
        out["return_t5"] = None
        out["return_t10"] = None
        out["return_t20"] = None
        return out

    price = price_df.copy()
    price["trade_date"] = pd.to_datetime(price["trade_date"], format="%Y%m%d")
    price = price.sort_values(["ts_code", "trade_date"])

    g = price.groupby("ts_code", group_keys=False)
    price["ret_t1"] = g["close_adj"].transform(lambda s: s.shift(-1) / s - 1)
    price["ret_t3"] = g["close_adj"].transform(lambda s: s.shift(-3) / s - 1)
    price["ret_t5"] = g["close_adj"].transform(lambda s: s.shift(-5) / s - 1)
    price["ret_t10"] = g["close_adj"].transform(lambda s: s.shift(-10) / s - 1)
    price["ret_t20"] = g["close_adj"].transform(lambda s: s.shift(-20) / s - 1)

    # 4.3 增强字段：信号日后 20 日内最大涨幅 / 最大跌幅
    def _max_gain_20(s):
        """向前 20 个交易日内的最大涨幅（基于信号日收盘价）"""
        result = pd.Series(np.nan, index=s.index)
        values = s.values
        for i in range(len(values)):
            window = values[i+1:i+21]
            if len(window) > 0:
                result.iloc[i] = (max(window) / values[i]) - 1 if values[i] > 0 else np.nan
            else:
                result.iloc[i] = np.nan
        return result

    def _max_loss_20(s):
        """向前 20 个交易日内的最大跌幅（基于信号日收盘价）"""
        result = pd.Series(np.nan, index=s.index)
        values = s.values
        for i in range(len(values)):
            window = values[i+1:i+21]
            if len(window) > 0:
                result.iloc[i] = (min(window) / values[i]) - 1 if values[i] > 0 else np.nan
            else:
                result.iloc[i] = np.nan
        return result

    price["max_gain_20"] = g["close_adj"].transform(_max_gain_20)
    price["max_loss_20"] = g["close_adj"].transform(_max_loss_20)

    # 天际线止损：信号日向前第 3 根 K 线的最低点
    if "low_adj" in price.columns:
        price["skyline_level"] = g["low_adj"].transform(lambda s: s.shift(1).rolling(3, min_periods=3).min())
        price["skyline_hit"] = (price["low_adj"].shift(-1) < price["skyline_level"]).astype(int) if "low_adj" in price.columns else 0
        # 找首次触发天际线的天数
        def _skyline_hit_day(group):
            result = pd.Series(np.nan, index=group.index)
            low_vals = group["low_adj"].values if "low_adj" in group.columns else None
            sky_vals = group["skyline_level"].values if "skyline_level" in group.columns else None
            if low_vals is None or sky_vals is None:
                return result
            for i in range(len(group)):
                for d in range(1, 21):
                    if i + d >= len(group):
                        break
                    if not np.isnan(sky_vals[i]) and low_vals[i + d] < sky_vals[i]:
                        result.iloc[i] = d
                        break
            return result
        price["skyline_hit_day"] = g.apply(_skyline_hit_day).reset_index(drop=True)
    else:
        price["skyline_level"] = np.nan
        price["skyline_hit"] = 0
        price["skyline_hit_day"] = np.nan

    price_map = price[["ts_code", "trade_date", "close_adj", "ret_t1", "ret_t3", "ret_t5", "ret_t10", "ret_t20",
                        "max_gain_20", "max_loss_20", "skyline_hit", "skyline_hit_day"]]

    out = signals_df.copy()
    out["signal_date"] = pd.to_datetime(out["signal_date"], format="%Y%m%d")
    out = out.merge(
        price_map,
        left_on=["ts_code", "signal_date"],
        right_on=["ts_code", "trade_date"],
        how="left",
    )
    out = out.drop(columns=["trade_date"])
    out = out.rename(
        columns={
            "close_adj": "close_on_signal",
            "ret_t1": "return_t1",
            "ret_t3": "return_t3",
            "ret_t5": "return_t5",
            "ret_t10": "return_t10",
            "ret_t20": "return_t20",
        }
    )

    # 8.2 向量化 data_gap 计算（替代逐行 apply）
    ret_cols = ["return_t1", "return_t3", "return_t5", "return_t10", "return_t20"]
    gap_parts = []
    labels = ["T+1", "T+3", "T+5", "T+10", "T+20"]
    for col, label in zip(ret_cols, labels):
        if col in out.columns:
            gap_parts.append(out[col].isna().map({True: label, False: ""}))
    if gap_parts:
        out["data_gap_reason"] = pd.concat(gap_parts, axis=1).apply(
            lambda row: ",".join([v for v in row if v]), axis=1
        )
    else:
        out["data_gap_reason"] = ""
    out["data_gap"] = (out["data_gap_reason"] != "").astype(int)

    # 4.3 outcome_label：基于 return_t20 分类
    def _outcome_label(r):
        if pd.isna(r):
            return "NO_DATA"
        if r > 0.10:
            return "BIG_WIN"
        if r > 0.03:
            return "SMALL_WIN"
        if r >= -0.03:
            return "FLAT"
        if r >= -0.10:
            return "SMALL_LOSS"
        return "BIG_LOSS"
    out["outcome_label"] = out["return_t20"].apply(_outcome_label)

    out["signal_date"] = out["signal_date"].dt.strftime("%Y%m%d")
    return out


def _bulk_load_factor_history(start_date: str, end_date: str, lookback_days: int = 320) -> pd.DataFrame:
    """一次性批量加载全区间因子数据，在内存中按交易日切片。
    替代逐日调用 load_factor_snapshot 的方式，DB 查询次数从 N*8+ 降到 8 次。
    回测时包含已退市股票以避免生存偏差。
    """
    from code.core.config import CALENDAR_BIAS_MAP, POLICY_CATALYST_DATES, POLICY_CATALYST_WINDOW_DAYS

    # 回溯起始日：需要为 start_date 提供 lookback_days 的历史数据
    lookback_start = (datetime.strptime(start_date, "%Y%m%d") - timedelta(days=lookback_days)).strftime("%Y%m%d")

    # 1. 一次性加载全区间价格面板并计算因子
    price_df = _load_price_panel(start_date=lookback_start, end_date=end_date)
    if price_df.empty:
        return pd.DataFrame()

    factor_df = _compute_price_factors(price_df)
    if factor_df.empty:
        return pd.DataFrame()

    # 2. 批量加载辅助数据（只加载一次）
    daily_basic_dates = factor_df[factor_df["trade_date"] >= pd.to_datetime(start_date, format="%Y%m%d")]
    trade_dates_list = sorted(daily_basic_dates["trade_date"].dt.strftime("%Y%m%d").unique())

    # 4.2 生存偏差处理：加载全量 stock_basic（含已退市），不过滤 list_status
    stock_basic_df = _load_stock_basic()
    chip_panel_df = _load_chip_panel(start_date=lookback_start, end_date=end_date)
    hsgt_panel_df = _load_moneyflow_hsgt_panel(start_date=lookback_start, end_date=end_date)
    forecast_df = _load_forecast_latest(end_date)

    # 3. 预处理筹码面板
    chip_cv_df = pd.DataFrame()
    if not chip_panel_df.empty:
        chip_panel_df = chip_panel_df.copy()
        chip_panel_df["trade_date"] = pd.to_datetime(chip_panel_df["trade_date"], format="%Y%m%d")
        chip_panel_df = chip_panel_df.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)
        gc = chip_panel_df.groupby("ts_code", group_keys=False)
        chip_panel_df["chip_peak_mean_10"] = gc["chip_peak_price"].transform(lambda s: s.rolling(10, min_periods=10).mean())
        chip_panel_df["chip_peak_std_10"] = gc["chip_peak_price"].transform(lambda s: s.rolling(10, min_periods=10).std())
        chip_panel_df["chip_peak_cv_10"] = chip_panel_df["chip_peak_std_10"] / chip_panel_df["chip_peak_mean_10"]
        chip_cv_df = chip_panel_df[["ts_code", "trade_date", "winner_rate", "chip_peak_price", "chip_peak_cv_10"]]
        if "cost_band_90" in chip_panel_df.columns:
            chip_cv_df = chip_panel_df[["ts_code", "trade_date", "winner_rate", "chip_peak_price", "chip_peak_cv_10", "cost_band_90"]]

    # 4. 预处理北向资金
    north_df = pd.DataFrame()
    if not hsgt_panel_df.empty:
        hsgt = hsgt_panel_df.copy()
        hsgt["trade_date"] = pd.to_datetime(hsgt["trade_date"], format="%Y%m%d")
        hsgt = hsgt.sort_values("trade_date").reset_index(drop=True)
        inflow_flag = (hsgt["north_money"] > 0).astype(int)
        hsgt["north_consecutive_days"] = inflow_flag * (
            inflow_flag.groupby((inflow_flag == 0).cumsum()).cumcount() + 1
        )
        hsgt["north_sum_3d"] = hsgt["north_money"].rolling(3, min_periods=3).sum()
        north_df = hsgt[["trade_date", "north_money", "north_consecutive_days", "north_sum_3d"]]

    # 5. 逐日切片组装完整因子快照
    all_snapshots = []
    for td_str in trade_dates_list:
        td = pd.to_datetime(td_str, format="%Y%m%d")
        latest_df = factor_df[factor_df["trade_date"] == td].copy()
        if latest_df.empty:
            continue

        latest_df["trade_date"] = latest_df["trade_date"].dt.strftime("%Y%m%d")

        # daily_basic 逐日加载（轻量单日查询）
        daily_basic_df = _load_daily_basic(td_str)
        moneyflow_df = _load_moneyflow(td_str)
        top_inst_df = _load_top_inst_agg(td_str)

        out = latest_df.merge(daily_basic_df, on=["ts_code", "trade_date"], how="left")

        # 筹码
        if not chip_cv_df.empty:
            chip_day = chip_cv_df[chip_cv_df["trade_date"] == td].copy()
            chip_day["trade_date"] = chip_day["trade_date"].dt.strftime("%Y%m%d")
            out = out.merge(chip_day, on=["ts_code", "trade_date"], how="left")

        out = out.merge(moneyflow_df, on=["ts_code", "trade_date"], how="left")
        out = out.merge(top_inst_df, on=["ts_code", "trade_date"], how="left")
        out = out.merge(stock_basic_df, on="ts_code", how="left")

        # 行业RPS slope
        if not factor_df.empty and not stock_basic_df.empty:
            panel_ind = factor_df.merge(stock_basic_df[["ts_code", "industry"]], on="ts_code", how="left")
            ind_daily = (
                panel_ind.dropna(subset=["industry"])
                .groupby(["trade_date", "industry"], as_index=False)["rps_20"].mean()
                .sort_values(["industry", "trade_date"])
            )
            ind_daily["industry_rps_slope_3d"] = (
                ind_daily.groupby("industry")["rps_20"].transform(lambda s: (s - s.shift(2)) / 2)
            )
            latest_ind = ind_daily[ind_daily["trade_date"] == td][["industry", "industry_rps_slope_3d"]]
            if not latest_ind.empty:
                ind_slope = out[["ts_code"]].merge(
                    stock_basic_df[["ts_code", "industry"]], on="ts_code", how="left"
                ).merge(latest_ind, on="industry", how="left")[["ts_code", "industry_rps_slope_3d"]]
                out = out.merge(ind_slope, on="ts_code", how="left")

        # 北向
        if not north_df.empty:
            north_day = north_df[north_df["trade_date"] == td].copy()
            north_day["trade_date"] = north_day["trade_date"].dt.strftime("%Y%m%d")
            out = out.merge(north_day, on="trade_date", how="left")

        # 业绩预告
        out = out.merge(forecast_df, on="ts_code", how="left")

        # 宏观因子
        out["policy_theme_hit"] = out["industry"].fillna("").apply(_is_policy_theme_industry)
        out["policy_catalyst_active"] = _is_policy_catalyst_active(td_str)
        out["calendar_bias"] = _infer_calendar_bias(td_str)

        _disclosure_months = {1, 4, 7, 10}
        if "ann_date" in out.columns:
            out["earnings_disclosure_month"] = out["ann_date"].apply(
                lambda d: int(str(d)[4:6]) in _disclosure_months if pd.notna(d) and len(str(d)) >= 6 else False
            ).astype(int)
        else:
            out["earnings_disclosure_month"] = int(datetime.strptime(td_str, "%Y%m%d").month in _disclosure_months)

        out["earnings_negative_flag"] = out.get("forecast_type", pd.Series(dtype=str)).isin(["预减", "首亏", "续亏", "略减"]).astype(int)
        out["earnings_preincrease_flag"] = out.get("forecast_type", pd.Series(dtype=str)).eq("预增").astype(int)

        all_snapshots.append(out)

    if not all_snapshots:
        return pd.DataFrame()

    return pd.concat(all_snapshots, ignore_index=True)


def run_min_backtest(start_date: str, end_date: str, strategy_ids: Optional[Iterable[str]] = None) -> pd.DataFrame:
    trade_dates = list_trade_dates(start_date, end_date)
    if not trade_dates:
        return pd.DataFrame()

    # 批量加载：一次加载全区间价格面板，内存中按交易日切片
    factor_history_df = _bulk_load_factor_history(start_date, end_date, lookback_days=320)
    if factor_history_df is None or factor_history_df.empty:
        return pd.DataFrame()

    signals = build_signals_from_factor_history(factor_history_df, strategy_ids=strategy_ids)

    price_cols = ["ts_code", "trade_date", "close_adj"]
    if "low_adj" in factor_history_df.columns:
        price_cols.append("low_adj")
    price_df = factor_history_df[price_cols].drop_duplicates(subset=["ts_code", "trade_date"])
    result = attach_forward_returns(signals, price_df)
    result = attach_market_context(result, start_date=start_date, end_date=end_date)
    return result


def _load_market_pct_chg(start_date: str, end_date: str) -> pd.DataFrame:
    engine = get_engine("db1")
    sql = text(
        """
        SELECT trade_date AS signal_date, pct_chg AS market_pct_chg
        FROM index_daily
        WHERE ts_code = '000001.SH'
          AND trade_date BETWEEN :start_date AND :end_date
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"start_date": start_date, "end_date": end_date})


def attach_market_context(result_df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    if result_df is None or result_df.empty:
        return result_df

    market_df = _load_market_pct_chg(start_date=start_date, end_date=end_date)
    out = result_df.copy()
    if market_df.empty:
        out["market_pct_chg"] = None
        out["market_state"] = "UNKNOWN"
        return out

    out = out.merge(market_df, on="signal_date", how="left")

    def _state(x):
        if pd.isna(x):
            return "UNKNOWN"
        if x >= 1.0:
            return "UP"
        if x <= -1.0:
            return "DOWN"
        return "RANGE"

    out["market_state"] = out["market_pct_chg"].apply(_state)
    return out


def persist_backtest_signals(result_df: pd.DataFrame, param_snapshot: Optional[dict] = None) -> int:
    if result_df is None or result_df.empty:
        return 0

    df = result_df.copy()
    df["signal_uid"] = df.apply(
        lambda r: make_signal_uid(str(r.get("signal_date")), str(r.get("ts_code")), str(r.get("strategy_id"))),
        axis=1,
    )
    df["strategy_signal_id"] = None
    df["param_snapshot"] = json.dumps(param_snapshot or {}, ensure_ascii=False)

    keep_cols = [
        "strategy_id",
        "ts_code",
        "signal_date",
        "signal_uid",
        "strategy_signal_id",
        "score_raw",
        "close_on_signal",
        "return_t1",
        "return_t3",
        "return_t5",
        "return_t10",
        "return_t20",
        "max_gain_20",
        "max_loss_20",
        "skyline_hit",
        "skyline_hit_day",
        "outcome_label",
        "data_gap",
        "data_gap_reason",
        "param_snapshot",
    ]
    for col in keep_cols:
        if col not in df.columns:
            df[col] = None
    df = df[keep_cols]

    engine = get_engine("db3")
    ensure_backtest_signals_table(engine)
    with engine.connect() as conn:
        uids = df["signal_uid"].dropna().unique().tolist()
        if uids:
            uid_to_id = {}
            chunk_size = 1000
            for i in range(0, len(uids), chunk_size):
                chunk = uids[i:i + chunk_size]
                placeholders = ",".join([f":u{j}" for j in range(len(chunk))])
                params = {f"u{j}": u for j, u in enumerate(chunk)}
                rows = conn.execute(
                    text(
                        f"SELECT id, signal_uid FROM strategy_signals WHERE signal_uid IN ({placeholders})"
                    ),
                    params,
                ).fetchall()
                uid_to_id.update({r[1]: r[0] for r in rows})
            if uid_to_id:
                df["strategy_signal_id"] = df["signal_uid"].map(uid_to_id)

        dates = df["signal_date"].dropna().unique().tolist()
        if dates:
            for i in range(0, len(dates), chunk_size):
                chunk = dates[i:i + chunk_size]
                placeholders = ",".join([f":d{j}" for j in range(len(chunk))])
                params = {f"d{j}": d for j, d in enumerate(chunk)}
                conn.execute(text(f"DELETE FROM backtest_signals WHERE signal_date IN ({placeholders})"), params)
            conn.commit()

    df.to_sql("backtest_signals", engine, if_exists="append", index=False, method="multi", chunksize=200)
    return len(df)


def summarize_backtest_by_strategy(result_df: pd.DataFrame) -> pd.DataFrame:
    if result_df is None or result_df.empty:
        return pd.DataFrame(
            columns=[
                "strategy_id",
                "signals_count",
                "avg_return_t1",
                "avg_return_t3",
                "avg_return_t5",
                "avg_return_t10",
                "avg_return_t20",
                "win_rate_t1",
                "win_rate_t5",
                "win_rate_t20",
                "p25_return_t20",
                "p50_return_t20",
                "p75_return_t20",
                "best_return_t20",
                "worst_return_t20",
                "sharpe_approx_t20",
                "calmar_approx_t20",
            ]
        )

    df = result_df.copy()

    def _win_rate(series: pd.Series) -> float:
        valid = series.dropna()
        if len(valid) == 0:
            return 0.0
        return float((valid > 0).mean())

    def _sharpe_approx(series: pd.Series) -> float:
        valid = series.dropna()
        if len(valid) < 2:
            return 0.0
        std = float(valid.std(ddof=1))
        if std == 0:
            return 0.0
        annual_factor = (252 / 20) ** 0.5
        return float(valid.mean() / std * annual_factor)

    def _calmar_approx(group: pd.DataFrame) -> float:
        """近似 Calmar 比率。注意：信号之间不一定时序连续（可能同日多个），
        此处将各信号 T+20 收益按信号日期排序后连乘作为伪权益曲线，
        结果仅供横向策略对比参考，不代表真实组合回撤。"""
        if group.empty:
            return 0.0
        seq = group[["signal_date", "return_t20"]].dropna().copy()
        if seq.empty:
            return 0.0
        seq = seq.sort_values("signal_date")
        equity = (1 + seq["return_t20"]).cumprod()
        peak = equity.cummax()
        drawdown = (equity / peak) - 1
        max_drawdown = float(drawdown.min())
        annual_ret = float(seq["return_t20"].mean() * (252 / 20))
        if max_drawdown >= 0:
            return 0.0
        return float(annual_ret / abs(max_drawdown))

    grouped = df.groupby("strategy_id", as_index=False)
    summary = grouped.agg(
        signals_count=("ts_code", "count"),
        avg_return_t1=("return_t1", "mean"),
        avg_return_t3=("return_t3", "mean"),
        avg_return_t5=("return_t5", "mean"),
        avg_return_t10=("return_t10", "mean"),
        avg_return_t20=("return_t20", "mean"),
        best_return_t20=("return_t20", "max"),
        worst_return_t20=("return_t20", "min"),
    )

    win_t1 = df.groupby("strategy_id")["return_t1"].apply(_win_rate).reset_index(name="win_rate_t1")
    win_t5 = df.groupby("strategy_id")["return_t5"].apply(_win_rate).reset_index(name="win_rate_t5")
    win_t20 = df.groupby("strategy_id")["return_t20"].apply(_win_rate).reset_index(name="win_rate_t20")

    q25_t20 = df.groupby("strategy_id")["return_t20"].quantile(0.25).reset_index(name="p25_return_t20")
    q50_t20 = df.groupby("strategy_id")["return_t20"].quantile(0.50).reset_index(name="p50_return_t20")
    q75_t20 = df.groupby("strategy_id")["return_t20"].quantile(0.75).reset_index(name="p75_return_t20")
    sharpe_t20 = df.groupby("strategy_id")["return_t20"].apply(_sharpe_approx).reset_index(name="sharpe_approx_t20")
    calmar_t20 = df.groupby("strategy_id").apply(_calmar_approx).reset_index(name="calmar_approx_t20")

    summary = summary.merge(win_t1, on="strategy_id", how="left")
    summary = summary.merge(win_t5, on="strategy_id", how="left")
    summary = summary.merge(win_t20, on="strategy_id", how="left")
    summary = summary.merge(q25_t20, on="strategy_id", how="left")
    summary = summary.merge(q50_t20, on="strategy_id", how="left")
    summary = summary.merge(q75_t20, on="strategy_id", how="left")
    summary = summary.merge(sharpe_t20, on="strategy_id", how="left")
    summary = summary.merge(calmar_t20, on="strategy_id", how="left")
    summary = summary.sort_values("signals_count", ascending=False).reset_index(drop=True)
    return summary


def summarize_backtest_by_market_and_strategy(result_df: pd.DataFrame) -> pd.DataFrame:
    if result_df is None or result_df.empty:
        return pd.DataFrame(columns=["strategy_id", "market_state", "signals_count", "avg_return_t20", "win_rate_t20"])

    df = result_df.copy()
    if "market_state" not in df.columns:
        df["market_state"] = "UNKNOWN"

    grouped = df.groupby(["strategy_id", "market_state"], as_index=False)
    summary = grouped.agg(
        signals_count=("ts_code", "count"),
        avg_return_t20=("return_t20", "mean"),
    )
    win = (
        df.groupby(["strategy_id", "market_state"])["return_t20"]
        .apply(lambda s: float((s.dropna() > 0).mean()) if len(s.dropna()) else 0.0)
        .reset_index(name="win_rate_t20")
    )
    summary = summary.merge(win, on=["strategy_id", "market_state"], how="left")
    return summary.sort_values(["strategy_id", "market_state"]).reset_index(drop=True)


def summarize_backtest_by_industry_and_strategy(result_df: pd.DataFrame) -> pd.DataFrame:
    if result_df is None or result_df.empty:
        return pd.DataFrame(columns=["strategy_id", "industry", "signals_count", "avg_return_t20", "win_rate_t20"])

    df = result_df.copy()
    if "industry" not in df.columns:
        df["industry"] = "UNKNOWN"
    df["industry"] = df["industry"].fillna("UNKNOWN")

    grouped = df.groupby(["strategy_id", "industry"], as_index=False)
    summary = grouped.agg(
        signals_count=("ts_code", "count"),
        avg_return_t20=("return_t20", "mean"),
    )
    win = (
        df.groupby(["strategy_id", "industry"])["return_t20"]
        .apply(lambda s: float((s.dropna() > 0).mean()) if len(s.dropna()) else 0.0)
        .reset_index(name="win_rate_t20")
    )
    summary = summary.merge(win, on=["strategy_id", "industry"], how="left")
    return summary.sort_values(["strategy_id", "signals_count"], ascending=[True, False]).reset_index(drop=True)


def run_min_backtest_s21_s31(start_date: str, end_date: str) -> pd.DataFrame:
    return run_min_backtest(start_date=start_date, end_date=end_date, strategy_ids=["S21", "S31"])
