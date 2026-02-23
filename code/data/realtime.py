from __future__ import annotations

from datetime import datetime
from typing import Dict, Optional

import pandas as pd
from sqlalchemy import text

from code.core.db_manager import get_engine
from code.core.logger import get_logger

logger = get_logger(__name__)


def classify_yesterday_context(prev_pct_chg: float, prev_vol_ratio: float) -> str:
    if prev_pct_chg >= 9.5:
        return "strong_limit"
    if -3.0 <= prev_pct_chg <= 1.5 and prev_vol_ratio <= 0.8:
        return "shrink_pullback"
    if prev_pct_chg <= -4.0 and prev_vol_ratio >= 1.2:
        return "bearish"
    return "neutral"


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return default
        return float(value)
    except Exception:
        return default


def _load_daily_window(ts_code: str, trade_date: Optional[str], lookback: int = 30) -> pd.DataFrame:
    engine = get_engine("db1")
    date_filter = "AND trade_date <= :trade_date" if trade_date else ""
    sql = text(
        f"""
        SELECT ts_code, trade_date, open, high, low, close, pct_chg, amount, vol
        FROM stock_daily
        WHERE ts_code = :ts_code
        {date_filter}
        ORDER BY trade_date DESC
        LIMIT :lookback
        """
    )

    params = {"ts_code": ts_code, "lookback": lookback}
    if trade_date:
        params["trade_date"] = trade_date

    try:
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params=params)
    except Exception as e:
        logger.warning("加载 %s 日线窗口失败: %s", ts_code, e)
        df = pd.DataFrame(columns=["ts_code", "trade_date", "open", "high", "low", "close", "pct_chg", "amount", "vol"])

    if df.empty:
        return df

    df = df.sort_values("trade_date").reset_index(drop=True)
    return df


def _build_market_features(df: pd.DataFrame) -> Dict[str, float | str]:
    if df.empty or len(df) < 2:
        return {
            "avg_amount_5d": 0.0,
            "avg_amp_20d": 0.08,
            "yesterday_context": "neutral",
            "last_close": 0.0,
        }

    latest = df.iloc[-1]
    prev = df.iloc[-2]

    amount_ma5 = _safe_float(df["amount"].tail(5).mean(), 0.0)
    amp_20 = (((df["high"] - df["low"]) / df["close"].replace(0, pd.NA)).tail(20)).mean()
    amp_20 = _safe_float(amp_20, 0.08)

    prev_amount = _safe_float(prev.get("amount"), 0.0)
    prev_amount_ma5 = _safe_float(df.iloc[:-1]["amount"].tail(5).mean(), prev_amount)
    prev_vol_ratio = (prev_amount / prev_amount_ma5) if prev_amount_ma5 > 0 else 1.0

    yesterday_context = classify_yesterday_context(
        prev_pct_chg=_safe_float(prev.get("pct_chg"), 0.0),
        prev_vol_ratio=_safe_float(prev_vol_ratio, 1.0),
    )

    return {
        "avg_amount_5d": amount_ma5,
        "avg_amp_20d": amp_20,
        "yesterday_context": yesterday_context,
        "last_close": _safe_float(latest.get("close"), 0.0),
    }


def build_realtime_snapshot(
    ts_code: str,
    trade_date: Optional[str] = None,
    open_gap_pct: Optional[float] = None,
    auction_amount: Optional[float] = None,
    price: Optional[float] = None,
    vwap: Optional[float] = None,
) -> Dict[str, float | str]:
    df = _load_daily_window(ts_code=ts_code, trade_date=trade_date, lookback=40)
    features = _build_market_features(df)

    last_close = _safe_float(features.get("last_close"), 0.0)
    implied_price = last_close * (1 + (_safe_float(open_gap_pct, 0.0) / 100.0)) if last_close > 0 else 0.0

    snapshot = {
        "open_gap_pct": _safe_float(open_gap_pct, 0.0),
        "auction_amount": _safe_float(auction_amount, _safe_float(features.get("avg_amount_5d"), 0.0) * 0.10),
        "avg_amount_5d": _safe_float(features.get("avg_amount_5d"), 0.0),
        "yesterday_context": str(features.get("yesterday_context", "neutral")),
        "price": _safe_float(price, implied_price),
        "vwap": _safe_float(vwap, implied_price),
        "avg_amp_20d": _safe_float(features.get("avg_amp_20d"), 0.08),
        "ts_code": ts_code,
        "trade_date": trade_date or datetime.now().strftime("%Y%m%d"),
    }
    return snapshot
