from typing import Dict, List, Tuple
from datetime import datetime

import numpy as np
import pandas as pd

from code.core.config import CALENDAR_BIAS_MAP, POLICY_THEME_KEYWORDS, QUANT_THRESHOLDS
from code.core.logger import get_logger
from code.strategy.risk_rules import check_earnings_meltdown, tag_basic_risks
from code.strategy.signals import DailyCandidate, StrategySignal


def _fval(v, default: float = 0.0) -> float:
    """安全转换为 float，兼容 None / np.nan / pd.NA，避免 bool(pd.NA) 歧义。"""
    try:
        f = float(v)
        return default if np.isnan(f) else f
    except (TypeError, ValueError):
        return default

logger = get_logger(__name__)


def _piecewise_score(x: float, anchors: List[Tuple[float, float]]) -> float:
    if x is None or np.isnan(x):
        return 0.0
    if x <= anchors[0][0]:
        return anchors[0][1]
    if x >= anchors[-1][0]:
        return anchors[-1][1]

    for (x1, y1), (x2, y2) in zip(anchors[:-1], anchors[1:]):
        if x1 <= x <= x2:
            if x2 == x1:
                return float(y2)
            ratio = (x - x1) / (x2 - x1)
            return float(y1 + ratio * (y2 - y1))
    return 0.0


def _hard_filter(df: pd.DataFrame) -> pd.DataFrame:
    pe_ttm_max = QUANT_THRESHOLDS["pe_ttm_max"]            # 宽松上限（默认 100）
    pe_ttm_strict = QUANT_THRESHOLDS["pe_ttm_strict"]      # 严格上限（60）
    theme_split = QUANT_THRESHOLDS["pe_theme_split_enabled"]  # 是否启用主题分裂逻辑

    if theme_split:
        # 主题股用宽松上限，非主题股用严格上限
        has_theme = df.get("policy_theme_hit", pd.Series(0, index=df.index)).fillna(0) >= 1
        pe_limit = pd.Series(pe_ttm_strict, index=df.index)
        pe_limit[has_theme] = pe_ttm_max
    else:
        # 全体统一使用宽松上限（概念库完善前的默认模式）
        pe_limit = pd.Series(pe_ttm_max, index=df.index)

    cond = (
        (df["circ_mv"] >= QUANT_THRESHOLDS["circ_mv_min"]) &
        (df["amount"] >= QUANT_THRESHOLDS["amount_min"]) &
        (df["amount_ma20_prev"] >= QUANT_THRESHOLDS["avg_amount_20d_min"]) &
        (
            (df["turnover_rate"] >= QUANT_THRESHOLDS["turnover_rate_min"]) |
            (df["amount"] >= QUANT_THRESHOLDS["turnover_amount_or_min"])
        ) &
        (
            (df["pe_ttm"] <= pe_limit) |
            (df["pe_ttm"].isna())
        )
    )
    return df[cond].copy()


def _infer_calendar_bias_from_row(row: pd.Series) -> str:
    bias = row.get("calendar_bias")
    if isinstance(bias, str) and bias:
        return bias

    trade_date = str(row.get("trade_date") or "")
    if len(trade_date) == 8 and trade_date.isdigit():
        month = datetime.strptime(trade_date, "%Y%m%d").month
        return CALENDAR_BIAS_MAP.get(month, "NEUTRAL")
    return "NEUTRAL"


def _infer_policy_theme_hit(row: pd.Series) -> float:
    direct_hit = row.get("policy_theme_hit")
    if direct_hit is not None and not np.isnan(direct_hit):
        return float(direct_hit)

    industry = row.get("industry")
    if not isinstance(industry, str) or not industry.strip():
        return 0.0
    text = industry.strip().lower()
    for keyword in POLICY_THEME_KEYWORDS:
        if keyword.lower() in text:
            return 1.0
    return 0.0


def _score_s11(row: pd.Series) -> float:
    theme_hit = _infer_policy_theme_hit(row)
    if theme_hit < 1:
        return 0.0

    catalyst_active = row.get("policy_catalyst_active")
    catalyst_active = 1.0 if catalyst_active is not None and not np.isnan(catalyst_active) and catalyst_active >= 1 else 0.0
    momentum_score = _piecewise_score(row.get("rps_20"), [(70, 0.0), (85, 1.2), (95, 2.0), (100, 2.5)])
    base = 7.0
    catalyst_bonus = 2.0 if catalyst_active >= 1 else 0.0
    return float(np.clip(base + catalyst_bonus + momentum_score * 0.4, 0, 10))


def _score_s12(row: pd.Series) -> float:
    bias = _infer_calendar_bias_from_row(row)

    if bias == "AGGRESSIVE":
        rps20 = row.get("rps_20")
        gpoint = row.get("g_point_strength")
        if (rps20 is None or np.isnan(rps20) or rps20 < 85) and (gpoint is None or np.isnan(gpoint) or gpoint < 2.5):
            return 0.0
        rps_score = _piecewise_score(rps20, [(80, 0.0), (90, 8.5), (97, 10.0)])
        gpoint_score = _piecewise_score(gpoint, [(2.0, 0.0), (3.0, 8.0), (4.0, 10.0)])
        return float(np.clip(0.7 * rps_score + 0.3 * gpoint_score, 0, 10))

    if bias == "DEFENSIVE":
        pe_ttm = row.get("pe_ttm")
        amplitude = row.get("amplitude_5d")
        if pe_ttm is None or np.isnan(pe_ttm) or pe_ttm > 35:
            return 0.0
        if amplitude is None or np.isnan(amplitude) or amplitude > 0.04:
            return 0.0
        pe_score = _piecewise_score(40 - pe_ttm, [(-10, 0.0), (5, 8.0), (20, 10.0)])
        amp_score = _piecewise_score(0.05 - amplitude, [(-0.03, 0.0), (0.01, 8.0), (0.03, 10.0)])
        return float(np.clip(0.6 * pe_score + 0.4 * amp_score, 0, 10))

    if bias == "VALUE":
        pe_ttm = row.get("pe_ttm")
        close_to_ma250 = row.get("close_to_ma250")
        if pe_ttm is None or np.isnan(pe_ttm) or pe_ttm > 30:
            return 0.0
        if close_to_ma250 is None or np.isnan(close_to_ma250) or close_to_ma250 > 1.08:
            return 0.0
        pe_score = _piecewise_score(35 - pe_ttm, [(-15, 0.0), (5, 8.0), (20, 10.0)])
        near_ma_score = _piecewise_score(1.10 - close_to_ma250, [(-0.05, 0.0), (0.02, 8.0), (0.08, 10.0)])
        return float(np.clip(0.55 * pe_score + 0.45 * near_ma_score, 0, 10))

    return 0.0


def _score_s21(row: pd.Series) -> float:
    if (row.get("g_point_strength") or 0) < QUANT_THRESHOLDS["g_point_vol_ratio"]:
        return 0.0
    if (row.get("close_to_ma250") or 999) > 1.10:
        return 0.0

    g_point = _piecewise_score(row.get("g_point_strength"), [(2.0, 0.0), (3.0, 8.0), (4.0, 10.0)])
    low_zone = _piecewise_score(1.10 - row.get("close_to_ma250"), [(-0.2, 0.0), (0.0, 8.0), (0.1, 10.0)])
    return float(np.clip(g_point * 0.75 + low_zone * 0.25, 0, 10))


def _score_s31(row: pd.Series) -> float:
    if (row.get("rps_20") or 0) < QUANT_THRESHOLDS["rps_20_min"]:
        return 0.0
    if (row.get("rps_60") or 0) < QUANT_THRESHOLDS["rps_60_min"]:
        return 0.0

    rps20_score = _piecewise_score(row.get("rps_20"), [(80, 0.0), (90, 7.0), (95, 9.0), (100, 10.0)])
    rps60_score = _piecewise_score(row.get("rps_60"), [(70, 0.0), (85, 8.0), (95, 10.0)])
    return float(np.clip(0.6 * rps20_score + 0.4 * rps60_score, 0, 10))


def _score_s32(row: pd.Series) -> float:
    winner_rate = row.get("winner_rate")
    chip_cv = row.get("chip_peak_cv_10")
    cost_band = row.get("cost_band_90")
    turnover_rate = row.get("turnover_rate")
    circ_mv = row.get("circ_mv")

    if winner_rate is None or np.isnan(winner_rate) or winner_rate <= QUANT_THRESHOLDS["winner_rate_high"]:
        return 0.0
    if chip_cv is None or np.isnan(chip_cv) or chip_cv >= 0.05:
        return 0.0
    # cost_band_90 < 15% 约束
    if cost_band is not None and not np.isnan(cost_band) and cost_band >= 0.15:
        return 0.0

    winner_score = _piecewise_score(winner_rate, [(85.0, 0.0), (90.0, 8.0), (95.0, 9.5), (100.0, 10.0)])
    chip_score = _piecewise_score(0.05 - chip_cv, [(-0.02, 0.0), (0.0, 8.0), (0.03, 10.0)])

    exhaustion_bonus = 0.0
    if (
        circ_mv is not None and not np.isnan(circ_mv) and 50e8 <= circ_mv <= 100e8 and
        turnover_rate is not None and not np.isnan(turnover_rate) and turnover_rate < 1.0
    ):
        exhaustion_bonus = 1.0

    return float(np.clip(0.5 * winner_score + 0.5 * chip_score + exhaustion_bonus, 0, 10))


def _score_s33(row: pd.Series) -> float:
    inst_count = row.get("inst_count")
    inst_net_buy = row.get("inst_net_buy")

    if inst_count is None or np.isnan(inst_count) or inst_count < 2:
        return 0.0
    if inst_net_buy is None or np.isnan(inst_net_buy) or inst_net_buy <= 50_000_000:
        return 0.0

    count_score = _piecewise_score(inst_count, [(2.0, 8.0), (3.0, 9.0), (5.0, 10.0)])
    net_buy_score = _piecewise_score(inst_net_buy, [(50_000_000, 8.0), (100_000_000, 9.2), (300_000_000, 10.0)])
    return float(np.clip(0.4 * count_score + 0.6 * net_buy_score, 0, 10))


def _score_s34(row: pd.Series) -> float:
    north_days = row.get("north_consecutive_days")
    north_sum_3d = row.get("north_sum_3d")
    industry_slope = row.get("industry_rps_slope_3d")

    if north_days is None or pd.isna(north_days) or north_days < 3:
        return 0.0
    if north_sum_3d is None or pd.isna(north_sum_3d) or north_sum_3d <= 0:
        return 0.0
    if industry_slope is None or pd.isna(industry_slope) or industry_slope <= 0:
        return 0.0

    days_score = _piecewise_score(north_days, [(3.0, 8.0), (5.0, 9.0), (8.0, 10.0)])
    north_sum_score = _piecewise_score(north_sum_3d, [(0.0, 7.0), (50.0, 8.5), (150.0, 10.0)])
    industry_score = _piecewise_score(industry_slope, [(0.0, 7.0), (2.0, 8.5), (5.0, 10.0)])
    return float(np.clip(0.4 * days_score + 0.35 * north_sum_score + 0.25 * industry_score, 0, 10))


def _score_s22(row: pd.Series) -> float:
    shrink_ratio = row.get("l_shape_shrink_ratio")
    if (row.get("had_limit_10d") or 0) < 1:
        return 0.0
    if shrink_ratio is None or np.isnan(shrink_ratio) or shrink_ratio > 0.50:
        return 0.0
    # Vol <= LLV(Vol,20) 约束
    vol_at_llv = row.get("vol_at_llv_20")
    if vol_at_llv is None or np.isnan(vol_at_llv) or vol_at_llv < 1:
        return 0.0

    has_limit = 10.0
    shrink = _piecewise_score(0.30 - shrink_ratio, [(-0.2, 5.0), (0.0, 8.5), (0.15, 10.0)])
    amplitude = _piecewise_score(0.03 - row.get("amplitude_5d"), [(-0.05, 0.0), (0.0, 7.5), (0.02, 10.0)])
    return float(np.clip(0.3 * has_limit + 0.45 * shrink + 0.25 * amplitude, 0, 10))


def _score_s23(row: pd.Series) -> float:
    if (row.get("cross3_close_break") or 0) < 1:
        return 0.0
    if (row.get("ma_dispersion") or 999) >= 0.05:
        return 0.0
    if (row.get("cross3_trend_strength") or -999) <= 0.03:
        return 0.0

    close_break = 10.0 if (row.get("cross3_close_break") or 0) >= 1 else 0.0
    dispersion_score = _piecewise_score(0.08 - row.get("ma_dispersion"), [(-0.1, 0.0), (0.03, 7.0), (0.05, 10.0)])
    trend_score = _piecewise_score(row.get("cross3_trend_strength"), [(-0.02, 0.0), (0.01, 5.0), (0.03, 8.0), (0.06, 10.0)])
    vol_score = _piecewise_score(row.get("cross3_vol_ratio"), [(0.8, 0.0), (1.2, 6.0), (2.0, 10.0)])
    return float(np.clip(0.35 * close_break + 0.25 * dispersion_score + 0.2 * trend_score + 0.2 * vol_score, 0, 10))


def _score_s24(row: pd.Series) -> float:
    reversal_flag = row.get("reversal_flag")
    guide_shadow_pct = row.get("guide_shadow_pct")
    guide_body_pct = row.get("guide_body_pct")
    vol_ratio = row.get("vol_ratio_5")

    if reversal_flag is None or np.isnan(reversal_flag) or reversal_flag < 1:
        return 0.0

    shadow_score = _piecewise_score(guide_shadow_pct, [(0.02, 0.0), (0.03, 8.0), (0.06, 10.0)])
    body_score = _piecewise_score(0.02 - guide_body_pct, [(-0.02, 0.0), (0.005, 8.0), (0.015, 10.0)])
    vol_score = _piecewise_score(vol_ratio, [(0.8, 0.0), (1.2, 8.0), (2.0, 10.0)])
    return float(np.clip(0.45 * shadow_score + 0.3 * body_score + 0.25 * vol_score, 0, 10))


def _score_s25(row: pd.Series) -> float:
    ma20 = row.get("ma20")
    low = row.get("low_adj")
    close = row.get("close_adj")
    false_break_depth = row.get("false_break_depth")
    lower_shadow_pct = row.get("lower_shadow_pct")

    if any(v is None or np.isnan(v) for v in [ma20, low, close, false_break_depth, lower_shadow_pct]):
        return 0.0
    if not (low < ma20 and close > ma20 and lower_shadow_pct > 0.02):
        return 0.0

    depth_score = _piecewise_score(false_break_depth, [(0.0, 6.5), (0.01, 8.0), (0.03, 10.0)])
    shadow_score = _piecewise_score(lower_shadow_pct, [(0.015, 0.0), (0.02, 8.0), (0.05, 10.0)])
    return float(np.clip(0.5 * depth_score + 0.5 * shadow_score, 0, 10))


def _score_s26(row: pd.Series) -> float:
    distance_to_618 = row.get("distance_to_618")
    vol_ratio = row.get("vol_ratio_5")
    g_point = row.get("g_point_strength")

    if distance_to_618 is None or np.isnan(distance_to_618) or distance_to_618 > 0.01:
        return 0.0
    if vol_ratio is None or np.isnan(vol_ratio) or vol_ratio > 0.95:
        return 0.0

    distance_score = _piecewise_score(0.012 - distance_to_618, [(-0.01, 0.0), (0.004, 8.0), (0.01, 10.0)])
    shrink_score = _piecewise_score(1.0 - vol_ratio, [(-0.1, 0.0), (0.05, 8.0), (0.2, 10.0)])
    anti_chase_score = _piecewise_score(2.5 - g_point, [(-1.0, 4.0), (0.0, 8.0), (1.5, 10.0)])
    return float(np.clip(0.45 * distance_score + 0.35 * shrink_score + 0.2 * anti_chase_score, 0, 10))


def _get_report_month(row: pd.Series) -> int:
    trade_date = str(row.get("trade_date") or "")
    if len(trade_date) == 8 and trade_date.isdigit():
        return int(trade_date[4:6])
    return 0


def _is_earnings_bomb(row: pd.Series) -> bool:
    forecast_type = str(row.get("forecast_type") or "")
    report_month = _get_report_month(row)

    if check_earnings_meltdown(forecast_type, report_month):
        return True

    disclosure = row.get("earnings_disclosure_month")
    negative = row.get("earnings_negative_flag")
    if (
        disclosure is not None and not np.isnan(disclosure) and disclosure >= 1 and
        negative is not None and not np.isnan(negative) and negative >= 1
    ):
        return True
    return False


def _is_sell_on_news(row: pd.Series) -> bool:
    preincrease = row.get("earnings_preincrease_flag")
    pct_20d = row.get("pct_chg_20d")

    if preincrease is None or np.isnan(preincrease) or preincrease < 1:
        return False
    if pct_20d is None or np.isnan(pct_20d):
        return False
    return pct_20d > 0.30


def detect_daily_candidates(factor_df: pd.DataFrame) -> List[DailyCandidate]:
    if factor_df.empty:
        return []

    screened = _hard_filter(factor_df)
    if screened.empty:
        return []

    candidates: List[DailyCandidate] = []
    for _, row in screened.iterrows():
        if _is_earnings_bomb(row):
            continue

        s11 = _score_s11(row)
        s12 = _score_s12(row)
        s21 = _score_s21(row)
        s22 = _score_s22(row)
        s23 = _score_s23(row)
        s24 = _score_s24(row)
        s25 = _score_s25(row)
        s26 = _score_s26(row)
        s31 = _score_s31(row)
        s32 = _score_s32(row)
        s33 = _score_s33(row)
        s34 = _score_s34(row)

        if s11 <= 0 and s12 <= 0 and s21 <= 0 and s22 <= 0 and s23 <= 0 and s24 <= 0 and s25 <= 0 and s26 <= 0 and s31 <= 0 and s32 <= 0 and s33 <= 0 and s34 <= 0:
            continue

        risk_tags = tag_basic_risks(row.get("turnover_rate"), row.get("pe_ttm"))
        if _is_sell_on_news(row):
            risk_tags = [*risk_tags, "SELL_ON_NEWS"]
        signals: List[StrategySignal] = []

        if s11 > 0:
            signals.append(
                StrategySignal(
                    ts_code=row["ts_code"],
                    trade_date=row["trade_date"],
                    strategy_id="S11",
                    strategy_name="十五五规划主线",
                    dimension="macro",
                    score_raw=s11,
                    score_scaled=s11 * 10,
                    factors={
                        "policy_theme_hit": float(_infer_policy_theme_hit(row)),
                        "policy_catalyst_active": float(row.get("policy_catalyst_active") or 0),
                        "rps_20": float(row.get("rps_20") or 0),
                    },
                    risk_tags=risk_tags,
                )
            )

        if s12 > 0:
            signals.append(
                StrategySignal(
                    ts_code=row["ts_code"],
                    trade_date=row["trade_date"],
                    strategy_id="S12",
                    strategy_name="季节性日历效应",
                    dimension="macro",
                    score_raw=s12,
                    score_scaled=s12 * 10,
                    factors={
                        "calendar_bias": _infer_calendar_bias_from_row(row),
                        "pe_ttm": float(row.get("pe_ttm") or 0),
                        "amplitude_5d": float(row.get("amplitude_5d") or 0),
                    },
                    risk_tags=risk_tags,
                )
            )

        if s21 > 0:
            signals.append(
                StrategySignal(
                    ts_code=row["ts_code"],
                    trade_date=row["trade_date"],
                    strategy_id="S21",
                    strategy_name="G点脉冲",
                    dimension="pattern",
                    score_raw=s21,
                    score_scaled=s21 * 10,
                    factors={
                        "g_point_strength": float(row.get("g_point_strength") or 0),
                        "close_to_ma250": float(row.get("close_to_ma250") or 0),
                    },
                    risk_tags=risk_tags,
                )
            )

        if s22 > 0:
            signals.append(
                StrategySignal(
                    ts_code=row["ts_code"],
                    trade_date=row["trade_date"],
                    strategy_id="S22",
                    strategy_name="缩量L型",
                    dimension="pattern",
                    score_raw=s22,
                    score_scaled=s22 * 10,
                    factors={
                        "had_limit_10d": float(row.get("had_limit_10d") or 0),
                        "l_shape_shrink_ratio": float(row.get("l_shape_shrink_ratio") or 0),
                        "amplitude_5d": float(row.get("amplitude_5d") or 0),
                    },
                    risk_tags=risk_tags,
                )
            )

        if s23 > 0:
            signals.append(
                StrategySignal(
                    ts_code=row["ts_code"],
                    trade_date=row["trade_date"],
                    strategy_id="S23",
                    strategy_name="一阳穿三线",
                    dimension="pattern",
                    score_raw=s23,
                    score_scaled=s23 * 10,
                    factors={
                        "ma_dispersion": float(row.get("ma_dispersion") or 0),
                        "cross3_trend_strength": float(row.get("cross3_trend_strength") or 0),
                        "cross3_vol_ratio": float(row.get("cross3_vol_ratio") or 0),
                    },
                    risk_tags=risk_tags,
                )
            )

        if s24 > 0:
            signals.append(
                StrategySignal(
                    ts_code=row["ts_code"],
                    trade_date=row["trade_date"],
                    strategy_id="S24",
                    strategy_name="仙人指路反包",
                    dimension="pattern",
                    score_raw=s24,
                    score_scaled=s24 * 10,
                    factors={
                        "guide_shadow_pct": float(row.get("guide_shadow_pct") or 0),
                        "guide_body_pct": float(row.get("guide_body_pct") or 0),
                        "reversal_flag": float(row.get("reversal_flag") or 0),
                    },
                    risk_tags=risk_tags,
                )
            )

        if s25 > 0:
            signals.append(
                StrategySignal(
                    ts_code=row["ts_code"],
                    trade_date=row["trade_date"],
                    strategy_id="S25",
                    strategy_name="釜底抽薪",
                    dimension="pattern",
                    score_raw=s25,
                    score_scaled=s25 * 10,
                    factors={
                        "false_break_depth": float(row.get("false_break_depth") or 0),
                        "lower_shadow_pct": float(row.get("lower_shadow_pct") or 0),
                    },
                    risk_tags=risk_tags,
                )
            )

        if s26 > 0:
            signals.append(
                StrategySignal(
                    ts_code=row["ts_code"],
                    trade_date=row["trade_date"],
                    strategy_id="S26",
                    strategy_name="黄金分割低吸",
                    dimension="pattern",
                    score_raw=s26,
                    score_scaled=s26 * 10,
                    factors={
                        "golden_buy_price": float(row.get("golden_buy_price") or 0),
                        "distance_to_618": float(row.get("distance_to_618") or 0),
                    },
                    risk_tags=risk_tags,
                )
            )

        if s31 > 0:
            signals.append(
                StrategySignal(
                    ts_code=row["ts_code"],
                    trade_date=row["trade_date"],
                    strategy_id="S31",
                    strategy_name="RPS动量筛选",
                    dimension="sentiment",
                    score_raw=s31,
                    score_scaled=s31 * 10,
                    factors={
                        "rps_20": float(row.get("rps_20") or 0),
                        "rps_60": float(row.get("rps_60") or 0),
                    },
                    risk_tags=risk_tags,
                )
            )

        if s32 > 0:
            signals.append(
                StrategySignal(
                    ts_code=row["ts_code"],
                    trade_date=row["trade_date"],
                    strategy_id="S32",
                    strategy_name="筹码低位锁仓",
                    dimension="sentiment",
                    score_raw=s32,
                    score_scaled=s32 * 10,
                    factors={
                        "winner_rate": float(row.get("winner_rate") or 0),
                        "chip_peak_cv_10": float(row.get("chip_peak_cv_10") or 0),
                        "turnover_rate": float(row.get("turnover_rate") or 0),
                    },
                    risk_tags=risk_tags,
                )
            )

        if s33 > 0:
            signals.append(
                StrategySignal(
                    ts_code=row["ts_code"],
                    trade_date=row["trade_date"],
                    strategy_id="S33",
                    strategy_name="龙虎榜机构合力",
                    dimension="sentiment",
                    score_raw=s33,
                    score_scaled=s33 * 10,
                    factors={
                        "inst_count": float(row.get("inst_count") or 0),
                        "inst_net_buy": float(row.get("inst_net_buy") or 0),
                    },
                    risk_tags=risk_tags,
                )
            )

        if s34 > 0:
            signals.append(
                StrategySignal(
                    ts_code=row["ts_code"],
                    trade_date=row["trade_date"],
                    strategy_id="S34",
                    strategy_name="北向资金共振",
                    dimension="sentiment",
                    score_raw=s34,
                    score_scaled=s34 * 10,
                    factors={
                        "north_consecutive_days": float(row.get("north_consecutive_days") or 0),
                        "north_sum_3d": float(row.get("north_sum_3d") or 0),
                        "industry_rps_slope_3d": float(row.get("industry_rps_slope_3d") or 0),
                    },
                    risk_tags=risk_tags,
                )
            )

        macro_signals_count = int(s11 > 0) + int(s12 > 0)
        macro_base = max(s11, s12)
        macro_resonance_bonus = max(0, macro_signals_count - 1) * 0.8
        macro_raw = float(np.clip(macro_base + macro_resonance_bonus, 0, 10))

        pattern_signals_count = int(s21 > 0) + int(s22 > 0) + int(s23 > 0) + int(s24 > 0) + int(s25 > 0) + int(s26 > 0)
        pattern_base = max(s21, s22, s23, s24, s25, s26)
        pattern_resonance_bonus = max(0, pattern_signals_count - 1) * 0.8
        pattern_raw = float(np.clip(pattern_base + pattern_resonance_bonus, 0, 10))

        sentiment_signals_count = int(s31 > 0) + int(s32 > 0) + int(s33 > 0) + int(s34 > 0)
        sentiment_base = max(s31, s32, s33, s34)
        sentiment_resonance_bonus = max(0, sentiment_signals_count - 1) * 0.6
        sentiment_raw = float(np.clip(sentiment_base + sentiment_resonance_bonus, 0, 10))

        candidates.append(
            DailyCandidate(
                ts_code=row["ts_code"],
                trade_date=row["trade_date"],
                name=row.get("name") or row["ts_code"],
                industry=row.get("industry") or "未知",
                macro_raw=macro_raw,
                pattern_raw=pattern_raw,
                sentiment_raw=sentiment_raw,
                intraday_raw=0.0,
                factor_snapshot={
                    "policy_theme_hit": float(_infer_policy_theme_hit(row)),
                    "policy_catalyst_active": _fval(row.get("policy_catalyst_active")),
                    "calendar_bias": _infer_calendar_bias_from_row(row),
                    "forecast_type": str(row.get("forecast_type") or ""),
                    "earnings_negative_flag": _fval(row.get("earnings_negative_flag")),
                    "earnings_preincrease_flag": _fval(row.get("earnings_preincrease_flag")),
                    "g_point_strength": _fval(row.get("g_point_strength")),
                    "close_to_ma250": _fval(row.get("close_to_ma250")),
                    "l_shape_shrink_ratio": _fval(row.get("l_shape_shrink_ratio")),
                    "amplitude_5d": _fval(row.get("amplitude_5d")),
                    "ma_dispersion": _fval(row.get("ma_dispersion")),
                    "cross3_trend_strength": _fval(row.get("cross3_trend_strength")),
                    "guide_shadow_pct": _fval(row.get("guide_shadow_pct")),
                    "guide_body_pct": _fval(row.get("guide_body_pct")),
                    "reversal_flag": _fval(row.get("reversal_flag")),
                    "false_break_depth": _fval(row.get("false_break_depth")),
                    "lower_shadow_pct": _fval(row.get("lower_shadow_pct")),
                    "golden_buy_price": _fval(row.get("golden_buy_price")),
                    "distance_to_618": _fval(row.get("distance_to_618")),
                    "atr_14": _fval(row.get("atr_14")),
                    "atr_14_trend_5": _fval(row.get("atr_14_trend_5")),
                    "ps_ttm": _fval(row.get("ps_ttm")),
                    "peg": _fval(row.get("peg")),
                    "rps_20": _fval(row.get("rps_20")),
                    "rps_60": _fval(row.get("rps_60")),
                    "winner_rate": _fval(row.get("winner_rate")),
                    "chip_peak_cv_10": _fval(row.get("chip_peak_cv_10")),
                    "inst_count": _fval(row.get("inst_count")),
                    "inst_net_buy": _fval(row.get("inst_net_buy")),
                    "north_consecutive_days": _fval(row.get("north_consecutive_days")),
                    "north_sum_3d": _fval(row.get("north_sum_3d")),
                    "industry_rps_slope_3d": _fval(row.get("industry_rps_slope_3d")),
                    "turnover_rate": _fval(row.get("turnover_rate")),
                    "pe_ttm": _fval(row.get("pe_ttm")),
                },
                signals=signals,
                risk_tags=risk_tags,
            )
        )

    return candidates
