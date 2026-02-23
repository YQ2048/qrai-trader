from typing import Dict


def calculate_deviation_threshold(avg_amp_20d: float) -> float:
    raw = 0.35 * avg_amp_20d
    return min(max(raw, 0.02), 0.035)


def evaluate_t_signal(price: float, vwap: float, avg_amp_20d: float) -> Dict[str, object]:
    if vwap <= 0:
        return {"triggered": False, "side": "NONE", "deviation": 0.0, "threshold": 0.0}

    threshold = calculate_deviation_threshold(avg_amp_20d)
    deviation = (price - vwap) / vwap

    if deviation >= threshold:
        side = "SELL"
    elif deviation <= -threshold:
        side = "BUY"
    else:
        side = "NONE"

    return {
        "triggered": side != "NONE",
        "side": side,
        "deviation": round(deviation, 4),
        "threshold": round(threshold, 4),
    }


def evaluate_closing_confirmation(
    close_price: float,
    ma20: float,
    last_30min_pct: float,
    sector_linked: bool = True,
) -> Dict[str, object]:
    """S43 尾盘定性确认（14:45）。

    检查条件：
    1. 收盘价 > MA20（趋势完整）
    2. 14:30 后无量偷袭：最后 30 分钟涨幅占全天涨幅 < 80%，
       或有板块联动时放宽（不视为偷袭）

    Args:
        close_price: 当前/收盘价
        ma20: 20 日均线价格
        last_30min_pct: 14:30 后涨幅占全天涨幅的比例（0-1）
        sector_linked: 是否有板块联动（True 则放宽偷袭判定）

    Returns:
        dict with keys: confirmed, close_above_ma20, sneak_attack, reason
    """
    close_above_ma20 = close_price > ma20 if (ma20 and ma20 > 0) else False

    sneak_attack_threshold = 0.80
    sneak_attack = last_30min_pct >= sneak_attack_threshold and not sector_linked

    confirmed = close_above_ma20 and not sneak_attack

    reasons = []
    if not close_above_ma20:
        reasons.append("收盘未站上MA20")
    if sneak_attack:
        reasons.append(f"尾盘偷袭(末30min占比{last_30min_pct:.0%}且无板块联动)")

    return {
        "confirmed": confirmed,
        "close_above_ma20": close_above_ma20,
        "sneak_attack": sneak_attack,
        "reason": "; ".join(reasons) if reasons else "尾盘定性通过",
    }
