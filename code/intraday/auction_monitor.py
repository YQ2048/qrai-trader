from typing import Dict


def evaluate_auction_signal(
    open_gap_pct: float,
    auction_amount: float,
    avg_amount_5d: float,
    yesterday_context: str = "neutral",
) -> Dict[str, object]:
    if avg_amount_5d <= 0:
        return {"triggered": False, "score": 0.0, "confidence": "LOW", "reason": "缺少均量基准"}

    auction_ratio = auction_amount / avg_amount_5d
    triggered = (auction_ratio >= 0.10) and (1.0 <= open_gap_pct <= 3.5)

    base_score = 0.0
    if triggered:
        gap_score = min(max((open_gap_pct - 1.0) / 2.5, 0.0), 1.0)
        ratio_score = min(max((auction_ratio - 0.10) / 0.20, 0.0), 1.0)
        base_score = (gap_score * 0.4 + ratio_score * 0.6) * 10

    context_bonus = 0.0
    confidence = "LOW"
    if yesterday_context in {"strong_limit", "shrink_pullback"}:
        context_bonus = 1.0
        confidence = "HIGH"
    elif yesterday_context == "neutral":
        confidence = "MEDIUM"

    score = min(base_score + context_bonus, 10.0) if triggered else 0.0
    return {
        "triggered": triggered,
        "score": round(score, 2),
        "confidence": confidence if triggered else "LOW",
        "auction_ratio": round(auction_ratio, 4),
    }
