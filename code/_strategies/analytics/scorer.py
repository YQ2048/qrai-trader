from typing import Dict, List, Optional

from code.core.config import STRATEGY_WEIGHTS
from code.strategy.signals import DailyCandidate


def _dimension_score(raw_score: float, weight: float) -> float:
    return max(0.0, min(10.0, raw_score)) * weight * 10


def rank_candidates(
    candidates: List[DailyCandidate],
    intraday_weight: Optional[float] = None,
    score_multiplier: float = 1.0,
    market_risk: Optional[Dict] = None,
) -> List[Dict]:
    ranked: List[Dict] = []
    effective_intraday_weight = (
        intraday_weight if intraday_weight is not None else STRATEGY_WEIGHTS["intraday_game"]
    )
    safe_multiplier = max(0.0, min(1.0, score_multiplier))

    for candidate in candidates:
        macro_score = _dimension_score(candidate.macro_raw, STRATEGY_WEIGHTS["macro_calendar"])
        pattern_score = _dimension_score(candidate.pattern_raw, STRATEGY_WEIGHTS["pattern_quant"])
        sentiment_score = _dimension_score(candidate.sentiment_raw, STRATEGY_WEIGHTS["sentiment_rps"])
        intraday_score = _dimension_score(candidate.intraday_raw, effective_intraday_weight)

        base_composite = macro_score + pattern_score + sentiment_score + intraday_score
        composite_score = base_composite * safe_multiplier

        ranked.append(
            {
                "ts_code": candidate.ts_code,
                "trade_date": candidate.trade_date,
                "name": candidate.name,
                "industry": candidate.industry,
                "composite_score": round(composite_score, 2),
                "base_composite_score": round(base_composite, 2),
                "score_detail": {
                    "macro": round(macro_score, 2),
                    "pattern": round(pattern_score, 2),
                    "sentiment": round(sentiment_score, 2),
                    "intraday": round(intraday_score, 2),
                },
                "score_multiplier": round(safe_multiplier, 4),
                "market_risk": market_risk or {},
                "signals": candidate.signals,
                "factor_snapshot": candidate.factor_snapshot,
                "risk_tags": candidate.risk_tags,
            }
        )

    ranked.sort(key=lambda x: x["composite_score"], reverse=True)
    return ranked
