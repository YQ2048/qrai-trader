from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class StrategySignal:
    ts_code: str
    trade_date: str
    strategy_id: str
    strategy_name: str
    dimension: str
    score_raw: float
    score_scaled: float
    factors: Dict[str, float] = field(default_factory=dict)
    risk_tags: List[str] = field(default_factory=list)


@dataclass
class DailyCandidate:
    ts_code: str
    trade_date: str
    name: str
    industry: str
    macro_raw: float
    pattern_raw: float
    sentiment_raw: float
    intraday_raw: float
    factor_snapshot: Dict[str, float]
    signals: List[StrategySignal] = field(default_factory=list)
    risk_tags: List[str] = field(default_factory=list)
