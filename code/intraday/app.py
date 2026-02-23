from typing import Dict

from code.intraday.auction_monitor import evaluate_auction_signal
from code.intraday.rules_engine import evaluate_t_signal


def run_intraday_cycle(snapshot: Dict[str, float]) -> Dict[str, object]:
    auction = evaluate_auction_signal(
        open_gap_pct=float(snapshot.get("open_gap_pct", 0.0)),
        auction_amount=float(snapshot.get("auction_amount", 0.0)),
        avg_amount_5d=float(snapshot.get("avg_amount_5d", 0.0)),
        yesterday_context=str(snapshot.get("yesterday_context", "neutral")),
    )
    t_signal = evaluate_t_signal(
        price=float(snapshot.get("price", 0.0)),
        vwap=float(snapshot.get("vwap", 0.0)),
        avg_amp_20d=float(snapshot.get("avg_amp_20d", 0.0)),
    )
    return {"auction": auction, "t_signal": t_signal}
