from typing import Iterable, List, Optional


def skyline_stop_loss(lows: Iterable[float], lookback: int = 3) -> Optional[float]:
    low_list = list(lows)
    if len(low_list) <= lookback:
        return None
    return min(low_list[-(lookback + 1):-1])


def check_earnings_meltdown(forecast_type: str, report_month: int) -> bool:
    if report_month not in {1, 4, 7, 10}:
        return False
    return forecast_type in {"预减", "首亏", "续亏", "略减"}


def tag_basic_risks(turnover_rate: float, pe_ttm: float) -> List[str]:
    tags: List[str] = []
    if turnover_rate is not None and turnover_rate >= 18:
        tags.append("HIGH_TURNOVER")
    if pe_ttm is not None and pe_ttm >= 50:
        tags.append("HIGH_PE")
    return tags
