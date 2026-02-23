from sqlalchemy import text

from code.core.config import (
    GLOBAL_MELTDOWN_DOWN_LIMIT_COUNT,
    GLOBAL_MELTDOWN_SCORE_MULTIPLIER,
    GLOBAL_MELTDOWN_UP_RATIO,
)
from code.core.db_manager import get_engine
from code.core.logger import get_logger

logger = get_logger(__name__)


def evaluate_market_meltdown(
    down_limit_count: int,
    up_ratio: float,
    down_limit_threshold: int = GLOBAL_MELTDOWN_DOWN_LIMIT_COUNT,
    up_ratio_threshold: float = GLOBAL_MELTDOWN_UP_RATIO,
    score_multiplier: float = GLOBAL_MELTDOWN_SCORE_MULTIPLIER,
) -> dict:
    is_meltdown = (down_limit_count > down_limit_threshold) or (up_ratio < up_ratio_threshold)
    return {
        "is_meltdown": bool(is_meltdown),
        "score_multiplier": float(score_multiplier if is_meltdown else 1.0),
        "down_limit_count": int(down_limit_count),
        "up_ratio": float(up_ratio),
    }


def get_market_risk_state(trade_date: str) -> dict:
    engine = get_engine("db1")
    sql = text(
        """
        SELECT
            SUM(CASE WHEN pct_chg > 0 THEN 1 ELSE 0 END) AS up_count,
            COUNT(*) AS total_count,
            SUM(CASE WHEN pct_chg <= -9.5 THEN 1 ELSE 0 END) AS down_limit_count
        FROM stock_daily
        WHERE trade_date = :trade_date
        """
    )

    up_count = 0
    total_count = 0
    down_limit_count = 0
    with engine.connect() as conn:
        row = conn.execute(sql, {"trade_date": trade_date}).fetchone()
        if row:
            up_count = int(row[0] or 0)
            total_count = int(row[1] or 0)
            down_limit_count = int(row[2] or 0)

    up_ratio = (up_count / total_count) if total_count > 0 else 0.0
    return evaluate_market_meltdown(down_limit_count=down_limit_count, up_ratio=up_ratio)


def build_market_context(trade_date: str) -> str:
    engine1 = get_engine("db1")
    engine2 = get_engine("db2")

    index_sql = text(
        """
        SELECT pct_chg, amount
        FROM index_daily
        WHERE ts_code = '000001.SH' AND trade_date = :trade_date
        LIMIT 1
        """
    )
    north_sql = text(
        """
        SELECT north_money
        FROM moneyflow_hsgt
        WHERE trade_date = :trade_date
        LIMIT 1
        """
    )

    pct_chg = None
    amount = None
    north_money = None

    with engine1.connect() as conn:
        row = conn.execute(index_sql, {"trade_date": trade_date}).fetchone()
        if row:
            pct_chg = float(row[0]) if row[0] is not None else None
            amount = float(row[1]) if row[1] is not None else None

    with engine2.connect() as conn:
        row = conn.execute(north_sql, {"trade_date": trade_date}).fetchone()
        if row:
            north_money = float(row[0]) if row[0] is not None else None

    pct_text = "未知" if pct_chg is None else f"上证涨跌 {pct_chg:.2f}%"
    amount_text = "未知" if amount is None else f"上证成交额 {amount / 1e8:.1f} 亿"
    north_text = "未知" if north_money is None else f"北向资金 {north_money:.1f} 亿"

    risk_state = get_market_risk_state(trade_date)
    risk_text = (
        f"熔断器触发=是(跌停{risk_state['down_limit_count']}家,上涨占比{risk_state['up_ratio'] * 100:.1f}%)"
        if risk_state["is_meltdown"]
        else f"熔断器触发=否(跌停{risk_state['down_limit_count']}家,上涨占比{risk_state['up_ratio'] * 100:.1f}%)"
    )

    return f"{pct_text}；{amount_text}；{north_text}；{risk_text}"
