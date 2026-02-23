import argparse
from pathlib import Path

from code.analytics.factors import get_latest_trade_date_from_db, load_factor_snapshot
from code.analytics.reason_generator import write_daily_outputs
from code.analytics.scorer import rank_candidates
from code.analytics.screener import detect_daily_candidates
from code.core.config import NIGHTLY_INTRADAY_WEIGHT, QUANT_THRESHOLDS
from code.core.logger import get_logger
from code.data.sync_daily import ensure_data_ready
from code.review.market_context import build_market_context, get_market_risk_state
from code.review.notion_pusher import push_daily_report_to_notion
from code.review.signal_store import persist_strategy_signals

logger = get_logger(__name__)


def run_daily_pipeline(
    trade_date: str,
    lookback_days: int = 260,
    push_notion: bool = False,
    notion_title_prefix: str = "QRAI 日级复盘",
    notion_existing_policy: str = "skip",
):
    logger.info("[daily] ── 开始日级复盘 trade_date=%s lookback_days=%d ──", trade_date, lookback_days)

    logger.info("[daily] Step 1/6  加载因子快照...")
    factor_df = load_factor_snapshot(trade_date=trade_date, lookback_days=lookback_days)
    if factor_df.empty:
        raise RuntimeError(f"未找到 {trade_date} 的因子输入数据")
    logger.info("[daily] Step 1/6  因子快照完成 rows=%d", len(factor_df))

    logger.info("[daily] Step 2/6  策略筛选...")
    candidates = detect_daily_candidates(factor_df)
    logger.info("[daily] Step 2/6  筛选完成 candidates=%d", len(candidates))

    logger.info("[daily] Step 3/6  评分排序...")
    market_risk = get_market_risk_state(trade_date)
    ranked = rank_candidates(
        candidates,
        intraday_weight=NIGHTLY_INTRADAY_WEIGHT,
        score_multiplier=float(market_risk.get("score_multiplier", 1.0)),
        market_risk=market_risk,
    )
    logger.info("[daily] Step 3/6  排序完成 ranked=%d", len(ranked))

    logger.info("[daily] Step 4/6  落库 strategy_signals...")
    persisted = persist_strategy_signals(ranked, trade_date=trade_date, top_n=QUANT_THRESHOLDS["daily_top_n"])
    logger.info("[daily] Step 4/6  落库完成 rows=%d", persisted)

    logger.info("[daily] Step 5/6  生成大盘概况...")
    market_overview = build_market_context(trade_date)
    logger.info("[daily] Step 5/6  大盘概况完成")

    logger.info("[daily] Step 6/6  输出报告...")
    project_root = Path(__file__).resolve().parents[2]
    output_dir = project_root / "report"
    paths = write_daily_outputs(
        ranked,
        trade_date=trade_date,
        output_dir=output_dir,
        top_n=QUANT_THRESHOLDS["daily_top_n"],
        market_overview=market_overview,
    )

    logger.info("候选生成完成: %d 只", len(ranked))
    logger.info(
        "全局熔断器: %s | 折减系数=%.2f | 跌停家数=%d | 上涨占比=%.1f%%",
        "触发" if market_risk.get("is_meltdown") else "未触发",
        market_risk.get("score_multiplier", 1.0),
        market_risk.get("down_limit_count", 0),
        market_risk.get("up_ratio", 0.0) * 100,
    )
    logger.info("落库完成: %d 条 strategy_signals", persisted)
    logger.info("Markdown: %s", paths["markdown"])
    logger.info("CSV:      %s", paths["csv"])

    if push_notion:
        push_result = push_daily_report_to_notion(
            trade_date=trade_date,
            markdown_path=paths["markdown"],
            csv_path=paths["csv"],
            market_overview=market_overview,
            title_prefix=notion_title_prefix,
            existing_page_policy=notion_existing_policy,
        )
        if push_result.get("success"):
            logger.info("Notion 推送成功: %s | %s", push_result.get("url"), push_result.get("message", "created"))
        else:
            logger.warning("Notion 推送未完成: %s", push_result.get("error"))


def main():
    parser = argparse.ArgumentParser(description="QRAI-Trader 日级复盘入口（Phase-1）")
    parser.add_argument("--date", type=str, help="目标交易日 YYYYMMDD，默认自动检测")
    parser.add_argument("--lookback-days", type=int, default=260, help="因子回看天数，默认 260")
    parser.add_argument("--skip-sync", action="store_true", help="跳过 sync_daily 检查补齐")
    parser.add_argument("--full-scan", action="store_true", help="同步时启用全量扫描")
    parser.add_argument("--push-notion", action="store_true", help="复盘完成后推送到 Notion")
    parser.add_argument("--notion-title-prefix", type=str, default="QRAI 日级复盘", help="Notion 页面标题前缀")
    parser.add_argument(
        "--notion-existing-policy",
        type=str,
        default="skip",
        choices=["skip", "update", "append"],
        help="同名 Notion 日报处理策略：skip 跳过，update 覆盖，append 追加",
    )
    args = parser.parse_args()

    if args.date:
        trade_date = args.date
        if not args.skip_sync:
            ensure_data_ready(end_date=trade_date, full_scan=args.full_scan)
    else:
        if args.skip_sync:
            latest = get_latest_trade_date_from_db()
            if not latest:
                raise RuntimeError("数据库无可用交易日，请先执行数据初始化")
            trade_date = latest
        else:
            trade_date = ensure_data_ready(full_scan=args.full_scan)

    run_daily_pipeline(
        trade_date=trade_date,
        lookback_days=args.lookback_days,
        push_notion=args.push_notion,
        notion_title_prefix=args.notion_title_prefix,
        notion_existing_policy=args.notion_existing_policy,
    )


if __name__ == "__main__":
    main()
