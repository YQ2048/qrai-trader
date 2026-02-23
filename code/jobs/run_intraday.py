import argparse

from code.data.realtime import build_realtime_snapshot
from code.intraday.app import run_intraday_cycle


def main():
    parser = argparse.ArgumentParser(description="QRAI-Trader 日内监控入口（M5-Seed）")
    parser.add_argument("--ts-code", type=str, help="目标股票代码，如 000001.SZ")
    parser.add_argument("--trade-date", type=str, help="交易日 YYYYMMDD，默认今日")
    parser.add_argument("--use-db-snapshot", action="store_true", help="优先从 DB 构建实时快照")

    parser.add_argument("--open-gap-pct", type=float)
    parser.add_argument("--auction-amount", type=float)
    parser.add_argument("--avg-amount-5d", type=float)
    parser.add_argument("--yesterday-context", type=str)
    parser.add_argument("--price", type=float)
    parser.add_argument("--vwap", type=float)
    parser.add_argument("--avg-amp-20d", type=float)
    args = parser.parse_args()

    if args.use_db_snapshot and args.ts_code:
        snapshot = build_realtime_snapshot(
            ts_code=args.ts_code,
            trade_date=args.trade_date,
            open_gap_pct=args.open_gap_pct,
            auction_amount=args.auction_amount,
            price=args.price,
            vwap=args.vwap,
        )
        if args.avg_amount_5d is not None:
            snapshot["avg_amount_5d"] = args.avg_amount_5d
        if args.avg_amp_20d is not None:
            snapshot["avg_amp_20d"] = args.avg_amp_20d
        if args.yesterday_context:
            snapshot["yesterday_context"] = args.yesterday_context
    else:
        snapshot = {
            "open_gap_pct": args.open_gap_pct if args.open_gap_pct is not None else 2.0,
            "auction_amount": args.auction_amount if args.auction_amount is not None else 3e7,
            "avg_amount_5d": args.avg_amount_5d if args.avg_amount_5d is not None else 2e8,
            "yesterday_context": args.yesterday_context if args.yesterday_context else "neutral",
            "price": args.price if args.price is not None else 10.0,
            "vwap": args.vwap if args.vwap is not None else 9.8,
            "avg_amp_20d": args.avg_amp_20d if args.avg_amp_20d is not None else 0.08,
            "ts_code": args.ts_code or "N/A",
            "trade_date": args.trade_date or "N/A",
        }

    result = run_intraday_cycle(snapshot)
    print("日内监控结果:")
    print(result)


if __name__ == "__main__":
    main()
