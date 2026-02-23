import argparse

from code.review.signal_store import normalize_ts_codes, update_signal_status, update_signal_status_batch


def main():
    parser = argparse.ArgumentParser(description="更新 strategy_signals 状态")
    parser.add_argument("--date", required=True, help="信号日期 YYYYMMDD")
    parser.add_argument("--ts-code", help="单只股票代码，如 000001.SZ")
    parser.add_argument("--ts-codes", help="批量股票代码，逗号分隔，如 000001.SZ,000002.SZ")
    parser.add_argument("--to", required=True, help="目标状态：CONFIRMED/EXECUTED/CANCELED")
    parser.add_argument("--from-status", help="可选：要求当前状态匹配后才更新")
    parser.add_argument("--operator", default="manual", help="操作者标识")
    parser.add_argument("--note", help="状态变更备注")
    args = parser.parse_args()

    ts_codes = []
    if args.ts_codes:
        ts_codes.extend(normalize_ts_codes(args.ts_codes))
    if args.ts_code:
        ts_codes.extend(normalize_ts_codes(args.ts_code))

    if not ts_codes:
        raise ValueError("至少传入 --ts-code 或 --ts-codes")

    if len(ts_codes) == 1:
        ts_code = ts_codes[0]
        ok = update_signal_status(
            signal_date=args.date,
            ts_code=ts_code,
            new_status=args.to,
            expected_current_status=args.from_status,
            operator=args.operator,
            note=args.note,
        )

        if ok:
            print(f"状态更新成功: {args.date} {ts_code} -> {args.to}")
        else:
            print(f"状态更新失败: {args.date} {ts_code} -> {args.to}")
        return

    summary = update_signal_status_batch(
        signal_date=args.date,
        ts_codes=ts_codes,
        new_status=args.to,
        expected_current_status=args.from_status,
        operator=args.operator,
        note=args.note,
    )

    print(
        f"批量状态更新完成: total={summary['total']}, "
        f"success={summary['success_count']}, failed={summary['failed_count']}"
    )
    if summary["failed_ts_codes"]:
        print("失败列表:", ",".join(summary["failed_ts_codes"]))
        reason_counts = summary.get("failed_reason_counts", {})
        if reason_counts:
            pretty = ", ".join([f"{k}:{v}" for k, v in reason_counts.items()])
            print("失败原因统计:", pretty)


if __name__ == "__main__":
    main()
