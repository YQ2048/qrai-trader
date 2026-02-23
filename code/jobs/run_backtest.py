import argparse
import json
from pathlib import Path

from code.backtest.engine import (
    DEFAULT_BACKTEST_STRATEGIES,
    persist_backtest_signals,
    run_min_backtest,
    summarize_backtest_by_industry_and_strategy,
    summarize_backtest_by_market_and_strategy,
    summarize_backtest_by_strategy,
)
from code.backtest.report_generator import generate_context_comparison_plots, generate_strategy_comparison_plots


def _apply_param_overrides(overrides: dict) -> None:
    """将 overrides 中的键值临时注入 QUANT_THRESHOLDS（进程隔离，不影响其他 Job）"""
    if not overrides:
        return
    from code.core import config as _cfg
    for k, v in overrides.items():
        if k in _cfg.QUANT_THRESHOLDS:
            orig_type = type(_cfg.QUANT_THRESHOLDS[k])
            _cfg.QUANT_THRESHOLDS[k] = orig_type(v)
            print(f"  [param_override] QUANT_THRESHOLDS[{k!r}] = {_cfg.QUANT_THRESHOLDS[k]} (原值已覆盖)")
        else:
            print(f"  [param_override] 警告: {k!r} 不在 QUANT_THRESHOLDS 中，已跳过")


def main():
    parser = argparse.ArgumentParser(description="最小回测入口（支持 S21/S31/S32/S33/S34）")
    parser.add_argument("--start", required=True, help="开始日期 YYYYMMDD")
    parser.add_argument("--end", required=True, help="结束日期 YYYYMMDD")
    parser.add_argument(
        "--strategy",
        default=",".join(DEFAULT_BACKTEST_STRATEGIES),
        help="策略列表，逗号分隔，如 S21,S31。留空=全部策略",
    )
    parser.add_argument(
        "--params",
        type=str,
        default="{}",
        help='超参数覆盖，JSON 格式，如 \'{"g_point_vol_ratio": 2.5, "rps_20_min": 88}\''
             "。留空=使用 config.py 默认值",
    )
    parser.add_argument("--no-persist", action="store_true", help="仅导出 CSV，不写入 backtest_signals")
    args = parser.parse_args()

    # 解析并注入超参数覆盖（在构建任何策略对象之前）
    try:
        overrides = json.loads(args.params) if args.params.strip() not in ("", "{}") else {}
    except json.JSONDecodeError as e:
        raise ValueError(f"--params 必须是合法 JSON，当前值: {args.params!r}，错误: {e}")
    _apply_param_overrides(overrides)

    strategy_ids = [s.strip() for s in args.strategy.split(",") if s.strip()]

    result = run_min_backtest(start_date=args.start, end_date=args.end, strategy_ids=strategy_ids)
    if result.empty:
        print("无可用回测结果")
        return

    project_root = Path(__file__).resolve().parents[2]
    output_dir = project_root / "report"
    output_dir.mkdir(parents=True, exist_ok=True)

    # 文件名中嵌入策略标签 + 超参标签（方便多次运行后对比）
    strategy_tag = "-".join(strategy_ids)
    param_tag = "_".join(f"{k}{v}" for k, v in overrides.items()) if overrides else ""
    run_tag = f"{strategy_tag}{'_' + param_tag if param_tag else ''}_{args.start}_{args.end}"

    output_file         = output_dir / f"backtest_{run_tag}.csv"
    summary_file        = output_dir / f"backtest_summary_{run_tag}.csv"
    summary_market_file = output_dir / f"backtest_summary_market_{run_tag}.csv"
    summary_industry_file = output_dir / f"backtest_summary_industry_{run_tag}.csv"

    result.to_csv(output_file, index=False, encoding="utf-8-sig")
    summary = summarize_backtest_by_strategy(result)
    summary_market = summarize_backtest_by_market_and_strategy(result)
    summary_industry = summarize_backtest_by_industry_and_strategy(result)
    summary.to_csv(summary_file, index=False, encoding="utf-8-sig")
    summary_market.to_csv(summary_market_file, index=False, encoding="utf-8-sig")
    summary_industry.to_csv(summary_industry_file, index=False, encoding="utf-8-sig")

    plot_files = generate_strategy_comparison_plots(summary, output_dir=output_dir, tag=run_tag)
    context_plot_files = generate_context_comparison_plots(
        summary_market,
        summary_industry,
        output_dir=output_dir,
        tag=run_tag,
    )
    all_plot_files = plot_files + context_plot_files

    persisted = 0
    if not args.no_persist:
        persisted = persist_backtest_signals(
            result,
            param_snapshot={
                "start_date": args.start,
                "end_date": args.end,
                "strategy_ids": strategy_ids,
                "param_overrides": overrides,
                "returns_horizon": [1, 3, 5, 10, 20],
            },
        )

    print(f"回测完成: {len(result)} 条信号  策略={strategy_ids}  超参={overrides or '默认'}")
    if not args.no_persist:
        print(f"落库完成: {persisted} 条 backtest_signals")
    print(f"输出文件: {output_file}")
    print(f"汇总文件: {summary_file}")
    print(f"市场分组汇总: {summary_market_file}")
    print(f"行业分组汇总: {summary_industry_file}")
    if all_plot_files:
        print("可视化文件:")
        for path in all_plot_files:
            print(f"  - {path}")
    else:
        print("可视化文件: 未生成（缺少 matplotlib 或汇总为空）")


if __name__ == "__main__":
    main()
