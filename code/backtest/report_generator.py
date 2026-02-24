from pathlib import Path
from typing import Dict, List

import pandas as pd

from code.core.logger import get_logger

logger = get_logger(__name__)


def build_plot_metric_frame(summary_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df is None or summary_df.empty:
        return pd.DataFrame(columns=["strategy_id", "avg_return_t20", "win_rate_t20", "sharpe_approx_t20", "calmar_approx_t20"])

    cols = ["strategy_id", "avg_return_t20", "win_rate_t20", "sharpe_approx_t20", "calmar_approx_t20"]
    out = summary_df.copy()
    for col in cols:
        if col not in out.columns:
            out[col] = 0.0 if col != "strategy_id" else "UNKNOWN"

    out = out[cols].copy()
    numeric_cols = [c for c in cols if c != "strategy_id"]
    out[numeric_cols] = out[numeric_cols].fillna(0.0)
    return out


def generate_strategy_comparison_plots(summary_df: pd.DataFrame, output_dir: Path, tag: str) -> List[Path]:
    metrics = build_plot_metric_frame(summary_df)
    if metrics.empty:
        return []

    try:
        import matplotlib
        import matplotlib.font_manager as fm
        import matplotlib.pyplot as plt
        # 尝试使用支持 CJK 的字体（GitHub Actions 需预装 fonts-noto-cjk）
        # 优先精确匹配，其次模糊匹配包含 Noto+CJK 或 SimHei 等关键词的字体
        _cjk_priority = ["Noto Sans CJK SC", "Noto Sans CJK", "WenQuanYi Micro Hei",
                         "Source Han Sans CN", "SimHei", "Microsoft YaHei"]
        _avail = {f.name for f in fm.fontManager.ttflist}
        _chosen = next((f for f in _cjk_priority if f in _avail), None)
        if _chosen is None:
            # 模糊匹配：搜索字体名中含 CJK/Noto/Hei 的字体
            _chosen = next(
                (f.name for f in fm.fontManager.ttflist
                 if any(kw in f.name for kw in ("CJK", "Noto Sans SC", "SimHei", "WenQuanYi", "Source Han"))),
                None,
            )
        if _chosen:
            matplotlib.rcParams["font.sans-serif"] = [_chosen, "DejaVu Sans"]
            matplotlib.rcParams["axes.unicode_minus"] = False
            logger.info("[report] matplotlib CJK font: %s", _chosen)
        else:
            import warnings
            warnings.filterwarnings("ignore", message="Glyph.*missing from font", category=UserWarning)
            logger.warning("[report] 未找到 CJK 字体，图表中文将显示为方框")
    except Exception as e:
        logger.warning("matplotlib 不可用，跳过策略对比图表生成: %s", e)
        return []

    output_dir.mkdir(parents=True, exist_ok=True)
    files: List[Path] = []

    chart_specs: Dict[str, str] = {
        "avg_return_t20": "策略平均收益（T+20）",
        "win_rate_t20": "策略胜率（T+20）",
        "sharpe_approx_t20": "策略 Sharpe 近似（T+20）",
        "calmar_approx_t20": "策略 Calmar 近似（T+20）",
    }

    for metric, title in chart_specs.items():
        fig, ax = plt.subplots(figsize=(8, 4.5))
        bars = ax.bar(metrics["strategy_id"], metrics[metric], color="#4F81BD")
        ax.set_title(title)
        ax.set_xlabel("Strategy")
        ax.set_ylabel(metric)
        ax.grid(axis="y", alpha=0.25)

        for bar in bars:
            height = bar.get_height()
            ax.annotate(
                f"{height:.3f}",
                xy=(bar.get_x() + bar.get_width() / 2, height),
                xytext=(0, 3),
                textcoords="offset points",
                ha="center",
                va="bottom",
                fontsize=8,
            )

        file_path = output_dir / f"backtest_plot_{metric}_{tag}.png"
        fig.tight_layout()
        fig.savefig(file_path, dpi=150)
        plt.close(fig)
        files.append(file_path)

    return files


def _build_market_plot_frame(summary_market_df: pd.DataFrame) -> pd.DataFrame:
    if summary_market_df is None or summary_market_df.empty:
        return pd.DataFrame(columns=["strategy_id", "market_state", "avg_return_t20", "win_rate_t20", "signals_count"])

    cols = ["strategy_id", "market_state", "avg_return_t20", "win_rate_t20", "signals_count"]
    out = summary_market_df.copy()
    for col in cols:
        if col not in out.columns:
            out[col] = 0.0 if col not in {"strategy_id", "market_state"} else "UNKNOWN"
    out = out[cols].copy()
    out[["avg_return_t20", "win_rate_t20", "signals_count"]] = out[["avg_return_t20", "win_rate_t20", "signals_count"]].fillna(0.0)
    out["market_state"] = out["market_state"].fillna("UNKNOWN")
    return out


def _build_industry_plot_frame(summary_industry_df: pd.DataFrame, top_n: int = 8) -> pd.DataFrame:
    if summary_industry_df is None or summary_industry_df.empty:
        return pd.DataFrame(columns=["strategy_id", "industry", "avg_return_t20", "signals_count"])

    cols = ["strategy_id", "industry", "avg_return_t20", "signals_count"]
    out = summary_industry_df.copy()
    for col in cols:
        if col not in out.columns:
            out[col] = 0.0 if col not in {"strategy_id", "industry"} else "UNKNOWN"

    out = out[cols].copy()
    out[["avg_return_t20", "signals_count"]] = out[["avg_return_t20", "signals_count"]].fillna(0.0)
    out["industry"] = out["industry"].fillna("UNKNOWN")

    top_industries = (
        out.groupby("industry", as_index=False)["signals_count"]
        .sum()
        .sort_values("signals_count", ascending=False)
        .head(top_n)["industry"]
        .tolist()
    )
    return out[out["industry"].isin(top_industries)].copy()


def generate_context_comparison_plots(
    summary_market_df: pd.DataFrame,
    summary_industry_df: pd.DataFrame,
    output_dir: Path,
    tag: str,
) -> List[Path]:
    market = _build_market_plot_frame(summary_market_df)
    industry = _build_industry_plot_frame(summary_industry_df)

    if market.empty and industry.empty:
        return []

    try:
        import matplotlib
        import matplotlib.font_manager as fm
        import matplotlib.pyplot as plt
        _cjk_priority = ["Noto Sans CJK SC", "Noto Sans CJK", "WenQuanYi Micro Hei",
                         "Source Han Sans CN", "SimHei", "Microsoft YaHei"]
        _avail = {f.name for f in fm.fontManager.ttflist}
        _chosen = next((f for f in _cjk_priority if f in _avail), None)
        if _chosen is None:
            _chosen = next(
                (f.name for f in fm.fontManager.ttflist
                 if any(kw in f.name for kw in ("CJK", "Noto Sans SC", "SimHei", "WenQuanYi", "Source Han"))),
                None,
            )
        if _chosen:
            matplotlib.rcParams["font.sans-serif"] = [_chosen, "DejaVu Sans"]
            matplotlib.rcParams["axes.unicode_minus"] = False
            logger.info("[report] matplotlib CJK font (context): %s", _chosen)
        else:
            import warnings
            warnings.filterwarnings("ignore", message="Glyph.*missing from font", category=UserWarning)
            logger.warning("[report] 未找到 CJK 字体，上下文图表中文将显示为方框")
    except Exception as e:
        logger.warning("matplotlib 不可用，跳过上下文图表生成: %s", e)
        return []

    output_dir.mkdir(parents=True, exist_ok=True)
    files: List[Path] = []

    if not market.empty:
        pivot_market = market.pivot(index="strategy_id", columns="market_state", values="avg_return_t20").fillna(0.0)
        ax = pivot_market.plot(kind="bar", figsize=(9, 5))
        ax.set_title("策略-市场状态平均收益（T+20）")
        ax.set_xlabel("Strategy")
        ax.set_ylabel("avg_return_t20")
        ax.grid(axis="y", alpha=0.25)
        fig = ax.get_figure()
        market_file = output_dir / f"backtest_plot_market_state_{tag}.png"
        fig.tight_layout()
        fig.savefig(market_file, dpi=150)
        plt.close(fig)
        files.append(market_file)

    if not industry.empty:
        pivot_industry = industry.pivot_table(
            index="industry",
            columns="strategy_id",
            values="avg_return_t20",
            aggfunc="mean",
        ).fillna(0.0)
        ax = pivot_industry.plot(kind="bar", figsize=(10, 5))
        ax.set_title("行业-策略平均收益（T+20，Top行业）")
        ax.set_xlabel("Industry")
        ax.set_ylabel("avg_return_t20")
        ax.grid(axis="y", alpha=0.25)
        fig = ax.get_figure()
        industry_file = output_dir / f"backtest_plot_industry_{tag}.png"
        fig.tight_layout()
        fig.savefig(industry_file, dpi=150)
        plt.close(fig)
        files.append(industry_file)

    return files
