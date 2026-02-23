from pathlib import Path
from typing import Dict, List

import pandas as pd


def _format_signal_text(signals) -> str:
    return " + ".join([f"{s.strategy_id} {s.strategy_name}" for s in signals])


def _signal_labels(signals) -> List[str]:
    return [f"{s.strategy_id} {s.strategy_name}" for s in signals]


def _format_factor_text(factor_snapshot: Dict) -> str:
    calendar_bias = factor_snapshot.get("calendar_bias", "NEUTRAL")
    return (
        f"政策主题命中={factor_snapshot.get('policy_theme_hit', 0):.0f}, "
        f"政策催化窗口={factor_snapshot.get('policy_catalyst_active', 0):.0f}, "
        f"日历偏好={calendar_bias}, "
        f"业绩类型={factor_snapshot.get('forecast_type', '')}, "
        f"业绩负向标记={factor_snapshot.get('earnings_negative_flag', 0):.0f}, "
        f"业绩预增标记={factor_snapshot.get('earnings_preincrease_flag', 0):.0f}, "
        f"PEG={factor_snapshot.get('peg', 0):.2f}, "
        f"PS_TTM={factor_snapshot.get('ps_ttm', 0):.2f}, "
        f"RPS20={factor_snapshot.get('rps_20', 0):.1f}, "
        f"RPS60={factor_snapshot.get('rps_60', 0):.1f}, "
        f"获利盘={factor_snapshot.get('winner_rate', 0):.1f}%, "
        f"筹码峰CV10={factor_snapshot.get('chip_peak_cv_10', 0):.3f}, "
        f"机构席位数={factor_snapshot.get('inst_count', 0):.0f}, "
        f"机构净买入={factor_snapshot.get('inst_net_buy', 0) / 1e8:.2f}亿, "
        f"北向连续净流入={factor_snapshot.get('north_consecutive_days', 0):.0f}天, "
        f"北向3日净流入={factor_snapshot.get('north_sum_3d', 0):.1f}亿, "
        f"板块RPS斜率3d={factor_snapshot.get('industry_rps_slope_3d', 0):.2f}, "
        f"G点强度={factor_snapshot.get('g_point_strength', 0):.2f}, "
        f"L型缩量比={factor_snapshot.get('l_shape_shrink_ratio', 0):.2f}, "
        f"三线离散度={factor_snapshot.get('ma_dispersion', 0):.3f}, "
        f"指路上影={factor_snapshot.get('guide_shadow_pct', 0):.3f}, "
        f"反包标记={factor_snapshot.get('reversal_flag', 0):.0f}, "
        f"假破位深度={factor_snapshot.get('false_break_depth', 0):.3f}, "
        f"618偏离={factor_snapshot.get('distance_to_618', 0):.3f}, "
        f"换手率={factor_snapshot.get('turnover_rate', 0):.2f}%"
    )


def _build_suggestion_text(candidate: Dict) -> str:
    factor = candidate.get("factor_snapshot", {})
    s11_hit = factor.get("policy_theme_hit", 0)
    s12_bias = factor.get("calendar_bias", "NEUTRAL")
    s21 = factor.get("g_point_strength", 0)
    s22 = factor.get("l_shape_shrink_ratio", 1)
    s24 = factor.get("reversal_flag", 0)
    s25 = factor.get("false_break_depth", 0)
    s26 = factor.get("distance_to_618", 1)
    s31 = factor.get("rps_20", 0)
    s32 = factor.get("winner_rate", 0)
    s34 = factor.get("north_consecutive_days", 0)

    tags = []
    if s11_hit >= 1:
        tags.append("政策主线命中，优先观察板块联动与持续性")
    if s12_bias == "AGGRESSIVE":
        tags.append("当月偏进攻，可优先趋势延续品种")
    elif s12_bias == "DEFENSIVE":
        tags.append("当月偏防守，仓位与止损应更保守")
    elif s12_bias == "VALUE":
        tags.append("当月偏价值，可关注低估值回归节奏")
    if s21 >= 3:
        tags.append("放量突破后回踩不破可分批关注")
    if s22 <= 0.3:
        tags.append("缩量结构完整，优先低吸不追高")
    if s24 >= 1:
        tags.append("反包确认出现，可关注次日开盘承接强度")
    if s25 >= 0.01:
        tags.append("假破位回收成立，关注均线之上企稳机会")
    if s26 <= 0.01:
        tags.append("接近黄金分割低吸位，宜分批介入控制追涨")
    if s31 >= 90 or s32 >= 90:
        tags.append("强势与锁仓并存，仓位可向中线倾斜")
    if s34 >= 3:
        tags.append("北向共振期可适当延长持有观察窗口")
    if not tags:
        tags.append("以风控优先，等待更强确认信号")

    if "SELL_ON_NEWS" in candidate.get("risk_tags", []):
        tags.append("业绩预增叠加短期涨幅过大，警惕利好兑现风险")

    tags.append("跌破天际线止损位执行离场")
    return "；".join(tags)


def _build_confidence_tags(candidate: Dict) -> List[str]:
    factor = candidate.get("factor_snapshot", {})
    confidence: List[str] = []

    amplitude_5d = factor.get("amplitude_5d", 0)
    if amplitude_5d and amplitude_5d < 0.02:
        confidence.append("[⭐ 弱活跃度]")

    shrink_ratio = factor.get("l_shape_shrink_ratio", 1)
    atr_trend = factor.get("atr_14_trend_5", 0)
    if shrink_ratio <= 0.5:
        if atr_trend < 0 and amplitude_5d < 0.025:
            confidence.append("[⭐⭐⭐ 真洗盘]")
        elif amplitude_5d >= 0.025:
            confidence.append("[⭐ 警惕阴跌]")

    pe_ttm = factor.get("pe_ttm", 0)
    peg = factor.get("peg", 0)
    ps_ttm = factor.get("ps_ttm", 0)
    if pe_ttm and pe_ttm <= 25:
        confidence.append("[⭐⭐ 估值相对友好]")
    elif pe_ttm and pe_ttm >= 55:
        confidence.append("[⭐ 估值压力偏高]")

    if peg and peg > 0:
        if peg < 1.5:
            confidence.append("[⭐⭐⭐ 高成长可信]")
        elif peg >= 3.0:
            confidence.append("[⭐ 高估值风险]")

    if ps_ttm and ps_ttm > 0:
        if ps_ttm < 2.0:
            confidence.append("[⭐⭐ PS估值友好]")
        elif ps_ttm >= 8.0:
            confidence.append("[⭐ PS估值偏高]")

    if factor.get("policy_theme_hit", 0) >= 1 and pe_ttm and pe_ttm <= 35:
        confidence.append("[⭐⭐⭐ 主线+估值匹配]")

    return confidence


def build_reason_payload(candidate: Dict) -> Dict:
    signal_text = _format_signal_text(candidate["signals"])
    factor_text = _format_factor_text(candidate["factor_snapshot"])
    risk_text = ", ".join(candidate["risk_tags"]) if candidate["risk_tags"] else "无明显硬风险"
    suggestion = _build_suggestion_text(candidate)
    confidence_tags = _build_confidence_tags(candidate)

    return {
        "triggered_strategies": _signal_labels(candidate["signals"]),
        "core_factors": factor_text,
        "risk_tags": candidate.get("risk_tags", []),
        "confidence_tags": confidence_tags,
        "suggestion": suggestion,
        "text": (
            f"触发策略: {signal_text} | "
            f"核心因子: {factor_text} | "
            f"置信度: {','.join(confidence_tags) if confidence_tags else '无'} | "
            f"风险标签: {risk_text} | "
            f"操作建议: {suggestion}"
        ),
    }


def build_reason_text(candidate: Dict) -> str:
    return build_reason_payload(candidate)["text"]


def write_daily_outputs(
    ranked_candidates: List[Dict],
    trade_date: str,
    output_dir: Path,
    top_n: int = 15,
    market_overview: str = "",
) -> Dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    top_candidates = ranked_candidates[:top_n]
    md_path = output_dir / f"candidates_{trade_date}.md"
    csv_path = output_dir / f"factor_snapshot_{trade_date}.csv"

    lines = [
        f"# 候选清单（{trade_date}）",
        "",
        f"共输出 {len(top_candidates)} 只候选。",
        "",
    ]

    if market_overview:
        lines.extend([
            "## 大盘概况",
            f"- {market_overview}",
            "",
        ])

    for idx, candidate in enumerate(top_candidates, start=1):
        score_detail = candidate["score_detail"]
        reason = build_reason_payload(candidate)
        lines.extend(
            [
                f"## {idx}. {candidate['ts_code']} {candidate['name']}（{candidate['industry']}）",
                f"- 综合得分: **{candidate['composite_score']}**",
                (
                    f"- 四维得分: 天={score_detail['macro']:.2f}, "
                    f"地={score_detail['pattern']:.2f}, "
                    f"人={score_detail['sentiment']:.2f}, "
                    f"弈={score_detail['intraday']:.2f}"
                ),
                f"- 触发策略: {' + '.join(reason['triggered_strategies'])}",
                f"- 核心因子: {reason['core_factors']}",
                f"- 置信度标签: {','.join(reason['confidence_tags']) if reason['confidence_tags'] else '无'}",
                f"- 风险标签: {','.join(reason['risk_tags']) if reason['risk_tags'] else '无明显硬风险'}",
                f"- 操作建议: {reason['suggestion']}",
                "",
            ]
        )

    md_path.write_text("\n".join(lines), encoding="utf-8")

    records = []
    for candidate in top_candidates:
        row = {
            "trade_date": candidate["trade_date"],
            "ts_code": candidate["ts_code"],
            "name": candidate["name"],
            "industry": candidate["industry"],
            "composite_score": candidate["composite_score"],
            **candidate["score_detail"],
            **candidate["factor_snapshot"],
            "confidence_tags": ",".join(reason["confidence_tags"]),
            "risk_tags": ",".join(candidate["risk_tags"]),
            "triggered_strategies": _format_signal_text(candidate["signals"]),
        }
        records.append(row)

    pd.DataFrame(records).to_csv(csv_path, index=False, encoding="utf-8-sig")
    return {"markdown": md_path, "csv": csv_path}
