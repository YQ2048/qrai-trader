from pathlib import Path
from typing import Dict, List
from datetime import datetime, timezone, timedelta

_CST = timezone(timedelta(hours=8))  # 北京时间 UTC+8

import requests

from code.analytics.reason_generator import build_reason_payload
from code.core.config import NOTION_PARENT_PAGE_ID, NOTION_TOKEN


NOTION_API_BASE = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"
_NOTION_BATCH_SIZE = 100  # Notion API 单次最多 100 个 block


def _notion_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": NOTION_VERSION,
    }


def _notion_request(method: str, endpoint: str, payload: Dict | None = None) -> Dict[str, object]:
    try:
        if method.upper() == "GET":
            response = requests.get(
                f"{NOTION_API_BASE}/{endpoint}",
                headers=_notion_headers(),
                timeout=30,
            )
        elif method.upper() == "POST":
            response = requests.post(
                f"{NOTION_API_BASE}/{endpoint}",
                headers=_notion_headers(),
                json=payload,
                timeout=30,
            )
        elif method.upper() == "PATCH":
            response = requests.patch(
                f"{NOTION_API_BASE}/{endpoint}",
                headers=_notion_headers(),
                json=payload,
                timeout=30,
            )
        elif method.upper() == "DELETE":
            response = requests.delete(
                f"{NOTION_API_BASE}/{endpoint}",
                headers=_notion_headers(),
                timeout=30,
            )
        else:
            return {"success": False, "error": f"Unsupported method: {method}"}

        if response.status_code >= 400:
            return {"success": False, "error": response.text}
        return {"success": True, "data": response.json()}
    except requests.RequestException as exc:
        return {"success": False, "error": str(exc)}


def _extract_page_title(page_obj: Dict) -> str:
    if page_obj.get("type") == "child_page":
        return page_obj.get("child_page", {}).get("title", "")

    properties = page_obj.get("properties", {})
    title_prop = properties.get("title", {})
    title_list = title_prop.get("title", [])
    if title_list:
        return title_list[0].get("plain_text", "")
    return ""


def _list_child_pages(parent_page_id: str) -> Dict[str, object]:
    """获取子页面列表（支持分页，Notion API 单次最多返回 100 个）"""
    pages = []
    start_cursor = None

    while True:
        endpoint = f"blocks/{parent_page_id}/children"
        if start_cursor:
            endpoint += f"?start_cursor={start_cursor}"
        result = _notion_request("GET", endpoint)
        if not result.get("success"):
            return {"success": False, "error": result.get("error")}

        data = result.get("data", {})
        for item in data.get("results", []):
            if item.get("type") == "child_page":
                pages.append({
                    "id": item.get("id"),
                    "title": _extract_page_title(item),
                    "raw": item,
                })

        if not data.get("has_more"):
            break
        start_cursor = data.get("next_cursor")
        if not start_cursor:
            break

    return {"success": True, "pages": pages}


def _list_child_blocks(parent_block_id: str) -> Dict[str, object]:
    """获取子块列表（支持分页）"""
    all_blocks = []
    start_cursor = None

    while True:
        endpoint = f"blocks/{parent_block_id}/children"
        if start_cursor:
            endpoint += f"?start_cursor={start_cursor}"
        result = _notion_request("GET", endpoint)
        if not result.get("success"):
            return {"success": False, "error": result.get("error")}

        data = result.get("data", {})
        all_blocks.extend(data.get("results", []))

        if not data.get("has_more"):
            break
        start_cursor = data.get("next_cursor")
        if not start_cursor:
            break

    return {"success": True, "blocks": all_blocks}


def _find_child_page_by_title(parent_page_id: str, title: str) -> Dict[str, object]:
    listed = _list_child_pages(parent_page_id)
    if not listed.get("success"):
        return listed

    for page in listed.get("pages", []):
        if page.get("title") == title:
            return {"success": True, "found": True, "page": page}
    return {"success": True, "found": False}


def _create_child_page(parent_page_id: str, title: str) -> Dict[str, object]:
    """创建子页面（不含 children，后续通过批量追加写入内容）"""
    payload = {
        "parent": {"page_id": parent_page_id},
        "properties": {
            "title": {
                "title": [{"type": "text", "text": {"content": title}}]
            }
        },
    }
    result = _notion_request("POST", "pages", payload)
    if not result.get("success"):
        return result
    data = result.get("data", {})
    return {
        "success": True,
        "page": {
            "id": data.get("id"),
            "title": title,
            "url": data.get("url"),
        },
    }


def _find_or_create_child_page(parent_page_id: str, title: str) -> Dict[str, object]:
    found = _find_child_page_by_title(parent_page_id, title)
    if not found.get("success"):
        return found
    if found.get("found"):
        return {"success": True, "page": found.get("page"), "created": False}

    created = _create_child_page(parent_page_id, title)
    if not created.get("success"):
        return created
    return {"success": True, "page": created.get("page"), "created": True}


def _append_blocks_to_page(page_id: str, blocks: List[Dict]) -> Dict[str, object]:
    if not blocks:
        return {"success": True}
    result = _notion_request("PATCH", f"blocks/{page_id}/children", {"children": blocks})
    if not result.get("success"):
        return result
    return {"success": True}


def _append_blocks_in_batches(page_id: str, blocks: List[Dict], batch_size: int = _NOTION_BATCH_SIZE) -> Dict[str, object]:
    """分批追加块，规避 Notion API 单次最多 100 个的限制"""
    for i in range(0, len(blocks), batch_size):
        batch = blocks[i:i + batch_size]
        result = _append_blocks_to_page(page_id, batch)
        if not result.get("success"):
            return result
    return {"success": True}


def _clear_page_blocks(page_id: str) -> Dict[str, object]:
    listed = _list_child_blocks(page_id)
    if not listed.get("success"):
        return listed

    for block in listed.get("blocks", []):
        block_id = block.get("id")
        if not block_id:
            continue
        deleted = _notion_request("DELETE", f"blocks/{block_id}")
        if not deleted.get("success"):
            return deleted
    return {"success": True}


def _build_date_hierarchy_titles(trade_date: str) -> Dict[str, str]:
    dt = datetime.strptime(trade_date, "%Y%m%d")
    return {
        "year": dt.strftime("%Y"),
        "month": dt.strftime("%Y-%m"),
        "day": dt.strftime("%Y-%m-%d"),
    }


def _ensure_date_hierarchy(parent_page_id: str, trade_date: str) -> Dict[str, object]:
    titles = _build_date_hierarchy_titles(trade_date)

    year_page = _find_or_create_child_page(parent_page_id, titles["year"])
    if not year_page.get("success"):
        return year_page

    month_page = _find_or_create_child_page(year_page["page"]["id"], titles["month"])
    if not month_page.get("success"):
        return month_page

    day_page = _find_or_create_child_page(month_page["page"]["id"], titles["day"])
    if not day_page.get("success"):
        return day_page

    return {
        "success": True,
        "year_page": year_page["page"],
        "month_page": month_page["page"],
        "day_page": day_page["page"],
    }


def _chunk_text(text: str, limit: int = 1800) -> List[str]:
    if not text:
        return [""]
    chunks: List[str] = []
    start = 0
    while start < len(text):
        chunks.append(text[start:start + limit])
        start += limit
    return chunks


def _rich_text(text: str) -> List[Dict]:
    chunks = _chunk_text(text)
    return [{"type": "text", "text": {"content": chunk}} for chunk in chunks]


def _paragraph(text: str) -> Dict:
    return {"object": "block", "type": "paragraph", "paragraph": {"rich_text": _rich_text(text)}}


def _heading2(text: str) -> Dict:
    return {"object": "block", "type": "heading_2", "heading_2": {"rich_text": _rich_text(text)}}


def _divider() -> Dict:
    return {"object": "block", "type": "divider", "divider": {}}


def _build_candidate_blocks(idx: int, candidate: Dict) -> List[Dict]:
    """为单只候选股构建结构化 Notion blocks（8 blocks per stock）。

    布局：标题 | 得分+策略+置信+风险 | [天] | [地] | [人] | 建议 | 分隔线
    """
    ts_code = candidate.get("ts_code", "")
    name = candidate.get("name", "")
    industry = candidate.get("industry", "")
    score = candidate.get("composite_score", 0)
    base_score = candidate.get("base_composite_score", score)
    multiplier = candidate.get("score_multiplier", 1.0)
    sd = candidate.get("score_detail", {})
    f = candidate.get("factor_snapshot", {})
    signals = candidate.get("signals", [])
    risk_tags = candidate.get("risk_tags", [])

    # 触发策略、置信度、建议
    reason = build_reason_payload(candidate)
    strategy_str = " + ".join(reason["triggered_strategies"]) if reason["triggered_strategies"] else "无"
    confidence_str = " ".join(reason["confidence_tags"]) if reason["confidence_tags"] else "无"
    risk_str = " / ".join(risk_tags) if risk_tags else "无明显硬风险"
    suggestion = reason["suggestion"]

    # ── 得分行 ──
    if multiplier < 1.0:
        score_line = (
            f"综合: {score:.2f}（原始 {base_score:.2f} × {multiplier:.2f} 熔断折减）"
            f"  |  天={sd.get('macro', 0):.1f}  地={sd.get('pattern', 0):.1f}"
            f"  人={sd.get('sentiment', 0):.1f}  弈={sd.get('intraday', 0):.1f}"
        )
    else:
        score_line = (
            f"综合: {score:.2f}"
            f"  |  天={sd.get('macro', 0):.1f}  地={sd.get('pattern', 0):.1f}"
            f"  人={sd.get('sentiment', 0):.1f}  弈={sd.get('intraday', 0):.1f}"
        )

    # ── [天] 宏观/政策维因子 ──
    policy_hit = f.get("policy_theme_hit", 0) or 0
    catalyst = f.get("policy_catalyst_active", 0) or 0
    calendar_bias = f.get("calendar_bias", "NEUTRAL") or "NEUTRAL"
    forecast_type = f.get("forecast_type", "") or "无"
    earnings_neg = f.get("earnings_negative_flag", 0) or 0
    pe_ttm = f.get("pe_ttm", 0) or 0
    peg = f.get("peg", 0) or 0
    ps_ttm = f.get("ps_ttm", 0) or 0

    sky_line = (
        f"[天]  政策主题={'命中✓' if policy_hit >= 1 else '未命中'}"
        f"  |  催化窗口={'是' if catalyst >= 1 else '否'}"
        f"  |  日历={calendar_bias}"
        f"  |  业绩={forecast_type}{'⚠负向' if earnings_neg >= 1 else ''}"
        f"  |  PE={pe_ttm:.1f}  PEG={peg:.2f}  PS={ps_ttm:.2f}"
    )

    # ── [地] 形态/量价维因子 ──
    g_pt = f.get("g_point_strength", 0) or 0
    l_shape = f.get("l_shape_shrink_ratio", 0) or 0
    ma_disp = (f.get("ma_dispersion", 0) or 0) * 100
    guide_shadow = (f.get("guide_shadow_pct", 0) or 0) * 100
    reversal = f.get("reversal_flag", 0) or 0
    false_break = (f.get("false_break_depth", 0) or 0) * 100
    dist_618 = (f.get("distance_to_618", 0) or 0) * 100
    turnover = f.get("turnover_rate", 0) or 0
    amplitude_5d = (f.get("amplitude_5d", 0) or 0) * 100

    earth_line = (
        f"[地]  G点强度={g_pt:.2f}"
        f"  |  L型缩量={l_shape:.2f}"
        f"  |  三线离散={ma_disp:.2f}%"
        f"  |  指路上影={guide_shadow:.2f}%"
        f"  |  反包={'是✓' if reversal >= 1 else '否'}"
        f"  |  假破深度={false_break:.2f}%"
        f"  |  618偏离={dist_618:.2f}%"
        f"  |  换手率={turnover:.2f}%"
        f"  |  5日振幅={amplitude_5d:.2f}%"
    )

    # ── [人] 资金/筹码维因子 ──
    rps20 = f.get("rps_20", 0) or 0
    rps60 = f.get("rps_60", 0) or 0
    winner_rate = f.get("winner_rate", 0) or 0
    chip_cv = f.get("chip_peak_cv_10", 0) or 0
    inst_count = f.get("inst_count", 0) or 0
    inst_net = (f.get("inst_net_buy", 0) or 0) / 1e8
    north_days = f.get("north_consecutive_days", 0) or 0
    north_3d = f.get("north_sum_3d", 0) or 0
    ind_rps_slope = f.get("industry_rps_slope_3d", 0) or 0

    human_line = (
        f"[人]  RPS20={rps20:.1f}  RPS60={rps60:.1f}"
        f"  |  获利盘={winner_rate:.1f}%  筹码CV={chip_cv:.3f}"
        f"  |  机构={inst_count:.0f}席 / 净买={inst_net:.2f}亿"
        f"  |  北向={north_days:.0f}天连续 / 3日={north_3d:.1f}亿"
        f"  |  板块RPS斜率={ind_rps_slope:.2f}"
    )

    return [
        _heading2(f"{idx}. {ts_code}  {name}（{industry}）"),
        _paragraph(
            f"{score_line}\n"
            f"触发策略: {strategy_str}\n"
            f"置信度: {confidence_str}  |  风险: {risk_str}"
        ),
        _paragraph(sky_line),
        _paragraph(earth_line),
        _paragraph(human_line),
        _paragraph(f"建议: {suggestion}"),
        _divider(),
    ]


def _build_report_blocks(
    trade_date: str,
    market_overview: str,
    markdown_path: Path,
    csv_path: Path,
    candidates: List[Dict] | None = None,
    top_n: int = 15,
) -> List[Dict]:
    """组装完整日报 Notion block 列表。

    有结构化 candidates 时构建分维度详情；否则降级解析 Markdown。
    """
    blocks: List[Dict] = []

    # ── 页头 ──
    blocks.append(_paragraph(f"交易日: {trade_date}  |  生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}"))
    blocks.append(_paragraph(f"大盘概况: {market_overview}"))
    blocks.append(_paragraph(f"本地报告: {markdown_path.name}  /  {csv_path.name}"))

    if candidates:
        top = candidates[:top_n]

        # 熔断器状态（取第一个候选的 market_risk）
        market_risk = top[0].get("market_risk", {}) if top else {}
        if market_risk.get("is_meltdown"):
            blocks.append(_paragraph(
                f"⚠ 全局熔断器触发  |  折减系数={market_risk.get('score_multiplier', 1.0):.2f}"
                f"  |  跌停={market_risk.get('down_limit_count', 0)}家"
                f"  |  上涨占比={market_risk.get('up_ratio', 0) * 100:.1f}%"
            ))

        blocks.append(_paragraph(f"候选股票: 共筛出 {len(candidates)} 只，Notion 展示前 {len(top)} 只，完整明细见 CSV"))
        blocks.append(_divider())

        for idx, cand in enumerate(top, start=1):
            blocks.extend(_build_candidate_blocks(idx, cand))
    else:
        # Fallback：解析 Markdown（保留原有逻辑）
        blocks.extend(_markdown_to_notion_blocks(markdown_path.read_text(encoding="utf-8")))

    return blocks


def _markdown_to_notion_blocks(markdown: str, max_lines: int = 120) -> List[Dict]:
    lines = markdown.splitlines()
    blocks: List[Dict] = []

    for line in lines[:max_lines]:
        stripped = line.strip()
        if not stripped:
            continue

        if stripped.startswith("# "):
            blocks.append(_heading2(stripped[2:].strip()))
        elif stripped.startswith("## "):
            blocks.append(_heading2(stripped[3:].strip()))
        elif stripped.startswith("- "):
            blocks.append(
                {
                    "object": "block",
                    "type": "bulleted_list_item",
                    "bulleted_list_item": {"rich_text": _rich_text(stripped[2:].strip())},
                }
            )
        else:
            blocks.append(_paragraph(stripped))

    if len(lines) > max_lines:
        blocks.append(_paragraph("内容较长，已截断。完整明细请查看本地 report 文件。"))

    return blocks


def push_daily_report_to_notion(
    trade_date: str,
    markdown_path: Path,
    csv_path: Path,
    market_overview: str,
    candidates: List[Dict] | None = None,
    top_n: int = 15,
    title_prefix: str = "QRAI日级复盘",
    existing_page_policy: str = "skip",
) -> Dict[str, object]:
    """推送日级复盘报告到 Notion。

    Args:
        candidates: rank_candidates() 返回的结构化候选列表。传入后按天/地/人维度分组展示；
                    不传则降级解析 Markdown 文件。
        top_n: Notion 页面展示的候选数量（不影响 CSV/Markdown 输出数量）。
        existing_page_policy: skip=跳过已有页面；update=清空重写；append=追加。
    """
    if not NOTION_TOKEN or not NOTION_PARENT_PAGE_ID:
        return {"success": False, "skipped": True, "error": "NOTION_TOKEN 或 NOTION_PARENT_PAGE_ID 未配置"}

    if not markdown_path.exists():
        return {"success": False, "skipped": True, "error": f"Markdown 文件不存在: {markdown_path}"}

    normalized_policy = (existing_page_policy or "skip").lower().strip()
    if normalized_policy not in {"skip", "update", "append"}:
        return {
            "success": False,
            "skipped": True,
            "error": f"existing_page_policy 非法: {existing_page_policy}，可选 skip/update/append",
        }

    run_ts = datetime.now(_CST).strftime("%H%M")
    title = f"【{title_prefix}】{trade_date}-{run_ts}"

    hierarchy = _ensure_date_hierarchy(NOTION_PARENT_PAGE_ID, trade_date)
    if not hierarchy.get("success"):
        return {"success": False, "skipped": False, "error": hierarchy.get("error")}

    day_page_id = hierarchy["day_page"]["id"]

    existing = _find_child_page_by_title(day_page_id, title)
    if not existing.get("success"):
        return {"success": False, "skipped": False, "error": existing.get("error")}

    # 构建所有 blocks（可能超过 100，通过分批追加处理）
    blocks = _build_report_blocks(
        trade_date=trade_date,
        market_overview=market_overview,
        markdown_path=markdown_path,
        csv_path=csv_path,
        candidates=candidates,
        top_n=top_n,
    )

    if existing.get("found"):
        page = existing.get("page", {})
        page_id = page.get("id")
        if normalized_policy == "skip":
            return {
                "success": True,
                "skipped": True,
                "page_id": page_id,
                "url": page.get("raw", {}).get("url"),
                "title": title,
                "message": "已存在同名日报，按 skip 策略跳过创建",
            }

        if not page_id:
            return {"success": False, "skipped": False, "error": "已存在页面缺少 page_id"}

        if normalized_policy == "update":
            cleared = _clear_page_blocks(page_id)
            if not cleared.get("success"):
                return {"success": False, "skipped": False, "error": cleared.get("error")}

        appended = _append_blocks_in_batches(page_id, blocks)
        if not appended.get("success"):
            return {"success": False, "skipped": False, "error": appended.get("error")}

        action = "覆盖更新" if normalized_policy == "update" else "追加更新"
        return {
            "success": True,
            "skipped": False,
            "page_id": page_id,
            "url": page.get("raw", {}).get("url"),
            "title": title,
            "message": f"已存在同名日报，已执行{action}",
        }

    # 创建空页面，再批量追加内容（规避 Notion 单次 ≤100 blocks 限制）
    created = _create_child_page(day_page_id, title)
    if not created.get("success"):
        return {"success": False, "skipped": False, "error": created.get("error")}

    page_id = created["page"]["id"]
    appended = _append_blocks_in_batches(page_id, blocks)
    if not appended.get("success"):
        return {"success": False, "skipped": False, "error": appended.get("error")}

    page = created.get("page", {})
    return {
        "success": True,
        "skipped": False,
        "page_id": page_id,
        "url": page.get("url"),
        "title": title,
        "blocks_count": len(blocks),
    }
