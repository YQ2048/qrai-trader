from pathlib import Path
from typing import Dict, List
from datetime import datetime

import requests

from code.core.config import NOTION_PARENT_PAGE_ID, NOTION_TOKEN


NOTION_API_BASE = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"


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


def _create_child_page(parent_page_id: str, title: str, children: List[Dict] | None = None) -> Dict[str, object]:
    payload = {
        "parent": {"page_id": parent_page_id},
        "properties": {
            "title": {
                "title": [{"type": "text", "text": {"content": title}}]
            }
        },
    }
    if children:
        payload["children"] = children

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


def _markdown_to_notion_blocks(markdown: str, max_lines: int = 120) -> List[Dict]:
    lines = markdown.splitlines()
    blocks: List[Dict] = []

    for line in lines[:max_lines]:
        stripped = line.strip()
        if not stripped:
            continue

        if stripped.startswith("# "):
            blocks.append(
                {
                    "object": "block",
                    "type": "heading_1",
                    "heading_1": {"rich_text": _rich_text(stripped[2:].strip())},
                }
            )
        elif stripped.startswith("## "):
            blocks.append(
                {
                    "object": "block",
                    "type": "heading_2",
                    "heading_2": {"rich_text": _rich_text(stripped[3:].strip())},
                }
            )
        elif stripped.startswith("- "):
            blocks.append(
                {
                    "object": "block",
                    "type": "bulleted_list_item",
                    "bulleted_list_item": {"rich_text": _rich_text(stripped[2:].strip())},
                }
            )
        else:
            blocks.append(
                {
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {"rich_text": _rich_text(stripped)},
                }
            )

    if len(lines) > max_lines:
        blocks.append(
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": _rich_text("内容较长，已截断。完整明细请查看本地 report 文件。")},
            }
        )

    return blocks


def push_daily_report_to_notion(
    trade_date: str,
    markdown_path: Path,
    csv_path: Path,
    market_overview: str,
    title_prefix: str = "QRAI 日级复盘",
    existing_page_policy: str = "skip",
) -> Dict[str, object]:
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

    markdown = markdown_path.read_text(encoding="utf-8")
    title = f"{title_prefix} {trade_date}"

    hierarchy = _ensure_date_hierarchy(NOTION_PARENT_PAGE_ID, trade_date)
    if not hierarchy.get("success"):
        return {"success": False, "skipped": False, "error": hierarchy.get("error")}

    day_page_id = hierarchy["day_page"]["id"]

    existing = _find_child_page_by_title(day_page_id, title)
    if not existing.get("success"):
        return {"success": False, "skipped": False, "error": existing.get("error")}

    intro = [
        {
            "object": "block",
            "type": "paragraph",
            "paragraph": {"rich_text": _rich_text(f"交易日: {trade_date}")},
        },
        {
            "object": "block",
            "type": "paragraph",
            "paragraph": {"rich_text": _rich_text(f"大盘概况: {market_overview}")},
        },
        {
            "object": "block",
            "type": "paragraph",
            "paragraph": {"rich_text": _rich_text(f"本地报告: {markdown_path.name} / {csv_path.name}")},
        },
    ]

    blocks = intro + _markdown_to_notion_blocks(markdown)

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

        appended = _append_blocks_to_page(page_id, blocks)
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

    created = _create_child_page(day_page_id, title, children=blocks)
    if not created.get("success"):
        return {"success": False, "skipped": False, "error": created.get("error")}

    page = created.get("page", {})
    return {
        "success": True,
        "skipped": False,
        "page_id": page.get("id"),
        "url": page.get("url"),
        "title": title,
    }
