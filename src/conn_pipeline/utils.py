"""通用工具：确定性哈希、时间获取、标识规范化。"""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def short_id(prefix: str, *parts: Any, length: int = 16) -> str:
    """由内容派生确定性 ID，保证同输入重复执行结果一致。"""
    material = "|".join(str(p) for p in parts)
    return f"{prefix}_{sha256_text(material)[:length]}"


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def now_iso() -> str:
    """当前 UTC 时间；可用环境变量 PIPELINE_NOW 固定，便于演示与测试复现。"""
    override = os.environ.get("PIPELINE_NOW")
    if override:
        # 允许传 '2026-10-20T09:00:00Z' 或 '2026-10-20 09:00:00'
        return override.replace(" ", "T").replace("Z", "") + (
            "" if override.endswith("Z") or "T" in override else ""
        )
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def norm_identifier(value: Any) -> str | None:
    """标识规范化：去首尾空白；空串/None 视为缺失。

    仅做大小写与空白层面的保守处理，绝不做名称相似性归并。
    """
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_month(value: str | None) -> str | None:
    """把 'YYYY-MM-DD' 或 'YYYY-MM' 归一化为月份 'YYYY-MM'。"""
    if not value:
        return None
    text = str(value).strip()
    if len(text) >= 7 and text[4] == "-":
        return text[:7]
    return text


def evidence_applies(applies_from: str | None, applies_to: str | None,
                     period: str) -> bool:
    """判断证据的适用区间是否覆盖报告期（按月，期号格式 YYYY-MM）。

    区间端点按月份比较：applies_from <= period <= applies_to，端点为含；
    端点可能写成完整日期，统一截取到月份再比较。
    """
    month_from = _to_month(applies_from)
    month_to = _to_month(applies_to)
    if month_from and period < month_from:
        return False
    if month_to and period > month_to:
        return False
    return True
