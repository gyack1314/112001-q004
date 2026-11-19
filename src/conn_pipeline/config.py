"""口径（caliber）模型与期清单（manifest）解析。

口径回答"哪些来源代表可相加的不同对象、哪些只是交叉观测"；
它与原始事实分开版本化，改口径不改写任何 landing/观测/证据数据。
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .utils import canonical_json, norm_identifier

# 角色：additive_population = 可相加的不同对象总体；
#       cross_observation   = 对同一批对象的交叉观测，绝不相加；
#       constraint          = 仅汇总数，作为覆盖约束，不产生逐终端事实
ROLE_ADDITIVE = "additive_population"
ROLE_CROSS = "cross_observation"
ROLE_CONSTRAINT = "constraint"

DEFAULT_CALIBER_V1: dict = {
    "version": 1,
    "description": (
        "v1 初始口径：operator_a 与 iot_registry 终端明细代表可相加的不同对象；"
        "sample_platform 仅为交叉观测，不重复相加；iot_registry 的 account_total "
        "只有汇总数，作为覆盖约束；关联必须有显式证据，证据不足保留 undetermined。"
    ),
    "source_roles": {
        "operator_a": {
            "role": ROLE_ADDITIVE,
            "note": "运营商账面，终端级明细，代表可相加对象",
        },
        "iot_registry": {
            "role": ROLE_ADDITIVE,
            "note": "物联网业务清单明细，终端级，代表可相加对象",
        },
        "sample_platform": {
            "role": ROLE_CROSS,
            "note": "平台采样，对同一终端的交叉观测，绝不与账面相加",
        },
    },
    "aggregate_roles": {
        "iot_registry": {
            "grain": "account_total",
            "role": ROLE_CONSTRAINT,
            "note": "只有汇总数、无逐终端明细，作为覆盖约束对照",
        },
    },
    "identity_policy": {
        "merge_requires_explicit_evidence": True,
        "name_similarity_merge": False,
        "unmatched_observation_status": "undetermined",
        "source_specific": {
            # 仅持有 ICCID、无法关联到任何终端的物联网行，v1 下保留未判定
            "iot_registry": {"iccid_only_status": "undetermined"},
        },
    },
}

DEFAULT_CALIBER_V2: dict = {
    "version": 2,
    "description": (
        "v2 口径修订（口径变化，非事实变化）：经业务确认 ICCID 即物联网终端的法定"
        "服务标识，iot_registry 中仅含 ICCID 的行视为已确认的独立终端；"
        "其余规则与 v1 完全相同。"
    ),
    "source_roles": {
        "operator_a": {
            "role": ROLE_ADDITIVE,
            "note": "运营商账面，终端级明细，代表可相加对象",
        },
        "iot_registry": {
            "role": ROLE_ADDITIVE,
            "note": "物联网业务清单明细，终端级，代表可相加对象",
        },
        "sample_platform": {
            "role": ROLE_CROSS,
            "note": "平台采样，对同一终端的交叉观测，绝不与账面相加",
        },
    },
    "aggregate_roles": {
        "iot_registry": {
            "grain": "account_total",
            "role": ROLE_CONSTRAINT,
            "note": "只有汇总数、无逐终端明细，作为覆盖约束对照",
        },
    },
    "identity_policy": {
        "merge_requires_explicit_evidence": True,
        "name_similarity_merge": False,
        "unmatched_observation_status": "undetermined",
        "source_specific": {
            "iot_registry": {"iccid_only_status": "confirmed_distinct"},
        },
    },
}

SUPPORTED_GRAINS = {"device", "account_total", "identity_mapping"}


@dataclass(frozen=True)
class ManifestFile:
    file: str
    source: str
    grain: str
    field_map: dict
    description: str = ""
    path: str | None = None  # 相对清单目录的实际读取路径，缺省与 file 相同


@dataclass(frozen=True)
class PeriodManifest:
    period: str
    description: str
    files: tuple[ManifestFile, ...]

    def by_grain(self, grain: str) -> tuple[ManifestFile, ...]:
        return tuple(f for f in self.files if f.grain == grain)


def load_manifest(path: str | Path) -> PeriodManifest:
    """读取并校验期清单。清单本身也会被原样保存为来源文件。"""
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    period = raw["period"]
    if not isinstance(period, str) or len(period) != 7 or period[4] != "-":
        raise ValueError(f"期号格式应为 YYYY-MM：{period!r}")
    files = []
    for entry in raw["files"]:
        grain = entry["grain"]
        if grain not in SUPPORTED_GRAINS:
            raise ValueError(f"不支持的 grain：{grain}（文件 {entry['file']}）")
        if not entry.get("source"):
            raise ValueError(f"文件 {entry['file']} 缺少 source")
        files.append(
            ManifestFile(
                file=entry["file"],
                source=entry["source"],
                grain=grain,
                field_map=dict(entry.get("field_map", {})),
                description=entry.get("description", ""),
                path=entry.get("path"),
            )
        )
    return PeriodManifest(
        period=period,
        description=raw.get("description", ""),
        files=tuple(files),
    )


def load_caliber(path: str | Path) -> dict:
    """读取口径 JSON 并做最小结构校验。"""
    rules = json.loads(Path(path).read_text(encoding="utf-8"))
    if "source_roles" not in rules or "identity_policy" not in rules:
        raise ValueError("口径文件必须包含 source_roles 与 identity_policy")
    policy = rules["identity_policy"]
    if policy.get("name_similarity_merge"):
        raise ValueError("口径禁止启用名称相似性合并：name_similarity_merge 必须为 false")
    if not policy.get("merge_requires_explicit_evidence", True):
        raise ValueError("口径必须要求显式证据：merge_requires_explicit_evidence 应为 true")
    return rules


def caliber_fingerprint(rules: dict) -> str:
    """口径指纹只覆盖规则实质（来源角色、汇总角色、身份策略）。

    description、note 属于说明文字，不改变统计口径，不参与指纹；
    这样同规则、不同描述的口径文件会复用同一版本。
    """
    def strip_notes(mapping: dict) -> dict:
        return {
            name: {k: v for k, v in cfg.items() if k != "note"}
            for name, cfg in (mapping or {}).items()
        }

    material = {
        "source_roles": strip_notes(rules.get("source_roles", {})),
        "aggregate_roles": strip_notes(rules.get("aggregate_roles", {})),
        "identity_policy": rules.get("identity_policy", {}),
    }
    return canonical_json(material)


def map_field(row: dict, field_map: dict, logical_name: str) -> str | None:
    """按清单声明的字段映射取数；未声明映射时回退到同名逻辑字段。"""
    source_field = field_map.get(logical_name, logical_name)
    return norm_identifier(row.get(source_field))
