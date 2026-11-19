"""命令行入口：

  ingest      按期清单摄入来源文件（原样保存 + 规范化）
  snapshot    构建并提交不可快变照（可指定口径文件/内置 v1、v2）
  reports     列出快照
  report      报告期汇总报表（口径、覆盖、确认/未确认数量）
  coverage    来源覆盖明细
  drill       终端事实下钻到观测、原始行与关联证据
  drill-agg   汇总事实下钻到原始汇总行与明细覆盖
  compare     同报告期两个修订快照对比（区分事实变化与口径变化）
"""
from __future__ import annotations

import argparse
import json
import sys

from .config import (DEFAULT_CALIBER_V1, DEFAULT_CALIBER_V2, load_caliber)
from .ingestion import ingest_manifest
from .reporting import (build_report, compare_snapshots, drill_aggregate,
                        drill_fact, latest_snapshot_id, list_snapshots,
                        source_coverage)
from .snapshot import create_snapshot
from .storage import connect

DEFAULT_DB = "build/connections.db"
BUILTIN_CALIBERS = {"v1": DEFAULT_CALIBER_V1, "v2": DEFAULT_CALIBER_V2}


def _print_json(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=False))


def _resolve_caliber(args) -> dict:
    if args.caliber_file:
        return load_caliber(args.caliber_file)
    return BUILTIN_CALIBERS[args.caliber]


def _resolve_snapshot(conn, period: str, revision: int | None) -> str:
    if revision is not None:
        row = conn.execute(
            "SELECT snapshot_id FROM snapshots WHERE period = ? AND revision_no = ?",
            (period, revision),
        ).fetchone()
        if row is None:
            raise SystemExit(f"报告期 {period} 不存在 revision {revision}")
        return row["snapshot_id"]
    sid = latest_snapshot_id(conn, period)
    if sid is None:
        raise SystemExit(f"报告期 {period} 尚无快照，请先执行 snapshot")
    return sid


def cmd_ingest(conn, args) -> None:
    result = ingest_manifest(conn, args.manifest)
    _print_json(result) if args.json else _print_ingest(result)


def _print_ingest(result: dict) -> None:
    print(f"报告期 {result['period']} 摄入完成："
          f"新批次 {result['new_batches']}，重复跳过 {result['skipped']}")
    for f in result["files"]:
        print(f"  - {f['file']}: {f['status']}，{f['rows']} 行（{f['batch_id']}）")


def cmd_snapshot(conn, args) -> None:
    rules = _resolve_caliber(args)
    summary = create_snapshot(conn, args.period, rules, note=args.note)
    _print_json(summary) if args.json else _print_snapshot(summary)


def _print_snapshot(s: dict) -> None:
    print(f"已提交不可变快照 {s['snapshot_id']}")
    print(f"  报告期={s['period']} revision={s['revision_no']} "
          f"口径={s['caliber_id']}")
    print(f"  终端簇={s['clusters']} 已确认={s['confirmed']} "
          f"未判定={s['undetermined']}")
    print(f"  输入指纹={s['input_fingerprint'][:16]}… 时间={s['created_at']}")


def cmd_reports(conn, args) -> None:
    rows = list_snapshots(conn, args.period)
    if args.json:
        _print_json(rows)
        return
    if not rows:
        print("（暂无快照）")
    for r in rows:
        print(f"{r['period']} rev{r['revision_no']} 快照={r['snapshot_id']} "
              f"口径版本=v{r['caliber_version']} 时间={r['created_at']} "
              f"备注={r['note']}")


def cmd_report(conn, args) -> None:
    sid = _resolve_snapshot(conn, args.period, args.revision)
    report = build_report(conn, sid)
    if args.json:
        _print_json(report)
        return
    _print_report(report)


def _print_report(r: dict) -> None:
    snap, counts, cal = r["snapshot"], r["counts"], r["caliber"]
    print(f"报告期 {snap['period']}（revision {snap['revision_no']}，"
          f"快照 {snap['snapshot_id']}）")
    print(f"口径版本 v{cal['version']}：")
    print(f"  可相加对象来源（additive）：{', '.join(cal['additive_population_sources'])}")
    print(f"  交叉观测来源（不相加）    ：{', '.join(cal['cross_observation_sources'])}")
    print(f"  仅汇总约束来源            ：{', '.join(cal['constraint_sources'])}")
    print("来源覆盖：")
    for c in r["coverage"]:
        print(f"  - {c['source']}: 明细 {c['detail_rows']} 行/"
              f"{c['batches']} 批次，汇总 {c['aggregate_rows']} 行"
              f"（报数合计 {c['aggregate_reported_total']}），"
              f"证据 {c['evidence_rows']} 条"
              f"（共现 {c['co_occurrence_evidence']}/映射 {c['mapping_evidence']}）")
    print("连接事实计数：")
    print(f"  已确认终端 confirmed   = {counts['confirmed_terminals']}")
    print(f"  未判定终端 undetermined= {counts['undetermined_terminals']}")
    print(f"  可识别实体合计         = {counts['distinct_observed_entities']}"
          "（未判定单列，不与已确认混算）")
    print(f"  汇总约束条数           = {counts['aggregate_constraints']}")
    print(f"  汇总无法解释缺口       = {counts['unexplained_aggregate_gap']}")
    if r["name_similarity_diagnostics"]:
        print("名称相似但保持独立的诊断（未合并）：")
        for d in r["name_similarity_diagnostics"]:
            print(f"  - {d['left_device_id']} ~ {d['right_device_id']} "
                  f"相似度 {d['similarity']}：{d['reason']}")


def cmd_coverage(conn, args) -> None:
    rows = source_coverage(conn, args.period)
    _print_json(rows) if args.json else _print_ingest_coverage(rows)


def _print_ingest_coverage(rows: list[dict]) -> None:
    for c in rows:
        print(f"{c['source']}: 明细 {c['detail_rows']}，汇总 {c['aggregate_rows']}"
              f"/{c['aggregate_reported_total']}，证据 {c['evidence_rows']}")


def cmd_drill(conn, args) -> None:
    sid = _resolve_snapshot(conn, args.period, args.revision)
    result = drill_fact(conn, sid, args.fact_uid)
    if args.json:
        _print_json(result)
        return
    f = result["fact"]
    print(f"事实 {f['fact_uid']} 类型={f['fact_type']} 状态={f['status']}")
    if "cluster" in result:
        cl = result["cluster"]
        print(f"终端簇 {cl['cluster_uid']}（terminal_uid={cl['terminal_uid']}，"
              f"判定原因={cl['status_reason']}）")
        print("关联观测与原始行：")
        for o in result["observations"]:
            print(f"  - 观测 {o['observation_uid']} 来源={o['source_name']} "
                  f"device={o['device_id']} imsi={o['imsi']} iccid={o['iccid']}")
            print(f"    原始行[{o['raw_record']['row_index']}]: "
                  f"{o['raw_record']['raw_payload']}")
        print("关联依据：")
        for e in result["evidence"]:
            if e.get("evidence_uid"):
                print(f"  - {e['basis']}: {e['evidence_type']} "
                      f"{e['id_type']}={e['id_value']} 适用 "
                      f"{e['applies_from'] or '…'}~{e['applies_to'] or '…'}"
                      f"（证据 {e['evidence_uid']}）")
            else:
                print(f"  - {e['basis']}: {e['detail']}")


def cmd_drill_agg(conn, args) -> None:
    sid = _resolve_snapshot(conn, args.period, args.revision)
    result = drill_aggregate(conn, sid, args.fact_uid)
    if args.json:
        _print_json(result)
        return
    f = result["fact"]
    print(f"汇总约束事实 {f['fact_uid']} 报数={f['reported_count']} "
          f"明细覆盖={f['detail_covered']} 缺口={f['unexplained_gap']}")
    print("汇总原始行：")
    for a in result["aggregate_observations"]:
        print(f"  - 来源={a['source_name']} 账号={a['account_ref']} "
              f"原始={a['raw_payload']}")
    print(f"可相加明细覆盖到的终端（{len(result['covered_detail_terminals'])} 个，按簇去重）：")
    for t in result["covered_detail_terminals"]:
        print(f"  - {t['terminal_uid']} 状态={t['status']} "
              f"来源={t['source_names']} device={t['device_id']}")


def cmd_compare(conn, args) -> None:
    left = _resolve_snapshot(conn, args.period, args.left_revision)
    right = _resolve_snapshot(conn, args.period, args.right_revision)
    result = compare_snapshots(conn, left, right)
    if args.json:
        _print_json(result)
        return
    d = result["difference_drivers"]
    print(f"报告期 {result['period']} 修订对比："
          f"rev{result['left']['revision_no']} -> "
          f"rev{result['right']['revision_no']}")
    print(f"驱动因素：原始事实变化={d['raw_fact_change']} "
          f"口径变化={d['caliber_change']}")
    print(f"说明：{d['explanation']}")
    print(f"计数：{result['counts_left']} -> {result['counts_right']}")
    for label, key in (("新增终端", "terminals_added"),
                       ("消失终端", "terminals_removed"),
                       ("状态变化", "status_changed")):
        items = result[key]
        print(f"{label}（{len(items)}）：")
        for it in items:
            print(f"  - {it} 原因={it['why']}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="run_pipeline",
        description="通信网络连接总量核对管道（证据驱动、快照不可变）",
    )
    parser.add_argument("--db", default=DEFAULT_DB, help=f"SQLite 路径（默认 {DEFAULT_DB}）")
    parser.add_argument("--json", action="store_true", help="以 JSON 输出")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("ingest", help="按期清单摄入来源文件")
    p.add_argument("manifest")
    p.set_defaults(func=cmd_ingest)

    p = sub.add_parser("snapshot", help="构建并提交不可变快照")
    p.add_argument("period")
    p.add_argument("--caliber", choices=("v1", "v2"), default="v1",
                   help="内置口径（默认 v1）")
    p.add_argument("--caliber-file", help="自定义口径 JSON 文件，优先于 --caliber")
    p.add_argument("--note", default="")
    p.set_defaults(func=cmd_snapshot)

    p = sub.add_parser("reports", help="列出快照")
    p.add_argument("--period", default=None)
    p.set_defaults(func=cmd_reports)

    p = sub.add_parser("report", help="报告期汇总报表")
    p.add_argument("period")
    p.add_argument("--revision", type=int, default=None)
    p.set_defaults(func=cmd_report)

    p = sub.add_parser("coverage", help="来源覆盖明细")
    p.add_argument("period")
    p.set_defaults(func=cmd_coverage)

    p = sub.add_parser("drill", help="终端事实下钻")
    p.add_argument("period")
    p.add_argument("fact_uid")
    p.add_argument("--revision", type=int, default=None)
    p.set_defaults(func=cmd_drill)

    p = sub.add_parser("drill-agg", help="汇总事实下钻")
    p.add_argument("period")
    p.add_argument("fact_uid")
    p.add_argument("--revision", type=int, default=None)
    p.set_defaults(func=cmd_drill_agg)

    p = sub.add_parser("compare", help="同期两个修订快照对比")
    p.add_argument("period")
    p.add_argument("left_revision", type=int)
    p.add_argument("right_revision", type=int)
    p.set_defaults(func=cmd_compare)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    conn = connect(args.db)
    try:
        args.func(conn, args)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
