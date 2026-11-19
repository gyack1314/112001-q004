"""管道单元测试：在临时目录内构造虚构来源，覆盖全部关键保证。

运行（隔离模式）：python3 -I -m unittest discover -s tests
"""
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from conn_pipeline.config import (DEFAULT_CALIBER_V1, DEFAULT_CALIBER_V2,
                                  load_manifest)
from conn_pipeline.ingestion import ingest_manifest
from conn_pipeline.reporting import (build_report, compare_snapshots,
                                     drill_aggregate, drill_fact,
                                     source_coverage)
from conn_pipeline.snapshot import create_snapshot
from conn_pipeline.storage import connect
from conn_pipeline.utils import evidence_applies

MANIFEST_TEMPLATE = {
    "period": "2026-03",
    "description": "test",
    "files": [
        {"file": "op.json", "source": "operator_a", "grain": "device",
         "field_map": {"device_id": "device_id", "imsi": "imsi",
                       "iccid": "iccid", "network": "network"}},
        {"file": "sp.json", "source": "sample_platform", "grain": "device",
         "field_map": {"device_id": "device_id", "imsi": "imsi",
                       "network": "network"}},
        {"file": "iot.json", "source": "iot_registry", "grain": "device",
         "field_map": {"device_id": "device_id", "imsi": "imsi",
                       "iccid": "iccid", "network": "network"}},
        {"file": "tot.json", "source": "iot_registry", "grain": "account_total",
         "field_map": {"count": "count", "account_ref": "account_ref"}},
    ],
}


class PipelineTestBase(unittest.TestCase):
    def setUp(self) -> None:
        os.environ["PIPELINE_NOW"] = "2026-04-01T00:00:00Z"
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.db_path = self.dir / "pipe.db"
        self.conn = connect(str(self.db_path))

    def tearDown(self) -> None:
        self.conn.close()
        self.tmp.cleanup()
        os.environ.pop("PIPELINE_NOW", None)

    def _write(self, name: str, rows) -> Path:
        p = self.dir / name
        p.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
        return p

    def _manifest(self, period="2026-03", files=None) -> Path:
        m = json.loads(json.dumps(MANIFEST_TEMPLATE))
        m["period"] = period
        if files is not None:
            m["files"] = files
        p = self.dir / f"manifest_{period}.json"
        p.write_text(json.dumps(m, ensure_ascii=False), encoding="utf-8")
        return p

    def _ingest_default(self, period="2026-03"):
        self._write("op.json", [
            {"device_id": "IMEI-1", "imsi": "IMSI-1", "iccid": None, "network": "5G"},
            {"device_id": "IMEI-2", "imsi": "IMSI-2", "iccid": None, "network": "4G"},
        ])
        # 平台与账面共享 device_id（交叉观测）；另有平台孤儿；再加一对"同名近似"
        self._write("sp.json", [
            {"device_id": "IMEI-1", "imsi": None, "network": "5G"},
            {"device_id": "IMEI-900000", "imsi": None, "network": "4G"},
            {"device_id": "IMEI-900000A", "imsi": None, "network": "4G"},
        ])
        self._write("iot.json", [
            {"device_id": "IMEI-7", "imsi": None, "iccid": "ICCID-7", "network": "NB"},
            {"device_id": None, "imsi": None, "iccid": "ICCID-8", "network": "NB"},
        ])
        self._write("tot.json", [{"account_ref": "A1", "count": 5}])
        return ingest_manifest(self.conn, self._manifest(period))


class LandingTests(PipelineTestBase):
    def test_raw_records_keep_original_payload_and_hash(self):
        self._ingest_default()
        raw = self.conn.execute(
            "SELECT raw_payload, row_sha256 FROM raw_records "
            "WHERE period = '2026-03' ORDER BY row_index"
        ).fetchall()
        first = json.loads(raw[0]["raw_payload"])
        self.assertEqual(first["device_id"], "IMEI-1")
        self.assertEqual(len(raw), 2 + 3 + 2 + 1)
        self.assertTrue(all(r["row_sha256"] for r in raw))

    def test_ingest_is_idempotent_by_file_hash(self):
        r1 = self._ingest_default()
        self.assertEqual(r1["new_batches"], 4)
        r2 = ingest_manifest(self.conn, self._manifest())
        self.assertEqual(r2["new_batches"], 0)
        self.assertEqual(r2["skipped"], 4)
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) n FROM raw_records").fetchone()["n"],
            8,
        )

    def test_landing_tables_are_append_only(self):
        self._ingest_default()
        for sql in ("UPDATE raw_records SET raw_payload='x'",
                    "DELETE FROM raw_records",
                    "UPDATE observations SET device_id='x'",
                    "DELETE FROM identity_evidence"):
            with self.assertRaises((sqlite3.OperationalError, sqlite3.IntegrityError)):
                self.conn.execute(sql)
            self.conn.rollback()


class IdentityTests(PipelineTestBase):
    def test_shared_identifier_merges_cross_source_but_keeps_orphans(self):
        self._ingest_default()
        snap = create_snapshot(self.conn, "2026-03", DEFAULT_CALIBER_V1)
        # 簇：IMEI-1(账面+平台)、IMEI-2、IMEI-9、IMEI-9X、IMEI-7、ICCID-8 = 6
        self.assertEqual(snap["clusters"], 6)
        self.assertEqual(snap["confirmed"], 1)   # 仅 IMEI-1 跨来源证据确认
        self.assertEqual(snap["undetermined"], 5)
        report = build_report(self.conn, snap["snapshot_id"])
        # 相似名称 IMEI-9 / IMEI-9X 保持独立，只出现在诊断里
        diag = report["name_similarity_diagnostics"]
        self.assertTrue(any(d["left_device_id"] == "IMEI-900000" for d in diag))
        self.assertTrue(all(d["action"] == "kept_separate" for d in diag))

    def test_explicit_mapping_with_applicable_period_merges(self):
        self._ingest_default()
        # 迟到映射：平台孤儿 IMSI 关联到账面 IMEI-2，适用 3 月
        self._write("map.json", [{
            "mapping_id": "M1", "id_type": "imsi", "id_value": "IMSI-2",
            "pair_type": "device_id", "pair_value": "IMEI-2",
            "applies_from": "2026-03-01", "applies_to": "2026-03-31",
        }])
        manifest = self._manifest("2026-03", files=[{
            "file": "map.json", "source": "operator_a",
            "grain": "identity_mapping",
            "field_map": {"mapping_id": "mapping_id", "id_type": "id_type",
                          "id_value": "id_value", "pair_type": "pair_type",
                          "pair_value": "pair_value",
                          "applies_from": "applies_from",
                          "applies_to": "applies_to"}}])
        ingest_manifest(self.conn, manifest)
        # 平台侧补一条只持 IMSI-2 的观测，模拟漫游
        self._write("sp2.json", [{"device_id": None, "imsi": "IMSI-2",
                                  "network": "4G"}])
        manifest2 = self._manifest("2026-03", files=[{
            "file": "sp2.json", "source": "sample_platform", "grain": "device",
            "field_map": {"device_id": "device_id", "imsi": "imsi",
                          "network": "network"}}])
        ingest_manifest(self.conn, manifest2)
        snap = create_snapshot(self.conn, "2026-03", DEFAULT_CALIBER_V1)
        self.assertEqual(snap["confirmed"], 2)
        merged = self.conn.execute(
            "SELECT COUNT(*) n FROM cluster_observations co "
            "JOIN clusters c ON c.cluster_uid = co.cluster_uid "
            "WHERE c.snapshot_id = ? GROUP BY c.cluster_uid HAVING n >= 2",
            (snap["snapshot_id"],),
        ).fetchall()
        self.assertEqual(len(merged), 2)

    def test_evidence_outside_period_is_not_used(self):
        self.assertTrue(evidence_applies("2026-03-01", "2026-03-31", "2026-03"))
        self.assertFalse(evidence_applies("2026-04-01", "2026-04-30", "2026-03"))
        self.assertTrue(evidence_applies(None, None, "2026-03"))
        self.assertFalse(evidence_applies("2026-02-01", "2026-02-28", "2026-03"))

        self._ingest_default("2026-04")
        # 映射适用区间只到 3 月，对 4 月快照不产生合并
        self._write("map_old.json", [{
            "mapping_id": "M_OLD", "id_type": "imsi", "id_value": "IMSI-1",
            "pair_type": "device_id", "pair_value": "IMEI-1",
            "applies_from": "2026-03-01", "applies_to": "2026-03-31",
        }])
        manifest = self._manifest("2026-04", files=[{
            "file": "map_old.json", "source": "operator_a",
            "grain": "identity_mapping",
            "field_map": {"mapping_id": "mapping_id", "id_type": "id_type",
                          "id_value": "id_value", "pair_type": "pair_type",
                          "pair_value": "pair_value",
                          "applies_from": "applies_from",
                          "applies_to": "applies_to"}}])
        ingest_manifest(self.conn, manifest)
        snap = create_snapshot(self.conn, "2026-04", DEFAULT_CALIBER_V1)
        # IMEI-1 仍仅靠共现+跨来源 device_id 确认；旧映射没有新增任何簇
        self.assertEqual(snap["clusters"], 6)

    def test_similar_names_never_merge_without_evidence(self):
        self._ingest_default()
        snap = create_snapshot(self.conn, "2026-03", DEFAULT_CALIBER_V1)
        ids = {r["device_id"] for r in self.conn.execute(
            "SELECT device_id FROM connection_facts WHERE snapshot_id = ? "
            "AND device_id IS NOT NULL", (snap["snapshot_id"],)).fetchall()}
        self.assertIn("IMEI-900000", ids)
        self.assertIn("IMEI-900000A", ids)


class SnapshotTests(PipelineTestBase):
    def test_snapshots_are_immutable_history(self):
        self._ingest_default()
        s1 = create_snapshot(self.conn, "2026-03", DEFAULT_CALIBER_V1, note="r1")
        # 迟到证据后生成新 revision
        self._write("map.json", [{
            "mapping_id": "M1", "id_type": "iccid", "id_value": "ICCID-8",
            "pair_type": "device_id", "pair_value": "IMEI-7",
            "applies_from": "2026-03-01", "applies_to": "2026-12-31",
        }])
        manifest = self._manifest("2026-03", files=[{
            "file": "map.json", "source": "iot_registry",
            "grain": "identity_mapping",
            "field_map": {"mapping_id": "mapping_id", "id_type": "id_type",
                          "id_value": "id_value", "pair_type": "pair_type",
                          "pair_value": "pair_value",
                          "applies_from": "applies_from",
                          "applies_to": "applies_to"}}])
        ingest_manifest(self.conn, manifest)
        s2 = create_snapshot(self.conn, "2026-03", DEFAULT_CALIBER_V1, note="r2")
        self.assertEqual(s1["revision_no"], 1)
        self.assertEqual(s2["revision_no"], 2)
        # 旧快照事实原样保留：合并使终端事实 6 -> 5，汇总事实恒为 1
        old = self.conn.execute(
            "SELECT COUNT(*) n FROM connection_facts WHERE snapshot_id = ?",
            (s1["snapshot_id"],)).fetchone()["n"]
        new = self.conn.execute(
            "SELECT COUNT(*) n FROM connection_facts WHERE snapshot_id = ?",
            (s2["snapshot_id"],)).fetchone()["n"]
        self.assertEqual(old, 7)
        self.assertEqual(new, 6)
        old_clusters = self.conn.execute(
            "SELECT COUNT(*) n FROM clusters WHERE snapshot_id = ?",
            (s1["snapshot_id"],)).fetchone()["n"]
        new_clusters = self.conn.execute(
            "SELECT COUNT(*) n FROM clusters WHERE snapshot_id = ?",
            (s2["snapshot_id"],)).fetchone()["n"]
        self.assertEqual(old_clusters, 6)
        self.assertEqual(new_clusters, 5)
        # 快照表禁止改写
        with self.assertRaises((sqlite3.OperationalError, sqlite3.IntegrityError)):
            self.conn.execute("UPDATE snapshots SET note = 'x'")
        self.conn.rollback()

    def test_compare_distinguishes_fact_change_from_caliber_change(self):
        self._ingest_default()
        s1 = create_snapshot(self.conn, "2026-03", DEFAULT_CALIBER_V1)
        # 同输入换口径 v2：ICCID-only 行被认可为已确认独立终端
        s2 = create_snapshot(self.conn, "2026-03", DEFAULT_CALIBER_V2)
        diff = compare_snapshots(self.conn, s1["snapshot_id"], s2["snapshot_id"])
        self.assertFalse(diff["difference_drivers"]["raw_fact_change"])
        self.assertTrue(diff["difference_drivers"]["caliber_change"])
        r2 = build_report(self.conn, s2["snapshot_id"])
        r1 = build_report(self.conn, s1["snapshot_id"])
        self.assertEqual(
            r2["counts"]["confirmed_terminals"],
            r1["counts"]["confirmed_terminals"] + 1,
        )

    def test_aggregate_is_constraint_not_added_to_terminal_count(self):
        self._ingest_default()
        snap = create_snapshot(self.conn, "2026-03", DEFAULT_CALIBER_V1)
        report = build_report(self.conn, snap["snapshot_id"])
        self.assertEqual(report["counts"]["aggregate_constraints"], 1)
        # 报数 5，可相加明细覆盖 2 个物联网簇 -> 缺口 3，绝不把 5 加进终端总数
        self.assertEqual(report["counts"]["unexplained_aggregate_gap"], 3)
        agg_fact = next(f for f in report["aggregate_facts"])
        self.assertEqual(agg_fact["reported_count"], 5)
        self.assertEqual(agg_fact["detail_covered"], 2)
        self.assertEqual(agg_fact["unexplained_gap"], 3)

    def test_drill_paths_reach_raw_records(self):
        self._ingest_default()
        snap = create_snapshot(self.conn, "2026-03", DEFAULT_CALIBER_V1)
        term_fact = self.conn.execute(
            "SELECT fact_uid FROM connection_facts WHERE snapshot_id = ? "
            "AND fact_type = 'terminal' AND device_id = 'IMEI-1'",
            (snap["snapshot_id"],)).fetchone()["fact_uid"]
        drilled = drill_fact(self.conn, snap["snapshot_id"], term_fact)
        sources = {o["source_name"] for o in drilled["observations"]}
        self.assertEqual(sources, {"operator_a", "sample_platform"})
        self.assertTrue(all("raw_payload" in o["raw_record"]
                            for o in drilled["observations"]))
        self.assertTrue(any(e["basis"] == "shared_identifier"
                            for e in drilled["evidence"]))
        agg_fact = self.conn.execute(
            "SELECT fact_uid FROM connection_facts WHERE snapshot_id = ? "
            "AND fact_type = 'aggregate_constraint'",
            (snap["snapshot_id"],)).fetchone()["fact_uid"]
        agg_drill = drill_aggregate(self.conn, snap["snapshot_id"], agg_fact)
        self.assertEqual(agg_drill["covered_detail_count"], 2)
        self.assertEqual(len(agg_drill["covered_detail_terminals"]), 2)
        self.assertTrue(agg_drill["aggregate_observations"][0]["raw_payload"])

    def test_coverage_reports_per_source_counts(self):
        self._ingest_default()
        coverage = {c["source"]: c for c in source_coverage(self.conn, "2026-03")}
        self.assertEqual(coverage["operator_a"]["detail_rows"], 2)
        self.assertEqual(coverage["sample_platform"]["detail_rows"], 3)
        self.assertEqual(coverage["iot_registry"]["aggregate_rows"], 1)
        self.assertEqual(coverage["iot_registry"]["aggregate_reported_total"], 5)


class ManifestValidationTests(unittest.TestCase):
    def test_manifest_period_validation(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "m.json"
            p.write_text(json.dumps({"period": "2026/03", "files": []}),
                         encoding="utf-8")
            with self.assertRaises(ValueError):
                load_manifest(p)

    def test_caliber_rejects_name_similarity_merge(self):
        from conn_pipeline.config import load_caliber
        bad = dict(DEFAULT_CALIBER_V1)
        bad = json.loads(json.dumps(bad))
        bad["identity_policy"]["name_similarity_merge"] = True
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "bad.json"
            p.write_text(json.dumps(bad, ensure_ascii=False), encoding="utf-8")
            with self.assertRaises(ValueError):
                load_caliber(p)


if __name__ == "__main__":
    unittest.main()
