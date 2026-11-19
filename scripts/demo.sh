#!/usr/bin/env bash
# 端到端演练：两个报告期 + 迟到更正 + 口径修订，全部使用虚构终端标识。
# 在隔离模式（python3 -I）下运行，避免来源目录中的同名模块被导入。
set -euo pipefail

cd "$(dirname "$0")/.."
DB=build/demo_connections.db
rm -f "$DB"
mkdir -p build

run() { python3 -I run_pipeline.py --db "$DB" "$@"; }

echo '############ 2026-03 常规来源摄入与初版快照 ############'
PIPELINE_NOW='2026-04-05T08:00:00Z' run ingest data/demo/2026-03/manifest.json
PIPELINE_NOW='2026-04-05T08:05:00Z' run snapshot 2026-03 --note '3月初版（仅常规来源）'
run report 2026-03

echo
echo '############ 2026-03 迟到标识更正 -> 追加 revision 2（旧快照不变）############'
PIPELINE_NOW='2026-04-20T09:30:00Z' run ingest data/demo/late/manifest_2026-03.json
PIPELINE_NOW='2026-04-20T09:35:00Z' run snapshot 2026-03 --note '3月修订1：迟到标识更正'
run compare 2026-03 1 2

echo
echo '############ 同输入切换口径 v2 -> revision 3（事实未变、口径变化）############'
PIPELINE_NOW='2026-04-20T10:00:00Z' run snapshot 2026-03 --caliber-file data/calibers/caliber_v2.json \
    --note '3月修订2：仅口径切换为v2（基于含迟到证据的当前输入）'
run compare 2026-03 2 3
# 对照：同一 v2 口径作用于 rev1 的"原始输入"不可直接回退，故另用单元测试固定该场景；
# 这里再演示 v2 口径对"当前输入"下 ICCID 规则的指纹差异（与 v1 规则实质不同）
run reports

echo
echo '############ 2026-04 常规来源、SIM 换卡证据与适用区间 ############'
PIPELINE_NOW='2026-05-06T08:00:00Z' run ingest data/demo/2026-04/manifest.json
PIPELINE_NOW='2026-05-06T08:05:00Z' run snapshot 2026-04 --note '4月初版'
PIPELINE_NOW='2026-05-10T08:00:00Z' run ingest data/demo/late/manifest_2026-04.json
PIPELINE_NOW='2026-05-10T08:10:00Z' run snapshot 2026-04 --note '4月修订1：SIM换卡映射（3月旧映射自动失效）'
run compare 2026-04 1 2
run report 2026-04

echo
echo '############ 快照清单与来源覆盖 ############'
run reports
run coverage 2026-03

echo
echo "演练完成，数据库：$DB（可用 drill / drill-agg 对任一 fact_uid 下钻）"
