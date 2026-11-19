# 通信网络连接总量核对管道

按月核对通信网络连接总量的数据管道。解决"把来源数直接相加"导致的重复观测、
无法区分真实新增与重复、月末数字变化无法解释的问题。**所有演练标识均为虚构。**

## 核心设计原则

1. **来源原貌只追加、永不改写**：每个来源文件按字节哈希登记，逐行 JSON 原样存入
   `raw_records`；landing/观测/证据表由 SQLite 触发器拒绝任何 `UPDATE`/`DELETE`。
2. **关联只认真实证据**：只有来源实际提供的强标识共现（`co_occurrence`）或显式
   标识映射（`identifier_mapping`，且适用区间覆盖报告期）才能把两个观测并为同一终端。
   名称相似**永不合并**，只在报表中给出"保持独立"的诊断提示。
3. **证据不足保留未判定**：无法与任何证据关联的观测保持独立，状态为 `undetermined`，
   单列计数，不与已确认数混算。
4. **口径与事实分离**：汇总口径（哪些来源可相加、哪些只是交叉观测、哪些仅作约束）
   独立版本化；改口径不动任何原始数据，对比时分别标注"事实变化"与"口径变化"。
5. **快照不可变，迟到证据只追加**：报告期快照一经提交即历史；迟到的标识更正只能
   生成新的 `revision`，旧快照行原样保留，可随时比较修订前后。

## 来源角色（口径 v1）

| 来源 | 粒度 | 角色 | 处理方式 |
| --- | --- | --- | --- |
| `operator_a` | 终端明细 | `additive_population` | 可相加的不同对象总体 |
| `iot_registry` | 终端明细 | `additive_population` | 可相加的不同对象总体（物联网） |
| `sample_platform` | 终端明细 | `cross_observation` | 对同一终端的交叉观测，**绝不与账面相加** |
| `iot_registry` | `account_total` | `constraint` | 只有汇总数、无明细，作为覆盖约束，差额保留为无法解释缺口 |

口径 v2（`data/calibers/caliber_v2.json`）演示口径变化：认可 ICCID 为物联网终端
法定服务标识后，仅含 ICCID 的行从 `undetermined` 变为 `confirmed`，原始事实不变。

## 分层架构

```
data/demo/                     虚构来源文件 + 期清单 manifest.json + 迟到证据
data/calibers/                 口径版本 JSON（规则实质变化才产生新版本）
src/conn_pipeline/
  storage.py     SQLite schema、连接、追加写/不可变触发器
  config.py      口径模型、期清单解析与校验（禁止名称相似合并）
  utils.py       内容哈希、确定性 ID、证据适用期判断（按月）
  ingestion.py   landing（原样保存）+ staging（观测/汇总/证据规范化）
  identity.py    DSU 并查集身份解析：仅显式证据并簇，其余保持未判定
  snapshot.py    口径注册、不可变快照与连接事实、汇总覆盖约束
  reporting.py   报表、来源覆盖、终端/汇总下钻、修订对比
  cli.py         命令行入口
run_pipeline.py  根入口（把 src/ 加入 sys.path，支持 python3 -I 隔离运行）
scripts/demo.sh  端到端演练
tests/           契约测试 + 15 项管道保证测试
```

## 快速开始

```bash
# 语法检查
python3 -m compileall src

# 单元测试（隔离模式，避免来源目录同名模块被导入）
python3 -I -m unittest discover -s tests

# 一键端到端演练（两个报告期、迟到更正、口径修订）
bash scripts/demo.sh
```

## 命令说明

所有命令通过根入口运行，默认数据库 `build/connections.db`（git 忽略）：

```bash
# 1. 按期清单摄入：原样保存来源并规范化（同文件重复摄入自动跳过）
python3 -I run_pipeline.py --db build/connections.db ingest data/demo/2026-03/manifest.json

# 2. 构建并提交不可变快照（--caliber v1|v2 或 --caliber-file 自定义口径）
python3 -I run_pipeline.py --db build/connections.db snapshot 2026-03 --note '3月初版'

# 3. 报告期报表：口径、来源覆盖、已确认/未判定数量、汇总缺口、名称相似诊断
python3 -I run_pipeline.py --db build/connections.db report 2026-03
python3 -I run_pipeline.py --db build/connections.db --json report 2026-03

# 4. 来源覆盖与快照清单
python3 -I run_pipeline.py --db build/connections.db coverage 2026-03
python3 -I run_pipeline.py --db build/connections.db reports

# 5. 下钻：终端事实 -> 终端簇 -> 关联观测 -> 原始行 -> 关联证据
python3 -I run_pipeline.py --db build/connections.db drill 2026-03 <fact_uid>
#    汇总事实 -> 汇总原始行 -> 可相加明细覆盖到的终端簇（按簇去重）
python3 -I run_pipeline.py --db build/connections.db drill-agg 2026-03 <fact_uid>

# 6. 同期修订对比：区分原始事实变化与口径变化
python3 -I run_pipeline.py --db build/connections.db compare 2026-03 1 2
```

时间戳可用环境变量 `PIPELINE_NOW` 固定（演练脚本据此复现提交时间）。

## 演练数据场景（全部虚构）

- **跨网络漫游**：平台采样只给 IMSI、账面只给 IMEI，迟到映射到达前保持两个未判定/
  待关联实体；映射适用期覆盖 3 月才并为同一已确认终端。
- **更换标识（SIM 换卡）**：同一终端 IMSI 由 `IMSI-9003` 变为 `IMSI-9103`，
  运营商工单提供新旧 IMSI 显式映射；3 月漫游映射的适用区间在 4 月自动失效。
- **同时出现在多个来源**：`IMEI-1001` 同时在账面与平台出现，靠共享 device_id
  并为一个终端，平台侧不重复相加。
- **只有汇总数**：`account_total=7` 无逐终端明细，仅生成约束事实；可相加明细
  覆盖到的终端按簇去重计数，差额 4/5 保留为"无法解释缺口"，不硬凑数字。
- **证据不足**：孤儿 ICCID、平台独立设备保持 `undetermined`；`IMEI-2001` 与
  `IMEI-2001X` 名称高度相似但无证据，诊断提示但保持独立。
- **迟到更正**：4 月才送达的 3 月映射生成 revision 2，revision 1 快照原样可查。

## 数据模型要点

- `ingest_batches` / `raw_records`：追加写 landing 层，含文件与逐行 SHA-256。
- `observations` / `aggregate_observations`：规范化后的终端观测与汇总观测。
- `identity_evidence`：共现与显式映射证据，带 `applies_from/applies_to` 适用区间。
- `snapshots` / `clusters` / `cluster_observations` / `connection_facts` /
  `fact_evidence`：按快照分区的不可变事实层，支持从汇总一路下钻到原始行与证据。
- `caliber_versions`：口径版本，指纹只取规则实质（角色与身份策略），描述文字不参与。
