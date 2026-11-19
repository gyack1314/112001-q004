#!/usr/bin/env python3
"""管道根入口：把 src/ 加入 sys.path 后转交给 conn_pipeline.cli。

可用 `python3 -I run_pipeline ...` 在隔离模式下运行，避免来源目录中的
同名模块（如伪造的 json.py）被导入。
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from conn_pipeline.cli import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
