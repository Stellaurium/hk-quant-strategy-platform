# HK Quant Strategy Platform

一个可独立运行的港股数据抓取与策略研究仓库。当前已包含：

- `lib/hk_quant_strategy_platform/`: 数据抓取、存储、刷新、分析核心模块
- `stock_data/`: 示例数据（精简版）
- `src/notebooks/hk_value_screening.ipynb`: 研究与筛选 notebook

## 1. 环境要求

- Python `3.11+`
- Windows / macOS / Linux 均可

## 2. 快速开始

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
# source .venv/bin/activate

pip install -r requirements-dev.txt
```

## 3. 运行测试

```bash
pytest
```

## 4. Notebook/脚本中的数据目录

默认示例数据目录是仓库根目录下的 `stock_data/`。

建议把 notebook 里类似下面的硬编码路径：

```python
r"E:\Program\Python\stock\data\stock_data"
```

改成相对路径或环境变量，例如：

```python
from pathlib import Path
import os

storage_dir = Path(os.getenv("HK_VALUE_DATA_DIR", "stock_data")).resolve()
```

## 5. 打包安装（可选）

如果希望在任何目录都能 `import hk_quant_strategy_platform`：

```bash
pip install -e .
```

## 6. GitHub 仓库建议

- 不要提交 `.venv/`（已在 `.gitignore`）
- 不要提交本地密钥或 `.env`
- 当前示例数据可以提交；若后续数据体积很大，建议使用 Git LFS 或将全量数据放对象存储

## 7. 初始化并推送到 GitHub

```bash
git init
git add .
git commit -m "Initial standalone hk value platform"
git branch -M main
git remote add origin <YOUR_GITHUB_REPO_URL>
git push -u origin main
```
