# HK Quant Strategy Platform

涓€涓彲鐙珛杩愯鐨勬腐鑲℃暟鎹姄鍙栦笌绛栫暐鐮旂┒浠撳簱銆傚綋鍓嶅凡鍖呭惈锛?
- `lib/hk_quant_strategy_platform/`: 鏁版嵁鎶撳彇銆佸瓨鍌ㄣ€佸埛鏂般€佸垎鏋愭牳蹇冩ā鍧?- `stock_data/`: 绀轰緥鏁版嵁锛堢簿绠€鐗堬級
- `鐑熻拏鑲＄瓫閫?ipynb`: 鐮旂┒涓庣瓫閫?notebook

## 1. 鐜瑕佹眰

- Python `3.11+`
- Windows / macOS / Linux 鍧囧彲

## 2. 蹇€熷紑濮?
```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
# source .venv/bin/activate

pip install -r requirements-dev.txt
```

## 3. 杩愯娴嬭瘯

```bash
pytest
```

## 4. Notebook/鑴氭湰涓殑鏁版嵁鐩綍

榛樿绀轰緥鏁版嵁鐩綍鏄粨搴撴牴鐩綍涓嬬殑 `stock_data/`銆?
寤鸿鎶?notebook 閲岀被浼间笅闈㈢殑纭紪鐮佽矾寰勶細

```python
r"E:\Program\Python\stock\data\stock_data"
```

鏀规垚鐩稿璺緞鎴栫幆澧冨彉閲忥紝渚嬪锛?
```python
from pathlib import Path
import os

storage_dir = Path(os.getenv("HK_VALUE_DATA_DIR", "stock_data")).resolve()
```

## 5. 鎵撳寘瀹夎锛堝彲閫夛級

濡傛灉甯屾湜鍦ㄤ换浣曠洰褰曢兘鑳?`import hk_quant_strategy_platform`锛?
```bash
pip install -e .
```

## 6. GitHub 浠撳簱寤鸿

- 涓嶈鎻愪氦 `.venv/`锛堝凡鍦?`.gitignore`锛?- 涓嶈鎻愪氦鏈湴瀵嗛挜鎴?`.env`
- 褰撳墠绀轰緥鏁版嵁鍙互鎻愪氦锛涜嫢鍚庣画鏁版嵁浣撶Н寰堝ぇ锛屽缓璁娇鐢?Git LFS 鎴栧皢鍏ㄩ噺鏁版嵁鏀惧璞″瓨鍌?
## 7. 鍒濆鍖栧苟鎺ㄩ€佸埌 GitHub

```bash
git init
git add .
git commit -m "Initial standalone hk value platform"
git branch -M main
git remote add origin <YOUR_GITHUB_REPO_URL>
git push -u origin main
```


