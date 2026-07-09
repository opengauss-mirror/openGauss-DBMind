# openGauss 三仓 pre-commit 开发者使用指南

> 面向对象：在 oGMemory / mcp-opengauss / openGauss-DBMind 上提交代码的开发者。
> 只讲**怎么装、怎么用、报错怎么办**。想了解设计原理见《实现方式详解》。

---

## 0. 一句话背景
提交代码时会自动做格式化、lint、类型、拼写、安全检查，**目的是让你在本地就修掉问题，
不用等推送后返工**。开发环境连不了 github，工具都从 gitcode 拉，所以安装步骤和平时略有不同。

---

## 1. 首次安装（每台开发机做一次）

### 方式 A：一键脚本（推荐）

仓库根目录已提供 `setup-pre-commit.sh`，它把所有工具装进仓库下的独立虚拟环境
`.pre-commit-venv/`，**不污染你的系统 python、不修改全局 pip 配置**，删除该目录即可完全卸载。

```bash
cd <仓库根目录>
bash setup-pre-commit.sh
# 如需换源： PIP_MIRROR=https://pypi.tuna.tsinghua.edu.cn/simple bash setup-pre-commit.sh
```

安装成功的标志：末尾出现 `完成！之后每次 git commit 会自动检查本次改动`。
之后正常 `git commit` 即可，钩子会自动使用该隔离环境里的工具，无需手动激活。

### 方式 B：手动安装（了解每一步时用）

> ⚠️ 最容易踩的坑：pre-commit 给工具建环境时若走默认 pypi 源，会报
> `Could not find a version ... ruamel.yaml`。下面用**临时指定源**的方式规避，
> 且**不建议** `pip config set`（那会全局改你的 pip、影响其它项目）。

```bash
# 建议先建独立虚拟环境，避免污染系统 python（现代系统还会因 PEP668 直接拒绝系统装包）
python3 -m venv .pre-commit-venv
source .pre-commit-venv/bin/activate

# ① 装 pre-commit 和 mypy（-i 仅对本条命令临时指定源，不写全局配置）
pip install -i https://mirrors.aliyun.com/pypi/simple/ pre-commit mypy
#    仅 openGauss-DBMind 仓额外需要 flake8：
pip install -i https://mirrors.aliyun.com/pypi/simple/ flake8

# ② 安装钩子
pre-commit install

# ③ 预热：用环境变量临时把源指到镜像（仅本次命令生效，不影响其它项目）
PIP_INDEX_URL=https://mirrors.aliyun.com/pypi/simple/ \
PIP_TRUSTED_HOST=mirrors.aliyun.com \
  pre-commit install-hooks
```

安装成功的标志：`install-hooks` 输出若干 `Installing environment for ...` 且无 ERROR。

---

## 2. 日常怎么用

**装好后就是全自动的。** 你照常 `git add` + `git commit` 即可：

```
git commit 时，钩子只对你本次改动的文件运行：
  · 全部通过         → 提交成功，无感
  · 有可自动修的问题 → ruff/darker 直接改好文件，但【提交会中止】
                       → 你 git add 这些被改好的文件，再 commit 一次即可
  · 有需手动改的问题 → 按提示改完，git add，重新 commit
```

### 典型流程示例
```bash
$ git add my_module.py
$ git commit -m "feat: add xxx"
# ruff 修了 import 顺序、darker 格式化了你改的行 → 提交被中止
$ git add my_module.py          # 把自动修改的结果加入暂存
$ git commit -m "feat: add xxx" # 这次通过
```

### 手动运行（不必等到 commit）
```bash
pre-commit run --files a.py b.py   # 只检查指定文件
pre-commit run --all-files         # 全仓检查（慎用：存量代码会大量报错）
```

---

## 3. 各检查项一览

| 检查 | 工具 | 触发范围 | 会自动改文件吗 |
|------|------|----------|----------------|
| 格式化 | darker | 你改动的**行** | ✅ 会 |
| Lint | ruff（DBMind 是 flake8） | 你改动的**文件** | ruff `--fix` 会 |
| 类型 | mypy | 你改动的文件 | ❌ 只报告 |
| 拼写 | codespell | 你改动的文件 | ❌ 只报告 |
| 安全 | bandit + 私钥检测 | 你改动的文件 | ❌ 只报告 |
| 文件卫生 | 行尾空格/末尾换行/yaml等 | 文件 | ✅ 会（清理空格等） |

**你没碰的老代码不会被检查**；一旦你改了某个老文件，该文件里你新增/改动的内容会被检查。

---

## 4. 怎么屏蔽（当检查挡住你、但确实该放行时）

按场景选，**优先用不改老代码行的方式**：

### 场景 A：改到老文件，报的是你没碰的老代码问题
在**配置文件**里集中忽略，不要去改那行老代码：

```toml
# oGMemory / mcp-opengauss：pyproject.toml
[tool.ruff.lint.per-file-ignores]
"src/legacy.py" = ["E501"]        # 这个文件忽略"行太长"
```
```ini
# openGauss-DBMind：flake8.conf 里加 per-file-ignores，或 mypy.ini / .bandit 对应字段
```

### 场景 B：你本次新写的代码，个别地方确需特例
在**你自己写的那一行**行尾加注释（注意：代码和 `#` 之间空两格）：

```python
result = compute()  # noqa: E501               # 屏蔽 ruff/flake8
data = load()  # type: ignore[assignment]      # 屏蔽 mypy
os.system(cmd)  # nosec B605                    # 屏蔽 bandit
myword  # codespell:ignore                      # 屏蔽拼写
```

> ⚠️ 不要跑到"你本次没碰的老代码行"末尾去加 `# noqa`。那样会把那行卷进本次改动，
> 反而触发格式化工具去动它。老代码问题一律用场景 A 的配置文件方式。

### 场景 C：紧急上线 / 工具本身故障，需要立刻提交
```bash
SKIP=mypy,bandit git commit -m "..."   # 只跳过指定的钩子
git commit --no-verify -m "..."        # 跳过所有钩子（仅限紧急，事后补检查）
```

---

## 5. 常见问题 FAQ

**Q1. `install-hooks` 报 `Could not find a version that satisfies ... ruamel.yaml`？**
A. pre-commit 建环境时没走国内源。最省事是直接用 `bash setup-pre-commit.sh`；
   手动的话按第 1 步方式 B，用 `PIP_INDEX_URL=<国内源> pre-commit install-hooks`，
   必要时先 `pre-commit clean` 再重试。

**Q2. 提示 `pre-commit: command not found`？**
A. 用一键脚本时无需全局 pre-commit（它在 `.pre-commit-venv/` 里）。
   手动安装时确认已 `source .pre-commit-venv/bin/activate` 或用了同一 python 环境。

**Q3. commit 时报 `mypy: command not found` 或 flake8 找不到？**
A. 这两个走本机（local）。一键脚本已装进 `.pre-commit-venv/`；手动则在该环境里
   `pip install -i <国内源> mypy`（DBMind 另需 flake8）。

**Q3.1 系统报 `externally-managed-environment` / PEP668，装不了包？**
A. 别用 `--break-system-packages` 去污染系统 python。用虚拟环境：
   一键脚本已自动建 `.pre-commit-venv/`；手动则先 `python3 -m venv .pre-commit-venv && source .pre-commit-venv/bin/activate`。

**Q4. clone gitcode 很慢或失败？**
A. 确认能访问 `https://gitcode.com`。本套配置所有工具源都指向 gitcode，不连 github。

**Q5. 我只想看看会改什么，不想真的改？**
A. `pre-commit run --files x.py` 后用 `git diff` 看改动；或提交前先 `git stash` 备份。

**Q6. 钩子把我的文件改了，我不认可这些改动？**
A. 格式化/lint 的自动修改是按仓库规则来的。若确属规则不合理，走第 4 节屏蔽，
   或反馈维护者调整 `pyproject.toml` / `flake8.conf` 规则，不要长期用 `--no-verify` 绕过。

**Q7. 换了新电脑 / 新克隆的仓库要重新做吗？**
A. 是。每台机器、每个本地克隆都要重新跑一次 `bash setup-pre-commit.sh`
   （`.git/` 和 `.pre-commit-venv/` 都不随仓库分发）。

---

## 6. 速查卡

```
装：  bash setup-pre-commit.sh          （一键，装进 .pre-commit-venv/，不污染系统）
用：  正常 git commit（自动检查改动文件；被改后 git add 再提交）
查：  pre-commit run --files x.py
屏蔽：老代码→配置文件 per-file-ignores ；新代码→行尾 # noqa/# type: ignore/# nosec
救火：SKIP=<hook> git commit   或   git commit --no-verify
```