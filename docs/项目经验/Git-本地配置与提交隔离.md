# Git 本地配置与提交隔离

## 背景

在日常开发里，经常会遇到几类“我不想提交上去”的改动：

- 本地临时调试代码
- 本地测试脚本或日志文件
- 带敏感信息的配置改动
- 只想本地生效的 `application.yaml` / `application.yml` 差异

这类场景最容易踩的坑是：

1. 误把本地测试代码提交上去
2. 误把敏感信息提交上去
3. 切分支时本地改动冲突
4. 以为 `.gitignore` 能忽略一切，结果对已跟踪文件无效

本文基于 Git 官方文档和当前项目里的实际用法，总结几种处理方式及适用场景。

## 1. 最常用的三种方式

### 1.1 `git stash`

`git stash` 的作用可以理解成：

> 把当前工作区改动临时收起来，工作目录恢复干净，之后需要时再恢复。

官方文档说明：
- `git stash push -u` 可以连未跟踪文件一起保存。[git-stash 文档](https://github.com/git/htmldocs/blob/gh-pages/git-stash.html)
- `git stash apply` 会恢复 stash，但不会删除 stash 记录；`pop` 则会恢复并删除记录。[git-stash 文档](https://github.com/git/htmldocs/blob/gh-pages/git-stash.adoc)
- `git stash push <pathspec>` 可以只 stash 指定文件，对这些文件回滚到 `HEAD`，不影响其他文件。[git-stash 文档](https://github.com/git/htmldocs/blob/gh-pages/git-stash.adoc)

#### 最常用命令

```bash
git stash
git stash push -m "temp debug"
git stash push -u -m "local temp files"
git stash push -m "save local config" -- bootstrap/src/main/resources/application.yaml mcp-server/src/main/resources/application.yml
```

查看 stash：

```bash
git stash list
git stash show -p stash@{0}
```

恢复 stash：

```bash
git stash apply stash@{0}
git stash pop
```

删除 stash：

```bash
git stash drop stash@{0}
git stash clear
```

#### 什么时候最适合用 stash

- 临时切分支
- 本地调试代码先藏起来
- 已跟踪配置文件不想提交
- 本地测试一半，之后还想恢复

#### 在当前项目里的典型例子

本项目里最典型的就是：
- `bootstrap/src/main/resources/application.yaml`
- `mcp-server/src/main/resources/application.yml`

这两个文件已经被 Git 跟踪，所以不能靠 `.gitignore` 解决。本地如果只想保留自己调试配置，最稳妥的方式就是：

```bash
git stash push -m "save local config" -- \
  bootstrap/src/main/resources/application.yaml \
  mcp-server/src/main/resources/application.yml
```

### 1.2 不 `git add`

如果一个文件已经改了，但你只是：

> 这次不想提交它

那么最简单的做法是：

```bash
git add docs/项目经验
git commit -m "docs: add note"
```

没有被 `git add` 的文件不会进这次提交。

这种方式适合：
- 这次提交只带一部分改动
- 你还要继续在当前分支改剩余内容
- 这些改动不需要暂时藏起来

#### 优点
- 最简单
- 不改变工作区
- 适合短期拆分提交

#### 缺点
- 文件仍然留在工作区
- 切分支时可能冲突
- 容易误执行 `git add .` 把它带进去
- 对敏感配置不够保险

### 1.3 `.gitignore`

`.gitignore` 的语义是：

> 指定哪些“未跟踪文件”应该被 Git 忽略。

官方文档说明：
- `.gitignore` 用于 intentionally untracked files。[gitignore 文档](https://github.com/git/htmldocs/blob/gh-pages/gitignore.adoc)
- 对已经被跟踪的文件不生效。[gitignore 文档](https://github.com/git/htmldocs/blob/gh-pages/gitignore.adoc)
- 如果一个文件之前已经被跟踪，想改成忽略，必须先 `git rm --cached`。[gitfaq 文档](https://github.com/git/htmldocs/blob/gh-pages/gitfaq.html)

#### 示例

```gitignore
.env.local
*.log
tmp/
local-debug/
```

#### 适合什么

- 本地新增的环境文件
- 日志文件
- 临时输出目录
- 测试脚本生成物

#### 不适合什么

- 已经被仓库跟踪的文件
- 临时不想提交的代码片段
- 已跟踪配置文件的本地差异

---

## 2. 三种方式怎么选

| 方式 | 核心目的 | 适合场景 | 对已跟踪文件是否有效 | 是否容易误提交 |
|---|---|---|---|---|
| `git stash` | 临时收起改动，之后恢复 | 切分支、本地调试、配置隔离 | 是 | 低 |
| 不 `git add` | 这次不提交 | 拆分提交、短期保留改动 | 是 | 中 |
| `.gitignore` | 从源头忽略未跟踪文件 | `.env`、日志、临时文件 | 否（对已跟踪文件无效） | 低 |

### 快速决策法

#### 情况 A：以后还想要这些改动
- 用 `git stash`

#### 情况 B：只是这次不提交
- 不 `git add`

#### 情况 C：这是本地新文件，以后长期都不想跟踪
- 用 `.gitignore`

#### 情况 D：这是已跟踪配置文件，本地差异只想自己保留
- 优先 `git stash`
- 长期建议拆出本地 override 文件，而不是一直改主配置文件

---

## 3. 为什么 `.gitignore` 不能解决 `application.yaml` 这种问题

Git 官方 FAQ 明确说明：

> Git 没有直接提供一种“忽略已跟踪文件改动”的可靠机制；`skip-worktree` 和 `assume-unchanged` 也不推荐作为这个用途。[gitfaq 文档](https://github.com/git/htmldocs/blob/gh-pages/gitfaq.adoc)

换句话说：
- `application.yaml` 已经在仓库里
- 你本地改了它
- 就算把它写进 `.gitignore`，Git 仍然会继续跟踪它

所以对于这类文件，正确思路通常是：

### 方案 1：短期用 `git stash`

```bash
git stash push -m "local config" -- bootstrap/src/main/resources/application.yaml
```

### 方案 2：长期拆分本地覆盖配置

例如改造成：
- 仓库里保留公共基线配置
- 本地用 `application-local.yaml` 或环境变量覆盖
- 把本地文件加入 `.gitignore`

这样才是更健康的长期方案。

---

## 4. 其他容易被误用的方式

### 4.1 `git update-index --skip-worktree`

很多人会拿它来做“本地配置别提示我改了”。

官方文档说明：
- `skip-worktree` 与 `assume-unchanged` 含义不同，本意不是拿来长期忽略已跟踪配置文件的改动。[git-update-index 文档](https://github.com/git/htmldocs/blob/gh-pages/git-update-index.adoc)
- Git 官方明确说，不推荐把它们当成“忽略 tracked file 改动”的标准方案。[git-update-index 文档](https://github.com/git/htmldocs/blob/gh-pages/git-update-index.adoc) [gitfaq 文档](https://github.com/git/htmldocs/blob/gh-pages/gitfaq.adoc)

因此，这种方式：
- 容易忘
- 容易让本地状态和团队状态脱节
- 排查问题麻烦

不建议作为日常主方案。

### 4.2 `git restore`

`git restore` 适合的是：

> 我确定不要这些改动了，直接丢掉

例如：

```bash
git restore file
git restore .
```

它不是“临时藏起来”，而是“明确不要了”。

---

## 5. 当前项目最推荐的实践

### 5.1 提交前只 add 你明确要提交的文件

比如：

```bash
git add docs/项目经验
git commit -m "docs: add note"
```

尽量少用：

```bash
git add .
```

因为这很容易把：
- 本地配置
- 调试代码
- 临时测试文件

一起带进去。

### 5.2 已跟踪配置文件的本地差异优先用 stash

当前项目最典型的文件是：
- `bootstrap/src/main/resources/application.yaml`
- `mcp-server/src/main/resources/application.yml`

这类文件本地测试很常见，但不应该轻易混进正式提交。优先用：

```bash
git stash push -m "save local config" -- \
  bootstrap/src/main/resources/application.yaml \
  mcp-server/src/main/resources/application.yml
```

### 5.3 本地新增文件才交给 `.gitignore`

例如：
- `.env.local`
- `logs/`
- `tmp/`
- 本地调试脚本输出

### 5.4 stash 尽量写说明

推荐：

```bash
git stash push -m "mineru local debug"
git stash push -m "sa-token config test"
```

不推荐：

```bash
git stash
git stash
git stash
```

后者过几天基本很难分清哪个 stash 是干什么的。

---

## 6. 一句话结论

- `git stash`：适合“暂时不想提交，但以后还要恢复”的改动
- 不 `git add`：适合“这次提交不带它，但改动继续留在工作区”
- `.gitignore`：适合“本地新文件从一开始就不想让 Git 跟踪”
- 对当前项目里的 `application.yaml` / `application.yml` 这类已跟踪配置文件，优先用 `stash`，长期再考虑本地 override 配置方案

## 相关资料

- [git-stash 文档](https://github.com/git/htmldocs/blob/gh-pages/git-stash.adoc)
- [git-stash HTML 文档](https://github.com/git/htmldocs/blob/gh-pages/git-stash.html)
- [gitignore 文档](https://github.com/git/htmldocs/blob/gh-pages/gitignore.adoc)
- [gitfaq 文档（关于 tracked file 与 ignore）](https://github.com/git/htmldocs/blob/gh-pages/gitfaq.adoc)
- [git-update-index 文档](https://github.com/git/htmldocs/blob/gh-pages/git-update-index.adoc)
