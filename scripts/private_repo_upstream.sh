#!/usr/bin/env bash
set -euo pipefail

# 用途：
# 1. 把当前仓库迁移到一个新的 GitHub private 仓库（origin）
# 2. 保留原开源仓库为 upstream，后续可继续同步作者更新
# 3. 可选保留当前 public fork 为 public-fork remote
#
# 典型用法：
#   bash scripts/private_repo_upstream.sh migrate liuzekuan/ragent-private
#   bash scripts/private_repo_upstream.sh sync
#
# 可选环境变量：
#   UPSTREAM_URL=https://github.com/nageoffer/ragent.git
#   DEFAULT_BRANCH=main
#   REMOTE_PROTOCOL=https            # https | ssh
#   SYNC_STRATEGY=merge             # merge | rebase
#   AUTO_CONFIRM=true               # true 时跳过确认
#   PUBLIC_FORK_REMOTE=public-fork
#   ORIGIN_REMOTE=origin
#   UPSTREAM_REMOTE=upstream

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

DEFAULT_BRANCH="${DEFAULT_BRANCH:-main}"
UPSTREAM_URL="${UPSTREAM_URL:-https://github.com/nageoffer/ragent.git}"
REMOTE_PROTOCOL="${REMOTE_PROTOCOL:-https}"
SYNC_STRATEGY="${SYNC_STRATEGY:-merge}"
AUTO_CONFIRM="${AUTO_CONFIRM:-false}"
PUBLIC_FORK_REMOTE="${PUBLIC_FORK_REMOTE:-public-fork}"
ORIGIN_REMOTE="${ORIGIN_REMOTE:-origin}"
UPSTREAM_REMOTE="${UPSTREAM_REMOTE:-upstream}"

info() {
  echo -e "${BLUE}[INFO]${RESET} $*"
}

ok() {
  echo -e "${GREEN}[OK]${RESET} $*"
}

warn() {
  echo -e "${YELLOW}[WARN]${RESET} $*"
}

err() {
  echo -e "${RED}[ERROR]${RESET} $*" >&2
}

usage() {
  cat <<EOF
${BOLD}private_repo_upstream.sh${RESET}

用途：把当前仓库迁移到 private GitHub 仓库，并保留 upstream 同步能力。

命令：
  migrate <owner/repo>   创建/连接 private 仓库，设置 origin/upstream/public-fork
  sync [branch]          从 upstream 同步指定分支（默认：${DEFAULT_BRANCH}）并 push 到 origin
  remotes                查看当前 remote 配置

示例：
  bash scripts/private_repo_upstream.sh migrate liuzekuan/ragent-private
  AUTO_CONFIRM=true bash scripts/private_repo_upstream.sh migrate liuzekuan/ragent-private
  bash scripts/private_repo_upstream.sh sync
  SYNC_STRATEGY=rebase bash scripts/private_repo_upstream.sh sync main
EOF
}

require_cmd() {
  local cmd="$1"
  command -v "$cmd" >/dev/null 2>&1 || {
    err "缺少命令：$cmd"
    exit 1
  }
}

require_git_repo() {
  git rev-parse --is-inside-work-tree >/dev/null 2>&1 || {
    err "当前目录不是 Git 仓库"
    exit 1
  }
}

remote_exists() {
  git remote get-url "$1" >/dev/null 2>&1
}

remote_url() {
  git remote get-url "$1" 2>/dev/null || true
}

build_repo_url() {
  local slug="$1"
  case "$REMOTE_PROTOCOL" in
    https) echo "https://github.com/${slug}.git" ;;
    ssh) echo "git@github.com:${slug}.git" ;;
    *)
      err "不支持的 REMOTE_PROTOCOL：$REMOTE_PROTOCOL（仅支持 https / ssh）"
      exit 1
      ;;
  esac
}

confirm() {
  local prompt="$1"
  if [[ "$AUTO_CONFIRM" == "true" ]]; then
    return 0
  fi
  read -r -p "$prompt [y/N] " answer
  [[ "$answer" == "y" || "$answer" == "Y" ]]
}

check_gh_auth() {
  require_cmd gh
  if ! gh auth status >/dev/null 2>&1; then
    err "gh 未登录，请先执行：gh auth login"
    exit 1
  fi
}

show_remotes() {
  echo
  info "当前 remotes："
  git remote -v || true
  echo
}

ensure_upstream_remote() {
  if remote_exists "$UPSTREAM_REMOTE"; then
    git remote set-url "$UPSTREAM_REMOTE" "$UPSTREAM_URL"
    ok "已更新 ${UPSTREAM_REMOTE} -> ${UPSTREAM_URL}"
    return
  fi

  if remote_exists "$ORIGIN_REMOTE" && [[ "$(remote_url "$ORIGIN_REMOTE")" == "$UPSTREAM_URL" ]]; then
    git remote rename "$ORIGIN_REMOTE" "$UPSTREAM_REMOTE"
    ok "已将 ${ORIGIN_REMOTE} 重命名为 ${UPSTREAM_REMOTE}"
    return
  fi

  git remote add "$UPSTREAM_REMOTE" "$UPSTREAM_URL"
  ok "已添加 ${UPSTREAM_REMOTE} -> ${UPSTREAM_URL}"
}

preserve_old_origin_if_needed() {
  local target_url="$1"

  if ! remote_exists "$ORIGIN_REMOTE"; then
    return
  fi

  local current_origin
  current_origin="$(remote_url "$ORIGIN_REMOTE")"

  if [[ "$current_origin" == "$target_url" ]]; then
    return
  fi

  if [[ "$current_origin" == "$UPSTREAM_URL" ]]; then
    git remote rename "$ORIGIN_REMOTE" "$UPSTREAM_REMOTE"
    ok "原 ${ORIGIN_REMOTE} 指向上游，已重命名为 ${UPSTREAM_REMOTE}"
    return
  fi

  if remote_exists "$PUBLIC_FORK_REMOTE"; then
    git remote set-url "$PUBLIC_FORK_REMOTE" "$current_origin"
    git remote remove "$ORIGIN_REMOTE"
    ok "已保留原 ${ORIGIN_REMOTE} 为 ${PUBLIC_FORK_REMOTE}"
  else
    git remote rename "$ORIGIN_REMOTE" "$PUBLIC_FORK_REMOTE"
    ok "已将原 ${ORIGIN_REMOTE} 重命名为 ${PUBLIC_FORK_REMOTE}"
  fi
}

ensure_origin_remote() {
  local target_url="$1"
  if remote_exists "$ORIGIN_REMOTE"; then
    git remote set-url "$ORIGIN_REMOTE" "$target_url"
  else
    git remote add "$ORIGIN_REMOTE" "$target_url"
  fi
  ok "已设置 ${ORIGIN_REMOTE} -> ${target_url}"
}

create_private_repo_if_needed() {
  local slug="$1"

  if gh repo view "$slug" >/dev/null 2>&1; then
    ok "GitHub 仓库已存在：$slug"
    return
  fi

  info "正在创建 private 仓库：$slug"
  gh repo create "$slug" --private --description "Private mirror with upstream sync for $(basename "$(git rev-parse --show-toplevel)")"
  ok "已创建 private 仓库：$slug"
}

list_push_branches() {
  git for-each-ref refs/heads --format='%(refname:short)' | while IFS= read -r branch; do
    [[ -z "$branch" ]] && continue
    [[ "$branch" == worktree-* ]] && continue
    echo "$branch"
  done
}

push_branches() {
  local branch
  while IFS= read -r branch; do
    [[ -z "$branch" ]] && continue
    info "推送分支：$branch"
    git push -u "$ORIGIN_REMOTE" "$branch"
  done < <(list_push_branches)

  if git tag --list | grep -q .; then
    info "推送 tags"
    git push "$ORIGIN_REMOTE" --tags
  fi
}

set_default_tracking() {
  if git show-ref --verify --quiet "refs/heads/${DEFAULT_BRANCH}"; then
    git branch --set-upstream-to="${ORIGIN_REMOTE}/${DEFAULT_BRANCH}" "$DEFAULT_BRANCH" >/dev/null 2>&1 || true
    ok "已尝试设置 ${DEFAULT_BRANCH} 跟踪 ${ORIGIN_REMOTE}/${DEFAULT_BRANCH}"
  fi
}

require_clean_worktree_for_sync() {
  if ! git diff --quiet || ! git diff --cached --quiet; then
    err "工作区有未提交修改，sync 前请先提交或 stash"
    exit 1
  fi
}

cmd_migrate() {
  local target_slug="${1:-${TARGET_REPO_SLUG:-}}"
  if [[ -z "$target_slug" ]]; then
    err "缺少目标仓库 slug，例如：liuzekuan/ragent-private"
    usage
    exit 1
  fi

  require_git_repo
  check_gh_auth

  local target_url
  target_url="$(build_repo_url "$target_slug")"

  local repo_root
  repo_root="$(git rev-parse --show-toplevel)"

  echo
  info "即将执行迁移："
  echo "  仓库目录      : $repo_root"
  echo "  private 仓库  : $target_slug"
  echo "  origin        : $target_url"
  echo "  upstream      : $UPSTREAM_URL"
  echo "  sync 策略     : $SYNC_STRATEGY"
  echo

  if ! confirm "确认继续迁移到 private 仓库吗？"; then
    warn "已取消"
    exit 0
  fi

  create_private_repo_if_needed "$target_slug"
  preserve_old_origin_if_needed "$target_url"
  ensure_upstream_remote
  ensure_origin_remote "$target_url"
  git fetch "$UPSTREAM_REMOTE" --prune || true
  push_branches
  set_default_tracking
  show_remotes

  ok "迁移完成。后续同步原作者代码可执行："
  echo "  bash scripts/private_repo_upstream.sh sync"
}

cmd_sync() {
  local branch="${1:-$DEFAULT_BRANCH}"
  require_git_repo
  require_clean_worktree_for_sync

  if ! remote_exists "$UPSTREAM_REMOTE"; then
    err "缺少 upstream remote，请先执行 migrate"
    exit 1
  fi
  if ! remote_exists "$ORIGIN_REMOTE"; then
    err "缺少 origin remote，请先执行 migrate"
    exit 1
  fi

  local current_branch
  current_branch="$(git branch --show-current)"

  info "抓取 upstream 更新"
  git fetch "$UPSTREAM_REMOTE" --prune

  if [[ "$current_branch" != "$branch" ]]; then
    info "切换到分支：$branch"
    git switch "$branch"
  fi

  case "$SYNC_STRATEGY" in
    merge)
      info "合并 ${UPSTREAM_REMOTE}/${branch} -> ${branch}"
      git merge --no-edit "${UPSTREAM_REMOTE}/${branch}"
      ;;
    rebase)
      info "变基 ${branch} onto ${UPSTREAM_REMOTE}/${branch}"
      git rebase "${UPSTREAM_REMOTE}/${branch}"
      ;;
    *)
      err "不支持的 SYNC_STRATEGY：$SYNC_STRATEGY（仅支持 merge / rebase）"
      exit 1
      ;;
  esac

  info "推送同步后的分支到 ${ORIGIN_REMOTE}/${branch}"
  git push "$ORIGIN_REMOTE" "$branch"

  if [[ "$current_branch" != "$branch" ]]; then
    git switch "$current_branch"
  fi

  ok "同步完成"
}

main() {
  local command="${1:-}"
  case "$command" in
    migrate)
      shift
      cmd_migrate "$@"
      ;;
    sync)
      shift
      cmd_sync "$@"
      ;;
    remotes)
      require_git_repo
      show_remotes
      ;;
    -h|--help|help|"")
      usage
      ;;
    *)
      err "未知命令：$command"
      usage
      exit 1
      ;;
  esac
}

main "$@"
