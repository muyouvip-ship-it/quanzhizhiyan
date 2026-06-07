#!/usr/bin/env bash
# release.sh — 发版自动化脚本
#
# 用法:
#   bash scripts/release.sh [major|minor|patch]
#
# 流程:
#   1. 根据参数或 commit message 决定 bump 级别
#   2. 更新 pyproject.toml 版本号
#   3. pip install -e . 同步到 venv
#   4. 更新 CHANGELOG.md（追加新版本区块）
#   5. git add + commit
#   6. git tag vX.Y.Z
#   7. git push + git push --tags

set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$REPO_ROOT"

PYPROJECT="$REPO_ROOT/pyproject.toml"
CHANGELOG="$REPO_ROOT/CHANGELOG.md"
VENV_PYTHON="$REPO_ROOT/.venv/bin/python"

# ── Determine bump level ────────────────────────────────────────────────
bump="${1:-patch}"
if [ "$bump" != "major" ] && [ "$bump" != "minor" ] && [ "$bump" != "patch" ]; then
    echo "用法: bash scripts/release.sh [major|minor|patch]"
    echo "默认: patch"
    exit 1
fi

# ── Read current version ────────────────────────────────────────────────
current=$(grep -E '^version\s*=' "$PYPROJECT" | head -1 | sed 's/.*"\(.*\)".*/\1/')
if [ -z "$current" ]; then
    echo "[release] 无法从 pyproject.toml 解析版本号"
    exit 1
fi

IFS='.' read -r major minor patch <<< "$current"
patch="${patch:-0}"

case "$bump" in
    major) major=$((major + 1)); minor=0; patch=0 ;;
    minor) minor=$((minor + 1)); patch=0 ;;
    patch) patch=$((patch + 1)) ;;
esac

new_version="${major}.${minor}.${patch}"
tag="v${new_version}"
today=$(date +%Y-%m-%d)

echo "[release] ${current} → ${new_version} (${bump})"

# ── 1. Update pyproject.toml ────────────────────────────────────────────
sed -i '' "s/^version = \"${current}\"/version = \"${new_version}\"/" "$PYPROJECT"

# ── 2. Reinstall editable ───────────────────────────────────────────────
if [ -f "$VENV_PYTHON" ]; then
    "$VENV_PYTHON" -m pip install -e "$REPO_ROOT" -q 2>/dev/null || true
    echo "[release] venv synced to ${new_version}"
fi

# ── 3. Update CHANGELOG.md ──────────────────────────────────────────────
if [ -f "$CHANGELOG" ]; then
    # Insert new version block after the first ## line
    recent_commits=$(git log --oneline "$(git describe --tags --abbrev=0 2>/dev/null || echo HEAD~10)"..HEAD 2>/dev/null | sed 's/^/  - /' || echo "  - Release ${new_version}")
    tmp_changelog=$(mktemp)
    awk -v ver="## [${new_version}] - ${today}" -v commits="${recent_commits}" '
        NR==1 { print; print ""; print ver; print ""; print commits; print ""; next }
        { print }
    ' "$CHANGELOG" > "$tmp_changelog"
    mv "$tmp_changelog" "$CHANGELOG"
    echo "[release] CHANGELOG.md updated"
fi

# ── 4. Git commit ───────────────────────────────────────────────────────
git add "$PYPROJECT" "$CHANGELOG"
git commit -m "chore: release ${tag}" --allow-empty
echo "[release] committed"

# ── 5. Tag ──────────────────────────────────────────────────────────────
if git rev-parse "$tag" >/dev/null 2>&1; then
    echo "[release] tag $tag already exists, skipping"
else
    git tag -a "$tag" -m "$tag"
    echo "[release] tagged $tag"
fi

# ── 6. Push ─────────────────────────────────────────────────────────────
current_branch=$(git branch --show-current)
echo ""
echo "[release] 准备推送: git push origin ${current_branch} --follow-tags"
echo "[release] 确认推送? (y/N)"
read -r confirm
if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
    git push origin "$current_branch" --follow-tags
    echo "[release] ✓ ${tag} 已推送"
else
    echo "[release] 已取消推送，本地已完成 commit + tag"
fi
