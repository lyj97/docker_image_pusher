#!/bin/bash
# ============================================================
# Docker 镜像推送到阿里云
# 用法: ./push.sh <镜像名>
# 例如: ./push.sh portainer/portainer-ce:2.27.3
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOKEN_FILE="$SCRIPT_DIR/.token"
IMAGES_FILE="$SCRIPT_DIR/images.txt"
REPO_OWNER="lyj97"
REPO_NAME="docker_image_pusher"
BRANCH="dev/240617"
POLL_INTERVAL=15
POLL_TIMEOUT=600

# ─── 检查参数 ─────────────────────────────────────────────────
if [[ -z "$1" ]]; then
    echo "用法: $0 <镜像名>"
    echo "例如: $0 portainer/portainer-ce:2.27.3"
    exit 1
fi
IMAGE="$1"

# ─── 读取 token ───────────────────────────────────────────────
if [[ ! -f "$TOKEN_FILE" ]]; then
    echo "❌ 找不到 token 文件: $TOKEN_FILE"
    exit 1
fi
TOKEN=$(cat "$TOKEN_FILE" | tr -d '[:space:]')

# ─── 写入 images.txt 并 push ──────────────────────────────────
echo "[1/3] 写入镜像: $IMAGE"
echo "$IMAGE" > "$IMAGES_FILE"

cd "$SCRIPT_DIR"
git remote set-url origin "https://${REPO_OWNER}:${TOKEN}@github.com/${REPO_OWNER}/${REPO_NAME}.git" 2>/dev/null
git add images.txt
git commit -m "push $IMAGE" -q
git push origin "$BRANCH" -q 2>&1

if [[ $? -ne 0 ]]; then
    echo "❌ git push 失败"
    exit 1
fi
echo "✅ 已 push 到 GitHub"

# ─── 等待 Actions 触发，获取 run_id ──────────────────────────
echo "[2/3] 等待 GitHub Actions 触发..."
sleep 15

RUN_ID=""
for i in {1..10}; do
    RUN_ID=$(curl -s \
        -H "Authorization: token $TOKEN" \
        -H "Accept: application/vnd.github.v3+json" \
        "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/actions/runs?branch=${BRANCH}&per_page=1" \
        | grep -o '"id":[0-9]*' | head -1 | cut -d: -f2)
    [[ -n "$RUN_ID" ]] && break
    sleep 3
done

if [[ -z "$RUN_ID" ]]; then
    echo "❌ 获取 run_id 失败"
    exit 1
fi
echo "   run_id: $RUN_ID"
echo "   详情: https://github.com/${REPO_OWNER}/${REPO_NAME}/actions/runs/${RUN_ID}"

# ─── 轮询等待完成 ─────────────────────────────────────────────
echo "[3/3] 等待 Actions 完成（每 ${POLL_INTERVAL}s 查询，超时 ${POLL_TIMEOUT}s）..."

ELAPSED=0
while [[ $ELAPSED -lt $POLL_TIMEOUT ]]; do
    RESP=$(curl -s \
        -H "Authorization: token $TOKEN" \
        -H "Accept: application/vnd.github.v3+json" \
        "https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}/actions/runs/${RUN_ID}")

    STATUS=$(echo "$RESP"     | grep -o '"status":"[^"]*"'     | head -1 | cut -d'"' -f4)
    CONCLUSION=$(echo "$RESP" | grep -o '"conclusion":"[^"]*"' | head -1 | cut -d'"' -f4)

    echo "   [${ELAPSED}s] status=$STATUS conclusion=${CONCLUSION:-running}"

    if [[ "$STATUS" == "completed" ]]; then
        echo ""
        if [[ "$CONCLUSION" == "success" ]]; then
            NAME=$(echo "$IMAGE" | sed 's|.*/||')
            echo "✅ Actions 执行成功！"
            echo "📦 阿里云镜像地址:"
            echo "   registry.cn-beijing.aliyuncs.com/lu97/$NAME"
            exit 0
        else
            echo "❌ Actions 执行失败（conclusion=$CONCLUSION）"
            echo "   详情: https://github.com/${REPO_OWNER}/${REPO_NAME}/actions/runs/${RUN_ID}"
            exit 1
        fi
    fi

    sleep $POLL_INTERVAL
    ELAPSED=$((ELAPSED + POLL_INTERVAL))
done

echo "❌ 超时（${POLL_TIMEOUT}s），Actions 仍未完成"
echo "   详情: https://github.com/${REPO_OWNER}/${REPO_NAME}/actions/runs/${RUN_ID}"
exit 1
