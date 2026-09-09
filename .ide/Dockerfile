# ===================================================================
# .ide/Dockerfile - CNB 云开发环境镜像（仅开发用，不参与生产部署）
# ===================================================================
# 说明: 构建/推送由 .cnb.yml 流水线维护（tag: trpc-agent-service:latest）。
#   生产服务镜像在仓库根 Dockerfile，两者职责分离。
# ===================================================================

# -----------------------------------------------------------------------------
# 基础镜像与环境变量
# -----------------------------------------------------------------------------

# 使用 Ubuntu 24.04 LTS 作为基础镜像
FROM ubuntu:24.04

# 环境变量配置
#   DEBIAN_FRONTEND=noninteractive: 禁用交互式提示，避免安装时卡住
ENV DEBIAN_FRONTEND=noninteractive

# 配置国内镜像源
# 说明: 使用腾讯云镜像加速 apt 下载
RUN . /etc/os-release 2>/dev/null || VERSION_CODENAME="noble" && \
    echo "deb https://mirrors.cloud.tencent.com/ubuntu/ ${VERSION_CODENAME} main restricted universe multiverse" > /etc/apt/sources.list && \
    echo "deb https://mirrors.cloud.tencent.com/ubuntu/ ${VERSION_CODENAME}-updates main restricted universe multiverse" >> /etc/apt/sources.list && \
    echo "deb https://mirrors.cloud.tencent.com/ubuntu/ ${VERSION_CODENAME}-backports main restricted universe multiverse" >> /etc/apt/sources.list && \
    echo "deb https://mirrors.cloud.tencent.com/ubuntu/ ${VERSION_CODENAME}-security main restricted universe multiverse" >> /etc/apt/sources.list

# -----------------------------------------------------------------------------
# 系统工具安装
# -----------------------------------------------------------------------------

# 安装基础开发工具
# 说明: 合并安装减少镜像层数，--no-install-recommends 避免安装不必要的包
# 工具说明:
#   git:                        版本控制系统，代码管理必备
#   curl/wget:                  命令行下载工具，用于获取文件和脚本
#   procps:                     进程管理工具(ps, top等)
#   ca-certificates/gpg:        HTTPS 证书与 GPG 密钥管理
#   python-is-python3:          让 python 命令指向 python3（Ubuntu 24.04 内置 python3）
#   redis-server:               Session/幂等/锁后端（本地开发验证 Redis 存储用）
# 清理: apt-get clean + rm -rf 删除缓存，减小镜像体积
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git \
        curl \
        wget \
        procps \
        ca-certificates \
        gpg \
        python-is-python3 \
        redis-server \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# -----------------------------------------------------------------------------
# UV 包管理器与 Python 库
# -----------------------------------------------------------------------------

# 安装 UV - 极速 Python 包管理器
# 说明: 比 pip 快 10-100 倍，支持并行安装，与系统 Python 3.12 完美兼容
# Ubuntu 24.04 内置 Python 3.12，无需额外安装
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# 添加 UV 到 PATH
ENV PATH="/root/.local/bin:$PATH"

# 说明: 使用 uv 创建虚拟环境避免系统Python保护限制，设为默认Python
RUN uv venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# 开发环境镜像：只装依赖，不复制源码（CNB 启动时 git 拉取仓库到 /workspace）。
#   依赖以 uv.lock 精确锁定（含框架 trpc-agent-py 的实测基线版本），构建可复现；
#   --frozen 保证锁与 pyproject 不同步时构建直接失败，杜绝"漂移构建"。
#   升级依赖流程: 改 pyproject.toml → uv lock → 提交 uv.lock → 重新构建镜像
COPY pyproject.toml uv.lock /tmp/deps/
RUN cd /tmp/deps && UV_PROJECT_ENVIRONMENT=/opt/venv uv sync --frozen --no-install-project

# -----------------------------------------------------------------------------
# Go 语言安装
# -----------------------------------------------------------------------------

# 安装 Go 1.25.13 (Go 1.25 系列最新稳定版本)
# 说明: 使用官方二进制包安装到 /usr/local/go，自动匹配 amd64/arm64 架构
RUN ARCH="$(uname -m)" && \
    case "$ARCH" in \
        x86_64|amd64) GOARCH="amd64" ;; \
        aarch64|arm64) GOARCH="arm64" ;; \
        *) echo "Unsupported arch: $ARCH" && exit 1 ;; \
    esac && \
    curl -fsSL "https://go.dev/dl/go1.25.13.linux-${GOARCH}.tar.gz" -o /tmp/go.tar.gz && \
    rm -rf /usr/local/go && \
    tar -C /usr/local -xzf /tmp/go.tar.gz && \
    rm /tmp/go.tar.gz && \
    /usr/local/go/bin/go version

# 添加 Go 到 PATH
ENV PATH="/usr/local/go/bin:${PATH}"

# -----------------------------------------------------------------------------
# code-server 与 VS Code 扩展
# -----------------------------------------------------------------------------

# 安装 code-server - 浏览器版 VS Code
# 说明: 官方 install.sh 自动下载最新版本并安装
RUN curl -fsSL https://code-server.dev/install.sh | sh

# 安装 VS Code 扩展
# 扩展列表（按功能分类）:
#   语言支持:
#     ms-python.python              Python 语言支持（智能提示、调试、Linter）
#     golang.go                     Go 语言支持（智能提示、调试、Linter）
#   文档预览:
#     cweijan.vscode-office         Office 文档预览（Word / Excel）
#     mathematic.vscode-pdf         PDF 文档预览与阅读
#   数据库:
#     cweijan.vscode-database-client2  Database Client（SQLite/MySQL/Redis/MongoDB 等）
#   编辑器增强:
#     oderwat.indent-rainbow        缩进彩虹，代码层级可视化
#   AI 编程助手:
#     tencent-cloud.coding-copilot  腾讯 AI 辅助编程
#     openai.chatgpt                ChatGPT AI 编程助手
#   CNB 平台扩展:
#     cnbcool.cnb-welcome           CNB 平台欢迎页
RUN code-server --install-extension ms-python.python && \
    code-server --install-extension golang.go && \
    code-server --install-extension cweijan.vscode-office && \
    code-server --install-extension mathematic.vscode-pdf && \
    code-server --install-extension cweijan.vscode-database-client2 && \
    code-server --install-extension oderwat.indent-rainbow && \
    code-server --install-extension tencent-cloud.coding-copilot && \
    code-server --install-extension openai.chatgpt && \
    code-server --install-extension cnbcool.cnb-welcome

# -----------------------------------------------------------------------------
# 环境变量与系统配置
# -----------------------------------------------------------------------------

# 字符集配置
ENV LANG=C.UTF-8
ENV LANGUAGE=C.UTF-8

# 容器默认工作目录
WORKDIR /workspace

# 容器默认启动命令
CMD ["/bin/bash"]