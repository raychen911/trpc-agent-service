# 使用 Python 后端与 React 管理前端

后端统一使用 Python 3.12、`trpc-agent-py==1.1.19`、FastAPI、Pydantic 2、SQLAlchemy 2 Async、Alembic、asyncpg 和异步 Kafka 客户端，以 uv 锁定依赖；不使用 Go 重写 SDK 核心链路。Web Console 使用 React、TypeScript 与 Vite，通过 OpenAPI 生成类型安全客户端并使用 OIDC Authorization Code + PKCE，默认简体中文且预留国际化。前后端保留在同一仓库但独立构建镜像和部署。
