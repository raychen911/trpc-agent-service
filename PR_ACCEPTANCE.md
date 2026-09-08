# 企业微信机器人联调验收说明

本 PR 完成了企业微信智能机器人（`wecom_ai_bot`）的完整联调，并提供了详细的文档和真实验收记录。

---

## ✅ 验收前提条件

本 PR 的企业微信联调验收需要：

- [ ] **Reviewer 拥有企业微信账号**（个人版或企业版均可）
- [ ] **Reviewer 已创建智能机器人**（参考 `docs/IM_INTEGRATION.md` 第 1.1 节）
- [ ] **Reviewer 有可用的 LLM API Key**（OpenAI/Claude 或兼容 API，如 New API、OpenRouter 等）

**如果没有企业微信智能机器人**，可以使用以下替代验收方案：
- 使用 **Web UI** 渠道验证核心功能（`http://127.0.0.1:18001/ui`）
- 使用 **Telegram** 或 **Feishu** 机器人（参考同一文档的其他章节）
- 进行 **代码审查** 和文档完整性检查

---

## 📖 验收步骤

### 1. 安装依赖并配置环境

```bash
# 安装项目依赖
uv sync --locked --extra dev --python 3.12

# 安装企业微信 SDK
uv pip install wecom-aibot-sdk-python

# 复制配置模板
cp .env.example .env
```

编辑 `.env` 文件，配置：
```bash
# 必填：LLM 模型配置
OPENAI_API_KEY=your-api-key
CPA_BASE_URL=https://your-api-endpoint/v1
CPA_MODEL=your-model-name  # 如 claude-sonnet-4-20250514

# 必填：管理员 API Key
ADMIN_API_KEY=your-random-key

# 必填：企业微信机器人配置
WECOM_AI_BOT_ENABLED=1
WECOM_AI_BOT_ACCOUNT_ID=your-bot-id
SECRET_TENANT_DEMO_WECOM_AI_BOT_BOT_SECRET=your-bot-secret

# 本地开发：禁用外部依赖
REDIS_URL=
POSTGRES_DSN=
TENANT_DB_DSN=
WORKER_QUEUE_URL=
OUTBOUND_QUEUE_URL=
OUTBOUND_QUEUE_ENABLED=0
```

---

### 2. 启动服务

```bash
# Windows
.\start-web-ui.ps1 --port 18001

# Linux/macOS
./start.sh
```

验证服务启动：
```bash
curl http://127.0.0.1:18001/health
```

预期输出：
```json
{
  "status": "ok",
  "worker_mode": "local",
  "local_workers": 1,
  "active_tenants": 1
}
```

---

### 3. 执行功能测试

参考 `docs/IM_INTEGRATION.md` 第 9 节的测试用例，在企业微信中依次测试：

| 测试项 | 测试内容 | 参考章节 |
|--------|---------|---------|
| 基础对话 | 发送 "你好" | 9.1 |
| 实时信息 | 发送 "北京的天气怎么样" | 9.2 |
| 上下文记忆 | 两轮对话测试颜色记忆 | 9.4 |
| 多轮协作 | 斐波那契函数 + 类型注解 | 9.5 |
| 长文本生成 | 微服务架构详解 | 9.6 |
| Markdown 格式 | Python/Go 特点列表 | 9.7 |

---

### 4. 对比真实验收记录

将你的测试结果与 `docs/IM_INTEGRATION.md` 第 9.8 节的真实联调记录对比：

**检查要点：**
- ✅ 响应时间是否在合理范围（基础对话 5-15 秒，长文本 30-180 秒）
- ✅ 上下文记忆是否正常工作
- ✅ Markdown 格式是否正确渲染
- ✅ 多轮对话是否理解指代关系

---

### 5. 验收通过标准

所有以下条件满足即为验收通过：

- [ ] 服务正常启动，`/health` 返回 `status=ok`
- [ ] 企业微信机器人能接收消息并正常回复
- [ ] 至少完成 4 项功能测试（基础对话、上下文记忆、多轮协作、Markdown 格式）
- [ ] 日志中无明文密钥泄露（应显示为 `[configured]` 或 `secret-redacted`）
- [ ] 响应时间在合理范围内
- [ ] 文档清晰完整，可复现联调过程

---

## 🔄 替代验收方案

### 方案 A：使用 Web UI 验证核心功能

如果没有企业微信机器人，可以通过 Web UI 验证相同功能：

1. 访问 `http://127.0.0.1:18001/ui`
2. 在聊天界面中执行 `docs/IM_INTEGRATION.md` 第 9 节的所有测试用例
3. 验证响应质量和功能完整性

**优势：** 无需企业微信账号，只需 LLM API Key

---

### 方案 B：代码审查和文档完整性检查

如果无法进行实际联调，可以审查：

1. **代码质量：**
   - 检查 `trpc_service/channels/wecom_ai_bot.py` 的实现
   - 验证 WebSocket 连接管理、错误处理、密钥脱敏

2. **文档完整性：**
   - `docs/IM_INTEGRATION.md` 是否包含完整的配置步骤
   - 测试用例是否清晰可执行
   - 真实联调记录是否详细

3. **配置正确性：**
   - `.env.example` 是否包含所有必要配置
   - `README.md` 快速启动指南是否清晰

---

## 📚 相关文档

- **完整联调文档：** `docs/IM_INTEGRATION.md`
- **快速启动指南：** `README.md` 第 "本地开发快速启动" 节
- **真实验收记录：** `docs/IM_INTEGRATION.md` 第 9.8 节
- **故障排查：** `docs/IM_INTEGRATION.md` 第 11 节

---

## 💡 提示

- 企业微信智能机器人使用 **WebSocket 长连接**，无需公网 webhook
- 本地开发可以直接联调，不需要内网穿透工具
- 首次调试建议先使用 Web UI 验证服务正常，再测试企业微信
- 遇到问题请参考文档第 11 节"常见故障排查"

---

## ✅ 验收完成后

验收通过后，请在 PR 中留言确认，并注明：
- 使用的验收方案（企业微信/Web UI/代码审查）
- 完成的测试用例数量
- 是否遇到问题及解决方案

感谢您的审阅！
