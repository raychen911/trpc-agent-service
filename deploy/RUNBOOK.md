# tRPC Agent 运行手册

## 发布前检查

1. 确认 `TRPC_RUNTIME_MODE=protected`、数据库、模型、IM、Redis、Qdrant、MinIO 和管理令牌均来自部署密钥系统。
2. 执行 `python -m trpc_service._cli backup-manifest --output reports/backup-manifest.json`。
3. 执行 `python -m trpc_service._cli verify-snapshot --snapshot-dir reports/last-snapshot`，确认快照 checksum 和对象 hash。
4. 查看 `GET /health/ready?verbose=true`，所有已配置依赖必须为 `ok`。

## 灰度发布

```bash
TRPC_ADMIN_TOKEN=... python -m trpc_service._cli release \
  --release-tenant TENANT --target-version VERSION --expected-active-version ACTIVE \
  --management-url http://gateway:8080 --probe http://worker-1:8080/health/ready \
  --probe http://worker-2:8080/health/ready
```

发布按 1%、10%、25%、50%、100% 推进。任一节点探针失败会把灰度比例归零；版本锁定和最终切换仍需操作员按变更单执行。

## 恢复演练

在隔离数据库上执行：

```bash
python -m trpc_service._cli restore-drill --snapshot-dir reports/last-snapshot \
  --target-database-url "$DRILL_DATABASE_URL" --evidence-output reports/restore-drill.json
```

演练必须输出 `drill: passed`，并确认 SQL、Qdrant 和对象存储中不存在未知租户引用。生产流量恢复前不得在原库执行 destructive restore。

## 监控和告警

- `GET /metrics` 提供队列、Worker、模型、工具和存储指标。
- `python -m trpc_service._cli slo-report` 输出可用性和 P95 延迟目标结果。
- `python -m trpc_service._cli alert-test --alert-webhook URL` 验证外部告警接收端返回 2xx。
- 数据库或依赖异常时保持 `/health/live` 为 200，让编排器依据 `/health/ready` 摘除节点。

## 事故处置

先冻结发布和租户配置，再保存 `backup-manifest`、`/metrics`、日志和告警事件。恢复前核对清单 checksum、schema 版本、密钥版本和租户数量；恢复后完成 consistency-check 与双 IM 冒烟测试，再解除流量隔离。
