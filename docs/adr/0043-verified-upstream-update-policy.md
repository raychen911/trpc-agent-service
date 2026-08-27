# 精确锁定并持续验证上游稳定版

项目从 `trpc-agent-py==1.1.19` 与已提交 uv.lock 开始，每日检查官方稳定 Release/PyPI 并自动创建版本与锁文件升级 PR，但不跟踪 main 或自动升级生产。升级必须审查 Release Notes 和依赖，通过 Session、Memory、Summary、Runner、Filter、Tool、Knowledge、Channel、AG-UI/A2A、性能与安全回归，先进入 Staging 再灰度；普通版本七天内评估，严重安全修复目标四十八小时，执行与审计记录实际 SDK 版本。最终验收前出现新稳定版时必须升级重测，除非存在已记录的可复现阻断。
