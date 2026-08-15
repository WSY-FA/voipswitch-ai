# VoIPSwitch AI Gateway

VoIPSwitch 的独立 AI Gateway，提供 AI 任务运行时、控制与媒体接口以及内嵌管理 Web。

## Workspace

- `ai-protocol`：版本化的控制与媒体协议类型。
- `ai-provider`：AI provider 抽象、配置和测试实现。
- `ai-gateway`：任务执行、持久化、重试和保留策略。
- `vs_ai_gatewayd`：Gateway 守护进程与内嵌管理 Web。

## 构建

```bash
cargo build --workspace
```

## 运行

默认入口只启动 `vs_ai_gatewayd`：

```bash
./scripts/start_default.sh
```

也可以直接启动：

```bash
cargo run -p vs_ai_gatewayd
```

管理 Web 默认监听 `0.0.0.0:18082`。默认 Unix socket 位于
`$XDG_RUNTIME_DIR/voipswitch/`；未设置 `XDG_RUNTIME_DIR` 时使用 `/tmp/voipswitch/`。

生产环境应通过环境变量或受限密码文件设置初始管理员密码：

```bash
AI_GATEWAY_ADMIN_PASSWORD='replace-with-a-strong-password' \
  ./scripts/start_default.sh
```

```bash
AI_GATEWAY_BOOTSTRAP_PASSWORD_FILE=/secure/path/admin-password \
  ./scripts/start_default.sh
```

可通过 `--config gateway-config.example.json` 加载显式配置。使用 `--help` 查看完整参数。

## 验证

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```
