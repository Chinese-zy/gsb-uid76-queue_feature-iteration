docker compose up --build --abort-on-container-exit

## 语义

- 延迟投递：`Msg.NotBefore` 之前不可投；到点后进入就绪队尾，与普通消息一起排队，不插队。
- 按键保序：同一 `Key` 前一条未 `Ack`，后一条即使到点也等待，`Ack` 后才放行。
- 重启恢复：未确认消息按原偏移（ID 序）重投，至少一次，允许重复，不漏、不乱同键顺序。
- 死信：`Nack` 重试达到 `MaxAttempts` 后进入独立死信路（`PullDead` 取出），不再混回主队列。
