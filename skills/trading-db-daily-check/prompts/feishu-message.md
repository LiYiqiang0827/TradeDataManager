# 飞书消息格式

每个 cron job 通过 `lark-cli im +messages-send` 把脚本输出发给用户(Home channel)。

## 通用约定

- **正常**:简洁摘要 + 表格(适用 offline/online 日检)
- **异常**:脚本 exit 1 时,output 末尾已含"⚠️ 异常汇总"区块
- **hourly 保活** exit 1 才发飞书(避免每小时都被刷屏)— 通过 `--quiet-ok` flag
- offline/online 日检 exit 0 也发飞书(用户要"确认"结果)

## 异常前缀

hourly_live_check.py 加前缀 `⚠️ scheduler 异常`;offline/online 加对应 `🗂 offline` / `📡 online` 前缀。
脚本本身输出已含 emoji 前缀,可直接转发。

## 飞书 message 长度限制

单条 4 KB,long output 会被截断。脚本输出 ~5 KB 上下,够用。