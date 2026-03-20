# kv-storage

一个基于 [sled](http://sled.rs/) 的 Rust 异步持久化 KV 存储库，整体使用方式接近 Redis 的常见数据结构，当前支持：

- Key-Value
- Map / Hash
- List / Queue
- Counter
- TTL 过期
- 批量操作
- 异步迭代扫描

项目已经提供 `tokio` 异步接口，并通过测试覆盖了 KV、Map、List、Counter、TTL、批量写入、遍历和模式扫描等典型场景。

## 特性概览

- 基于 `sled` 的嵌入式本地存储，无需独立部署数据库服务
- 面向 `tokio` 的异步 API，适合服务端项目集成
- 支持序列化任意实现 `Serialize` / `DeserializeOwned` 的数据
- 提供独立的 `Map` 与 `List` 命名空间，便于组织结构化数据
- 支持计数器自增、自减、设置与读取
- 支持键、Map、List 的 TTL 与后台过期清理
- 支持批量插入、批量删除、前缀遍历、通配扫描
- 可通过 Cargo feature 控制 `ttl`、`len`、`map_len`

## 安装

`Cargo.toml`

```toml
[dependencies]
kv-storage = "0.1.0"
```

如需按需关闭默认特性：

```toml
[dependencies]
kv-storage = { version = "0.1.0", default-features = false, features = ["ttl", "len", "map_len"] }
```

默认启用的特性：

- `ttl`：支持过期时间与后台清理
- `len`：支持数据库级 `len()`
- `map_len`：支持 `Map::len()`

## 快速开始

```rust
use kv_storage::{Config, init_db, Map, List};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config {
        path: "./db/demo".into(),
        ..Default::default()
    };

    let db = init_db(&cfg).await?;

    db.insert("user:1", &"Alice").await?;
    let user: Option<String> = db.get("user:1").await?;
    println!("user = {:?}", user);

    let profile = db.map("profile", None).await?;
    profile.insert("name", &"Alice").await?;
    profile.insert("age", &18).await?;
    let age: Option<i32> = profile.get("age").await?;
    println!("age = {:?}", age);

    let logs = db.list("logs", None).await?;
    logs.push(&"created").await?;
    logs.push(&"updated").await?;
    let all_logs: Vec<String> = logs.all().await?;
    println!("logs = {:?}", all_logs);

    db.counter_incr("request_count", 1).await?;
    let count = db.counter_get("request_count").await?;
    println!("count = {:?}", count);

    Ok(())
}
```

## 配置说明

`Config` 当前包含以下主要字段：

```rust
use kv_storage::Config;

let cfg = Config {
    path: "./db/app".into(),
    ..Default::default()
};
```

- `path`：sled 数据目录，必填，不能为空
- `cache_capacity`：缓存大小，默认 `1GB`
- `cleanup_f`：过期数据清理函数，默认会启动后台清理任务

注意：

- `path` 为空时，`init_db` 会返回错误
- 默认配置会设置 `flush_every_ms(Some(3000))`
- 存储模式当前使用 `sled::Mode::LowSpace`

## 核心用法

### 1. Key-Value

```rust
db.insert("k1", &123).await?;
let value: Option<i32> = db.get("k1").await?;
let exists = db.contains_key("k1").await?;
db.remove("k1").await?;
```

支持批量操作：

```rust
db.batch_insert(vec![
    (b"k1".to_vec(), 1),
    (b"k2".to_vec(), 2),
]).await?;

db.batch_remove(vec![
    b"k1".to_vec(),
    b"k2".to_vec(),
]).await?;
```

### 2. Map / Hash

```rust
use kv_storage::Map;

let map = db.map("orders", None).await?;
map.insert("order_1", &100).await?;
map.insert("order_2", &200).await?;

let value: Option<i32> = map.get("order_1").await?;
let exists = map.contains_key("order_2").await?;
let empty = map.is_empty().await?;
map.remove("order_1").await?;
map.clear().await?;
```

创建时可附带过期时间（毫秒）：

```rust
let session_map = db.map("session:1", Some(30_000)).await?;
```

### 3. List / Queue

```rust
use kv_storage::List;

let list = db.list("task_queue", None).await?;
list.push(&"task-1").await?;
list.push(&"task-2").await?;

let first: Option<String> = list.pop().await?;
let all: Vec<String> = list.all().await?;
let by_index: Option<String> = list.get_index(0).await?;
let len = list.len().await?;
```

限制列表长度：

```rust
let removed: Option<i32> = list.push_limit(&100, 10, true).await?;
```

含义：

- `limit = 10`：列表最多保留 10 项
- `pop_front_if_limited = true`：超限时弹出最早的数据
- 返回值为被移除的元素

### 4. Counter

```rust
db.counter_incr("counter_a", 3).await?;
db.counter_decr("counter_a", 1).await?;
db.counter_set("counter_a", 100).await?;

let value = db.counter_get("counter_a").await?;
```

### 5. TTL

如果启用了 `ttl` 特性，可以对 KV、Map、List 设置过期时间。

```rust
db.insert("token", &"abc").await?;
db.expire("token", 5_000).await?;
let ttl = db.ttl("token").await?;
```

`Map` / `List` 同样支持：

```rust
let map = db.map("cache_map", None).await?;
map.expire(10_000).await?;

let list = db.list("recent_items", None).await?;
let now_ms = std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)?
    .as_millis() as i64;
list.expire_at(now_ms + 60_000).await?;
```

说明：

- `expire(key, dur)` 中的 `dur` 单位为毫秒
- `expire_at(key, at)` 使用毫秒时间戳
- 过期后读取通常返回 `None`、空集合或长度 `0`
- 默认后台任务会周期性清理真正落盘的过期数据

## 遍历与扫描

### 遍历 Map

```rust
use kv_storage::Map;

let mut map = db.map("profile", None).await?;
let mut iter = map.iter::<String>().await?;

while let Some(item) = iter.next().await {
    let (key, value) = item?;
    println!("{} = {}", String::from_utf8_lossy(&key), value);
}
```

### 遍历 List

```rust
use kv_storage::List;

let mut list = db.list("logs", None).await?;
let mut iter = list.iter::<String>().await?;

while let Some(item) = iter.next().await {
    println!("{:?}", item?);
}
```

### 遍历所有 Map / List

```rust
let mut db = db.clone();
let mut map_iter = db.map_iter().await?;
while let Some(map) = map_iter.next().await {
    let map = map?;
    println!("map = {}", String::from_utf8_lossy(map.name()));
}

let mut list_iter = db.list_iter().await?;
while let Some(list) = list_iter.next().await {
    let list = list?;
    println!("list = {}", String::from_utf8_lossy(list.name()));
}
```

### 模式扫描

```rust
let mut db = db.clone();
let mut iter = db.scan("foo/*").await?;

while let Some(item) = iter.next().await {
    println!("key = {}", String::from_utf8_lossy(&item?));
}
```

支持的模式字符：

- `*`：匹配任意长度字符
- `?`：匹配单个字符
- `\*` / `\?`：转义后的字面量

## API 概览

### `StorageDB`

- `insert` / `get` / `remove`
- `batch_insert` / `batch_remove`
- `contains_key`
- `counter_incr` / `counter_decr` / `counter_get` / `counter_set`
- `map` / `map_remove` / `map_iter`
- `list` / `list_remove` / `list_iter`
- `scan`
- `db_size`
- `info`
- `expire` / `expire_at` / `ttl`（需 `ttl` 特性）
- `len`（需 `len` 特性）

### `Map`

- `insert` / `get` / `remove`
- `contains_key`
- `clear` / `is_empty`
- `remove_and_fetch`
- `remove_with_prefix`
- `batch_insert` / `batch_remove`
- `iter` / `key_iter` / `prefix_iter`
- `expire` / `expire_at` / `ttl`（需 `ttl` 特性）
- `len`（需 `map_len` 特性）

### `List`

- `push` / `pushs` / `push_limit`
- `pop`
- `all` / `get_index`
- `len` / `is_empty` / `clear`
- `iter`
- `expire` / `expire_at` / `ttl`（需 `ttl` 特性）

## 适用场景

- 轻量级本地持久化缓存
- 单机服务内嵌存储
- 任务队列、事件列表、最近记录列表
- 会话数据、配置项、临时状态存储
- 需要 TTL 和计数器能力，但不想引入独立 Redis 服务的场景

## 当前实现说明

- 当前公开入口为 `sled` 后端
- 对外接口是异步的，但底层依赖 `sled` 的本地嵌入式能力
- 数据编码主要通过 `bincode` 完成
- 默认测试中已经验证大批量写入、TTL 清理、前缀遍历、通配扫描等能力

## 开发与测试

运行测试：

```bash
cargo test
```

格式检查与静态检查可按需执行：

```bash
cargo fmt
cargo clippy --all-features --all-targets
```

## License

MIT
