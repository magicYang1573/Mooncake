# Mooncake KV Event 发布规则

分析版本：`kvcache-ai/Mooncake` PR #2214 head `6e133671e13be615799099e4a1ead3b7ad53dfa5`。

主要源码依据：

- `mooncake-store/src/kv_event/kv_event_publisher.cpp`
- `mooncake-store/include/kv_event/kv_event_publisher.h`
- `mooncake-store/include/kv_event/kv_event_config.h`
- `mooncake-store/include/kv_event/key_util.h`
- `mooncake-store/src/master_service.cpp`
- `mooncake-store/src/master.cpp`
- `mooncake-store/src/master_admin_service.cpp`
- `docs/source/design/conductor/indexer-api-design.md`

## 总体模型

Mooncake 当前实现是在 `mooncake_master` 内增加一个可选的 KV event publisher。它不是在推理引擎内部按 token 序列发布 GPU KV block，而是在 Mooncake Store master 看到对象完成写入、删除或被驱逐时，把一个 Mooncake object key 当作一个 pooled KV block 发布出去。

发布链路：

1. `MasterService` 构造时根据 `MasterServiceConfig` 创建 `KvEventPublisher`。
2. KV 对象生命周期函数调用 `PublishKvStored` / `PublishKvRemoved`。
3. `KvEventPublisher` 把事件放入内部队列。
4. 后台线程最多每批取 64 个事件，编码为 msgpack。
5. 通过 ZMQ `PUB` socket 发出三帧消息。

启用条件：

- 编译时需要 `MOONCAKE_ENABLE_KV_EVENTS`；否则 `KvEventPublisher` 是 no-op stub。
- 运行时 `enable_kv_events=true`。
- `kv_events_bind_endpoint` 不能为空。
- `kv_events_backend_id` 不能为空。
- ZMQ context/socket 创建和 `zmq_bind` 成功。

相关配置项：

| 配置项 | 当前用途 |
|---|---|
| `enable_kv_events` | 是否启用 publisher。默认 `false`。 |
| `kv_events_bind_endpoint` | ZMQ PUB bind 地址，例如 `tcp://0.0.0.0:5557`。 |
| `kv_events_backend_id` | 写入每条事件的 `backend_id`，表示 cache owner / storage daemon / pool node。必填。 |
| `kv_events_emit_legacy_compat` | 是否额外写 legacy 字段 `type` / `block_hashes` / `parent_block_hash`。默认 `true`。 |
| `kv_events_emit_object_key` | 是否写 Mooncake `object_key`。默认 `true`。 |
| `kv_events_model_name` | 兼容保留字段，当前不写入事件。 |
| `kv_events_tenant_id` | 兼容保留字段，当前事件 tenant 来自对象操作参数。 |
| `kv_events_additional_salt` | 兼容保留字段，当前不写入事件。 |
| `kv_events_lora_name` | 兼容保留字段，当前不写入事件。 |
| `kv_events_block_size` | 兼容保留字段，当前不写入事件。 |
| `kv_events_dp_rank` | 兼容保留字段，当前不写入事件。 |
| `kv_events_queue_capacity` | deprecated，当前队列是 unbounded，不使用该值。 |

## ZMQ 传输格式

Mooncake 使用 vLLM/SGLang 风格的三帧 ZMQ multipart：

| Frame | 内容 | 来源 |
|---|---|---|
| 1 | 空 topic | 固定为空。 |
| 2 | 8 字节 big-endian batch sequence | `next_zmq_sequence_`，从 1 开始，每发一个 batch 加 1。 |
| 3 | msgpack payload | `[timestamp_ms, [event_map...], dp_rank]`。 |

payload 结构：

```text
[
  timestamp_ms,
  [
    { event fields... },
    { event fields... }
  ],
  0
]
```

说明：

- `timestamp_ms` 是当前 Unix epoch milliseconds，在同一批事件中共享。
- batch trailer `dp_rank` 固定写 `0`。代码注释说明 storage pool 没有 DP 上下文。
- 每个 event map 内也有 `timestamp` 字段，值同 batch timestamp。
- 内部队列不设容量上限；`dropped_events` 当前没有实际递增路径。
- `ZMQ_SNDHWM` 固定为 10000，`ZMQ_LINGER` 固定为 0。

## 事件触发规则

### Stored

触发点：

- `MasterService::PutEnd(...)` 在对象 replica 标记完成、同步 accounting、授予 lease 后调用 `PublishKvStored(key, replica_type, metadata, tenant_id)`。
- `BatchPutEnd` / `UpsertEnd` / `BatchUpsertEnd` 走 `PutEnd`，因此也会触发。

`medium` 来源：

- `ReplicaType::MEMORY` -> `"cpu"`
- `ReplicaType::DISK` / `LOCAL_DISK` / `NOF_SSD` -> `"disk"`
- `ReplicaType::ALL` 时按当前 metadata 判断：
  - 有 memory replica -> `"cpu"`
  - 否则有 nof/disk/local_disk replica -> `"disk"`
  - 否则 -> `"cpu"`

### Removed

触发点：

- `MasterService::Remove(...)` 在对象可删除、无未完成 replica、无 replication task 时，先发布 remove，再删除 metadata。
- `EvictDiskReplica(...)` 删除 DISK / LOCAL_DISK replica 后，如果确实删掉了 replica，发布 `medium="disk"`。
- eviction 流程删除 host memory 侧对象时，调用 `PublishKvRemovedAfterEvict(..., "cpu", ...)`。
- NOF / disk 侧 eviction 路径删除 replica 后发布 `medium="disk"`。
- `BatchRemove` / regex / remove all 等批量接口最终走这些删除路径。

`PublishKvRemovedAfterEvict` 的细节：

- 如果 `freed_bytes > 0`，直接发布指定 medium 的 remove。
- 如果没有 freed bytes，但 metadata 已经无效，则按剩余 metadata 推断 medium 再发布。
- 否则不发布。

## 字段含义和来源

### 通用字段

| 字段 | 类型/取值 | 是否总是出现 | 含义 | 来源 |
|---|---|---:|---|---|
| `event_id` | `u64` | 是 | Mooncake publisher 内部事件序号。 | `next_event_id_`，从 1 开始，每个被编码的 event 加 1。 |
| `timestamp` | integer ms | 是 | 事件时间戳，仅用于观测，不用于排序。 | `CurrentUnixTimeMs()`，同 batch timestamp。 |
| `event_type` | `"stored"` / `"removed"` | 是 | RFC 风格事件类型。 | `PendingEvent.kind`。 |
| `type` | `"BlockStored"` / `"BlockRemoved"` | 默认是 | legacy vLLM/SGLang 兼容类型。 | 由 `event_type` 映射；仅 `kv_events_emit_legacy_compat=true` 时写入。 |
| `model_name` | `nil` | 是 | 模型名。当前 master 不知道 per-block 模型上下文。 | 固定 `nil`。需要由 indexer register 提供。 |
| `block_size` | `nil` | 是 | KV block size。当前 master 不知道 token block size。 | 固定 `nil`。需要由 indexer register 提供。 |
| `additional_salt` | `nil` | 是 | hash namespace / 部署 salt。 | 固定 `nil`。需要由 indexer register 提供。 |
| `lora_name` | `nil` | 是 | LoRA adapter 名称。 | 固定 `nil`。master 没有 LoRA 上下文。 |
| `tenant_id` | string | 是 | Mooncake 对象所属 tenant。 | 对象操作传入的 `tenant_id`；空字符串时写 `"default"`。不是 `kv_events_tenant_id` 配置。 |
| `backend_id` | string | 是 | cache owner / storage daemon / pool node 身份。 | `kv_events_backend_id`。启用时必填。 |
| `medium` | string 或 `nil` | 是 | 对象所在 cache tier。 | `MasterService` 按 replica/metadata 推断，当前只发小写 `"cpu"` / `"disk"`；空字符串编码为 `nil`。 |
| `dp_rank` | `nil` | 是 | data parallel rank。 | event map 内固定 `nil`；batch trailer 固定 `0`。需要由 indexer register 提供。 |
| `object_key` | string | 默认是 | Mooncake Store 对象 key。 | 原始 `key`；仅 `kv_events_emit_object_key=true` 时写入。 |
| `seq_hashes` | `u64[]` | 是 | RFC 风格 rolling sequence hash 列表。Mooncake 当前每个对象最多给 1 个 hash。 | 从 `object_key` 解析 decimal 或 `0x`/`0X` hex u64；成功则 `[hash]`，失败则 `[]`。 |
| `block_hashes` | signed-like integer array | 默认是 | legacy alias，语义上等同 `seq_hashes`。 | 仅 `kv_events_emit_legacy_compat=true` 时写入；成功解析时 `[seq_hash]`，失败时 `[]`。 |

关于 `object_key` 和 `seq_hashes`：

- `ParseSeqHashFromObjectKey` 只接受完整 decimal u64 或 `0x`/`0X` 前缀 hex u64。
- 如果无法解析：
  - `kv_events_emit_object_key=true` 且 `object_key` 非空：事件仍发布，`seq_hashes=[]`，同时 `skipped_unparsed_keys` 加 1。
  - `kv_events_emit_object_key=false` 或 `object_key` 为空：事件跳过，同时 `skipped_unparsed_keys` 加 1。

### Stored 专有字段

| 字段 | 类型/取值 | 含义 | 来源 |
|---|---|---|---|
| `base_block_idx` | `0` | 第一个 block 的深度。Mooncake 把每个 object key 当独立 pooled block。 | 固定写 `0`。 |
| `parent_hash` | `nil` | 父 rolling hash。Mooncake master 没有序列树上下文。 | 固定 `nil`。 |
| `token_ids` | `nil` | 该批 block 对应 token ids。Mooncake master 没有 token 上下文。 | 固定 `nil`。 |
| `parent_block_hash` | `nil` | legacy parent hash alias。 | 仅 `kv_events_emit_legacy_compat=true` 时写入，固定 `nil`。 |

Stored 示例：

```json
{
  "event_id": 1,
  "timestamp": 1780000000000,
  "event_type": "stored",
  "type": "BlockStored",
  "model_name": null,
  "block_size": null,
  "additional_salt": null,
  "lora_name": null,
  "tenant_id": "default",
  "backend_id": "mooncake-master-0",
  "medium": "cpu",
  "dp_rank": null,
  "object_key": "0x2a",
  "seq_hashes": [42],
  "block_hashes": [42],
  "base_block_idx": 0,
  "parent_hash": null,
  "token_ids": null,
  "parent_block_hash": null
}
```

### Removed 专有字段

| 字段 | 类型/取值 | 含义 | 来源 |
|---|---|---|---|
| `base_block_idx` | `nil` | 删除事件的起始 block depth。Mooncake 不提供。 | 固定 `nil`。 |

Removed 示例：

```json
{
  "event_id": 2,
  "timestamp": 1780000000100,
  "event_type": "removed",
  "type": "BlockRemoved",
  "model_name": null,
  "block_size": null,
  "additional_salt": null,
  "lora_name": null,
  "tenant_id": "default",
  "backend_id": "mooncake-master-0",
  "medium": "disk",
  "dp_rank": null,
  "object_key": "42",
  "seq_hashes": [42],
  "block_hashes": [42],
  "base_block_idx": null
}
```

## 观测接口

`GET /kv_events/status` 返回：

| 字段 | 含义 |
|---|---|
| `enabled` | 当前 active master service 是否启用 KV events。 |
| `published_batches` | 已发送 batch 数。 |
| `published_events` | 已编码进 batch 的事件数。 |
| `dropped_events` | 当前实现没有递增路径，通常为 0。 |
| `skipped_unparsed_keys` | object key 无法解析为 u64 的次数。注意如果 `object_key` 仍被发送，这个计数也会增加。 |

## 当前对齐观察

1. Mooncake 目前发布的是 RFC 风格字段加 legacy alias 的混合格式：`event_type` / `seq_hashes` / `object_key` 是 RFC 风格，`type` / `block_hashes` 是 legacy 兼容字段。
2. `model_name`、`block_size`、`additional_salt`、`lora_name`、event 内 `dp_rank` 都固定为 `nil`，实现意图是由 indexer registration 提供这些 stream dimensions。
3. Stored 事件没有 `token_ids` 和 `block_size`。如果下游严格按 SGLang/vLLM ZMQ `BlockStored` 解码，可能无法接受 `token_ids=nil` / `block_size=nil`。
4. `medium` 当前是小写 `"cpu"` / `"disk"`。如果下游只识别大写 medium，需要转换或扩展映射。
5. 如果 Mooncake object key 不是 decimal/hex u64，那么 `seq_hashes` / `block_hashes` 会是空数组，只能依赖 `object_key` 做匹配。

