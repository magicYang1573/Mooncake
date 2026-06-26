# Dynamo KV Event 规则

分析版本：`ai-dynamo/dynamo` main `1b37e82ea778a1982eee5318fa78d70d21027370`。

主要源码依据：

- `docs/integrations/kv-events-custom-engines.md`
- `lib/bindings/python/rust/llm/kv.rs`
- `lib/llm/src/kv_router/publisher/mod.rs`
- `lib/llm/src/kv_router/publisher/zmq_listener.rs`
- `lib/llm/src/kv_router/publisher/event_processor.rs`
- `lib/llm/src/kv_router/publisher/batching.rs`
- `lib/llm/src/kv_router/publisher/dedup.rs`
- `lib/kv-router/src/zmq_wire/types.rs`
- `lib/kv-router/src/zmq_wire/deserialize.rs`
- `lib/kv-router/src/zmq_wire/convert.rs`
- `lib/kv-router/src/zmq_wire/mod.rs`
- `lib/kv-router/src/zmq_wire/README.md`
- `lib/kv-router/src/protocols.rs`

## 总体模型

Dynamo 的 KV-aware routing 需要 worker 持续告诉 router：哪些 KV cache block 在哪个 worker / DP rank / storage tier 上存在。KV Router 根据这些事件维护 prefix cache index，并在新请求进来时选择能命中更多 KV block 的 worker。

当前文档给 custom engine 两种接入模式：

1. Direct publishing：引擎直接调用 Python 绑定 `KvEventPublisher.publish_stored()` / `publish_removed()`。
2. ZMQ relay：引擎像 SGLang/vLLM 一样在本地 ZMQ PUB socket 发原始 KV event，Dynamo `KvEventPublisher` 订阅后转成内部 `RouterEvent` 并发布到 Dynamo event plane。

事件最终进入的内部结构是：

```text
RouterEvent {
  worker_id,
  storage_tier,
  event: KvCacheEvent {
    event_id,
    dp_rank,
    data: Stored | Removed | Cleared
  }
}
```

事件 plane subject 是 `kv-events`。

## Event 类型

| 类型 | 外部文档名 | 内部类型 | 触发时机 |
|---|---|---|---|
| store | `BlockStored` | `KvCacheEventData::Stored` | KV cache block 分配/写入成功后。 |
| remove | `BlockRemoved` | `KvCacheEventData::Removed` | KV cache block 被 evict/free 时。 |
| clear | `AllBlocksCleared` | `KvCacheEventData::Cleared` | cache reset 或 worker restart 时。 |

## Direct Publishing API

Python 绑定当前构造函数签名：

```python
KvEventPublisher(
    endpoint,
    worker_id=None,
    kv_block_size=0,
    dp_rank=0,
    enable_local_indexer=False,
    zmq_endpoint=None,
    zmq_topic=None,
    batching_timeout_ms=None,
    image_token_id=None,
)
```

| 参数 | 含义 |
|---|---|
| `endpoint` | Dynamo endpoint，绑定到当前 model/component。Rust 侧从它取 `component`。 |
| `worker_id` | 可选 worker id。不传时用 runtime connection id。 |
| `kv_block_size` | KV block size，必须大于 0，并且要和引擎真实 block size 一致。 |
| `dp_rank` | 当前 publisher 所属 data-parallel rank，默认 0。 |
| `enable_local_indexer` | 是否在 worker 内启用 local indexer，供 router 直接恢复/查询事件。 |
| `zmq_endpoint` | 如果设置，则进入 ZMQ relay 模式；否则 direct publishing。 |
| `zmq_topic` | ZMQ topic filter；默认空字符串。 |
| `batching_timeout_ms` | 事件 batching flush 超时时间；`None` / `0` 表示不按时间等待，最大 15000ms。 |
| `image_token_id` | 多模态场景中 vLLM image placeholder token id，用于 ZMQ relay 的 hash normalization。 |

### `publish_stored`

签名：

```python
publish_stored(
    token_ids: list[int],
    num_block_tokens: list[int],
    block_hashes: list[int],
    parent_hash: int | None = None,
    block_mm_infos: list[dict | None] | None = None,
    lora_name: str | None = None,
    is_eagle: bool | None = None,
)
```

字段含义：

| 字段 | 类型 | 含义 | 当前处理规则 |
|---|---|---|---|
| `token_ids` | `u32[]` | 这些 stored blocks 覆盖的 token ids。 | 用于按 `kv_block_size` 分块并重新计算 `tokens_hash`。 |
| `num_block_tokens` | `u64[]` | 每个 block 的 token 数。 | 每项必须等于 `kv_block_size`；遇到不等的 block 后停止生成后续 blocks。 |
| `block_hashes` | signed i64 list | 引擎 block manager 给出的 sequence block hashes。 | 转成 `u64` 后作为 `ExternalSequenceBlockHash`，用于 prefix index。 |
| `parent_hash` | signed i64 或 `None` | 第一个 stored block 的父 sequence hash。 | 转成 `ExternalSequenceBlockHash`；根 block 可为 `None`。 |
| `block_mm_infos` | optional list | 每个 block 的多模态对象信息。 | 参与 `tokens_hash` 计算，避免同 token 不同图像混淆。 |
| `lora_name` | optional string | LoRA adapter 名。 | 混入本地 token hash 的 seed，避免 base model 和不同 adapter 的 block 混淆。 |
| `is_eagle` | optional bool | Eagle/speculative 相关 token 窗口标记。 | 影响每个 block 用于 hash 的 token 窗口长度。 |

内部转换为：

```text
KvCacheEventData::Stored {
  parent_hash,
  start_position: None,
  blocks: [
    KvCacheStoredBlockData {
      block_hash,
      tokens_hash,
      mm_extra_info
    }
  ]
}
```

其中：

- `block_hash` 是外部 sequence block hash，即 custom engine 传进来的 `block_hashes[i]`。
- `tokens_hash` 是 Dynamo 根据 `token_ids`、`kv_block_size`、`lora_name`、多模态信息重新计算出的 local block hash。
- `event_id` 由 publisher 管理；Python 调用方不需要提供。

### `publish_removed`

签名：

```python
publish_removed(block_hashes: list[int])
```

字段含义：

| 字段 | 类型 | 含义 | 当前处理规则 |
|---|---|---|---|
| `block_hashes` | signed i64 list | 被删除的 sequence block hashes。 | 转成 `ExternalSequenceBlockHash`，生成 `KvCacheRemoveData { block_hashes }`。 |

Direct Python 绑定当前没有公开 `publish_cleared()`；内部结构和 ZMQ relay 都支持 clear。

## ZMQ Relay Wire Format

Dynamo 当前 ZMQ relay 兼容 SGLang/vLLM 风格消息：

| Frame | 内容 | Dynamo 处理 |
|---|---|---|
| 1 | topic | 订阅时由 `zmq_topic` filter 控制；listener 要求总帧数正好为 3。 |
| 2 | 8 字节 big-endian sequence | 解码为 `engine_seq`，目前主要用于 trace/log，不作为内部 `event_id`。 |
| 3 | msgpack payload | 解码为 `[timestamp, [events], dp_rank]`。 |

payload：

```text
[
  timestamp,
  [
    raw_event,
    raw_event
  ],
  dp_rank
]
```

| payload 字段 | 类型 | 含义 |
|---|---|---|
| `timestamp` | `f64` | engine 侧 batch 时间戳。当前转换逻辑不把它写入内部 `KvCacheEvent`。 |
| `events` | array | 一批 raw KV event。 |
| `dp_rank` | optional i32 | batch 所属 DP rank；缺省按 0。 |

Dynamo 的 ZMQ 解码器支持两种 raw event 形状：

- map/object event：按字段名解析，未知字段忽略。
- positional tuple event：兼容 Python `msgspec(tag=True, array_like=True)`。

### ZMQ map event 字段

`BlockStored`：

```python
{
    "type": "BlockStored",
    "block_hashes": [signed_i64_or_u64, ...],
    "parent_block_hash": signed_i64_or_u64 | None,
    "token_ids": [int, ...],
    "block_size": int,
    "medium": str | None,
    "lora_name": str | None,
    "block_mm_infos": list | None,
    "extra_keys": list | None,
    "is_eagle": bool | None,
    "group_idx": int | None,
    "kv_cache_spec_kind": str | None,
    "kv_cache_spec_sliding_window": int | None,
}
```

字段含义：

| 字段 | 必需 | 含义 | 当前处理规则 |
|---|---:|---|---|
| `type` | 是 | 必须是 `"BlockStored"`。 | map 解码只认 `type`，不认 `event_type`。 |
| `block_hashes` | 是 | sequence block hashes。 | signed/unsigned 都接受，统一转 `u64`。 |
| `parent_block_hash` | 否 | 第一个 block 的父 sequence hash。 | `None` 表示根。 |
| `token_ids` | 是 | stored blocks 对应 token ids。 | 不能是 `null`；用于重新计算 local `tokens_hash`。 |
| `block_size` | 是 | event 声明的每个 block token 数。 | 转成 `num_block_tokens=[block_size]*len(block_hashes)`；后续必须等于 publisher 的 `kv_block_size`。 |
| `medium` | 否 | cache medium。 | 映射成 `StorageTier`；缺省或未知时默认为 Device。 |
| `lora_name` | 否 | LoRA adapter 名。 | 参与 local hash seed。 |
| `block_mm_infos` | 否 | block-level 多模态 metadata。 | 优先使用；否则尝试从 `extra_keys` 转换。 |
| `extra_keys` | 否 | vLLM-style 多模态额外 key。 | 可转换为 `block_mm_infos`。 |
| `is_eagle` | 否 | Eagle/speculative 标记。 | 影响 block token window。 |
| `group_idx` | 否 | KV cache group index。 | 用于过滤非主 attention 组。 |
| `kv_cache_spec_kind` | 否 | cache spec 类型。 | 只接受 main attention 类型；非主类型会过滤。 |
| `kv_cache_spec_sliding_window` | 否 | sliding window 参数。 | 作为 group metadata 尾字段记录。 |

`BlockRemoved`：

```python
{
    "type": "BlockRemoved",
    "block_hashes": [signed_i64_or_u64, ...],
    "medium": str | None,
    "group_idx": int | None,
    "kv_cache_spec_kind": str | None,
    "kv_cache_spec_sliding_window": int | None,
}
```

| 字段 | 必需 | 含义 | 当前处理规则 |
|---|---:|---|---|
| `type` | 是 | 必须是 `"BlockRemoved"`。 | map 解码只认 `type`。 |
| `block_hashes` | 是 | 要删除的 sequence block hashes。 | signed/unsigned 都接受，统一转 `u64`。 |
| `medium` | 否 | cache medium。 | 映射成 `StorageTier`。 |
| `group_idx` | 否 | KV cache group index。 | 用于过滤非主 attention 组。 |
| `kv_cache_spec_kind` | 否 | cache spec 类型。 | 非主类型会过滤。 |
| `kv_cache_spec_sliding_window` | 否 | sliding window 参数。 | 作为 group metadata 记录。 |

`AllBlocksCleared`：

```python
{"type": "AllBlocksCleared"}
```

`Ignored` 也可被解码，但会被 normalizer 丢弃。

### ZMQ positional event 字段

`BlockStored` fixed prefix：

| 位置 | 字段 |
|---:|---|
| 0 | tag，`"BlockStored"` |
| 1 | `block_hashes` |
| 2 | `parent_block_hash` |
| 3 | `token_ids` |
| 4 | `block_size` |
| 5 | old `lora_id` slot，当前消费后丢弃 |
| 6 | `medium` |
| 7 | `lora_name` |
| 8 | `extra_keys` |

`BlockStored` optional tail：

| 顺序 | 字段 |
|---:|---|
| 1 | `block_mm_infos` |
| 2 | `group_idx` |
| 3 | `kv_cache_spec_kind` |
| 4 | `kv_cache_spec_sliding_window` |

`BlockRemoved` fixed prefix：

| 位置 | 字段 |
|---:|---|
| 0 | tag，`"BlockRemoved"` |
| 1 | `block_hashes` |
| 2 | `medium` |

`BlockRemoved` optional tail：

| 顺序 | 字段 |
|---:|---|
| 1 | `group_idx` |
| 2 | `kv_cache_spec_kind` |
| 3 | `kv_cache_spec_sliding_window` |

positional event 如果要携带后面的字段，需要为前面的可选位置补 placeholder；如果没有后续字段，可以提前结束。

## medium 到 StorageTier 的映射

Dynamo 内部 `StorageTier`：

| StorageTier | 识别的 `medium` 字符串 |
|---|---|
| `Device` | `"GPU"` / `"DEVICE"`；缺省或未知值也会 fallback 到 `Device`。 |
| `HostPinned` | `"CPU"` / `"CPU_PINNED"` / `"CPU_TIER1"` |
| `Disk` | `"CPU_TIER2"` / `"DISK"` / `"NVME"` |
| `External` | `"EXTERNAL"` / `"NETWORK"` / `"REMOTE"` / `"SHARED"` |

注意：当前匹配是大小写敏感的。小写 `"cpu"` / `"disk"` 不会命中上述映射，会 fallback 到 `Device`。

## Hash 规则

Dynamo 文档要求 `block_hashes` 是 sequence block hashes，而不是单个 block 内 token 的 local hash。

内部有两类 hash：

| 名称 | 内部类型 | 含义 | 来源 |
|---|---|---|---|
| external sequence hash | `ExternalSequenceBlockHash` | engine 上报的 cumulative prefix hash。 | `block_hashes` / `parent_hash` / `parent_block_hash`。 |
| local token hash | `LocalBlockHash` | 单个 block 的 token 内容 hash。 | Dynamo 根据 `token_ids`、`kv_block_size`、LoRA、多模态信息重新计算。 |

默认 local hash 规则：

- seed 为 `XXH3_SEED = 1337`。
- 每个 token 按 little-endian `u32` 字节参与 hash。
- 普通 block 使用长度为 `kv_block_size` 的 token 窗口。
- `lora_name` 非空时，把 adapter name hash 混入 seed。
- 有多模态 metadata 时，mm hash 也参与 hash。
- `is_eagle=true` 时，窗口长度使用 `kv_block_size + 1`。

sequence hash 规则：

- 第一个 block 的 sequence hash 等于第一个 local block hash。
- 后续 block 使用 `compute_next_sequence_hash(parent_sequence_hash, current_local_block_hash)` 链式计算。
- 对 custom engine / ZMQ 生产者来说，`block_hashes` 应该已经是这类 sequence hashes。

## 内部事件字段

### `RouterEvent`

| 字段 | 类型 | 含义 | 来源 |
|---|---|---|---|
| `worker_id` | `u64` | 发出事件的 worker。 | `KvEventPublisher` 构造时传入或 runtime connection id。 |
| `storage_tier` | `StorageTier` | block 所在 tier。 | direct publish 默认 Device；ZMQ 从 `medium` 映射；Rust `publish_with_storage_tier` 可显式指定。 |
| `event` | `KvCacheEvent` | KV cache 事件内容。 | direct API 或 ZMQ relay 转换。 |

### `KvCacheEvent`

| 字段 | 类型 | 含义 |
|---|---|---|
| `event_id` | `u64` | Dynamo publisher 侧事件序号。最终发布前 batching 会重新用 `next_publish_id` 从 1 编号。 |
| `dp_rank` | `u32` | data-parallel rank。direct API 来自构造参数；ZMQ 来自 payload 第 3 项。 |
| `data` | `Stored` / `Removed` / `Cleared` | 事件数据。 |

### `Stored`

| 字段 | 类型 | 含义 |
|---|---|---|
| `parent_hash` | `ExternalSequenceBlockHash` 或 `None` | 第一块的父 sequence hash；根 block 为 `None`。 |
| `start_position` | `u32` 或 `None` | positional replay 起始位置；direct/ZMQ 转换当前写 `None`。 |
| `blocks` | `KvCacheStoredBlockData[]` | 实际 stored blocks。 |

`KvCacheStoredBlockData`：

| 字段 | 类型 | 含义 |
|---|---|---|
| `block_hash` | `ExternalSequenceBlockHash` | 该 block 的 sequence hash。 |
| `tokens_hash` | `LocalBlockHash` | Dynamo 根据 token 内容计算的 local hash。 |
| `mm_extra_info` | optional | block 级多模态 metadata。 |

### `Removed`

| 字段 | 类型 | 含义 |
|---|---|---|
| `block_hashes` | `ExternalSequenceBlockHash[]` | 要从 index 中删除的 sequence hashes。 |

### `Cleared`

无额外字段。语义是清空该 worker 相关 KV 状态，并清空 publisher 侧去重 refcount。

## Event processor 规则

ZMQ relay 或 direct API 产生的 placement events 会先进入 `event_processor`：

- 检查 raw input event id 是否有 gap，并记录日志/指标。
- 按 `dp_rank` 和 `storage_tier` 分批。
- Removed 可以合并连续的 remove hashes。
- Stored 可以合并连续 stored blocks，但要求新 stored event 的 `parent_hash` 等于上一批最后一个 block 的 `block_hash`。
- 遇到 stored/remove 类型变化、`dp_rank` 变化、`storage_tier` 变化、parent 不连续、超时或 block 数超过 128，会 flush。
- Remove 事件经过 refcount 去重：同一 `(dp_rank, storage_tier, block_hash)` 多次 store 后，只有 refcount 降到 0 的 remove 才真正发出。
- Flush 后发布 `RouterEvent` 到 `kv-events` subject，并可同步写入 local indexer。

## 当前约束和容易踩的点

1. ZMQ map event 必须有 `type`，且值为 `BlockStored` / `BlockRemoved` / `AllBlocksCleared`。当前解码器不认 `event_type`。
2. ZMQ `BlockStored` 的 `block_hashes`、`token_ids`、`block_size` 是必需字段；`token_ids=null` 或 `block_size=null` 会导致反序列化失败。
3. `block_size` 必须和 `KvEventPublisher(kv_block_size=...)` 一致；不一致的 block 不会进入 index。
4. ZMQ 第二帧 engine sequence 目前不驱动内部排序；Dynamo relay 会用自己的 monotonic id。
5. `medium` 大小写敏感，小写未知值会 fallback 为 Device。
6. `block_hashes` 在文档中写 signed i64，但 Rust ZMQ decoder 实际接受 signed i64 和 unsigned u64；Python direct API 仍暴露为 signed i64 list。
7. Stored event 必须能提供 token 内容，Dynamo 才能计算 local `tokens_hash` 并把外部 sequence hash 放进 prefix index。
8. 如果 raw stored event 出现 parent hash 和 block hashes 内部重复，Dynamo 会把它转成空 remove no-op，避免错误清空整个 worker。

