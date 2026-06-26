# Mooncake 与 Dynamo KV Event 对齐分析报告

分析版本：

- Mooncake PR #2214 head: `6e133671e13be615799099e4a1ead3b7ad53dfa5`
- Dynamo main: `1b37e82ea778a1982eee5318fa78d70d21027370`
- SGLang main: `413aeac0c9f7cd71cf700b42bed42fad074902e3`

相关背景文档：

- `mooncake_kv_event_rules.md`
- `dynamo_kv_event_rules.md`

## 结论

Mooncake PR #2214 当前发布的 KV event **不能直接满足 Dynamo 当前 ZMQ relay 的要求**。两边在 ZMQ 三帧批格式上基本对齐，但事件字段语义没有对齐，尤其是 `BlockStored` 缺少 Dynamo 当前强依赖的 `token_ids` 和非空 `block_size`。Dynamo 当前实现不是单纯转发 legacy event，而是会先反序列化 raw event，再用 `token_ids + kv_block_size + lora_name/mm_info` 重新计算内部 `tokens_hash`，最后生成 `RouterEvent`。

当前直接连接时的结果大致是：

- Mooncake `stored` event：大概率整个 batch 在 Dynamo ZMQ 解码阶段失败，因为 `token_ids=null`、`block_size=null`，而 Dynamo map parser 要求二者为非空值。
- Mooncake `removed` event：如果 `kv_events_emit_legacy_compat=true` 且 `block_hashes` 非空，可以被 Dynamo 当作 `BlockRemoved` 解析；但 `medium="cpu"/"disk"` 是小写，Dynamo 会把未知 medium 默认成 `Device`，导致 storage tier 错误。
- 如果 Mooncake 关闭 legacy 字段，只发 `event_type` / `seq_hashes`，Dynamo 当前 parser 不识别这些字段，会因为缺少 `type` 而拒绝。

因此，这个 PR 更像是在 Mooncake 侧实现了一个“存储池对象生命周期事件”的雏形，而不是已经兼容了 Dynamo 当前的 SGLang/vLLM ZMQ KV event 规范。

## 1. Dynamo 当前真正消费什么

Dynamo 文档给 custom engine 两种路径：

- Direct publishing：引擎调用 `KvEventPublisher.publish_stored()` / `publish_removed()`。
- ZMQ relay：Dynamo 订阅 SGLang/vLLM 风格 ZMQ PUB，把 raw event 转成内部 `RouterEvent`。

Mooncake master 当前只能走 ZMQ relay 路径。这个路径要求三帧消息：

```text
[topic, 8-byte big-endian seq, msgpack([timestamp, [events], dp_rank])]
```

这点 Mooncake 已经对齐。

Dynamo 对 map 形态 raw event 的关键要求如下：

| 字段 | Dynamo 当前要求 | 代码依据 |
|---|---|---|
| `type` | 必须是 `BlockStored` / `BlockRemoved` / `AllBlocksCleared`。不识别 `event_type`。 | `dynamo/lib/kv-router/src/zmq_wire/deserialize.rs:56` |
| `block_hashes` | `BlockStored` / `BlockRemoved` 都必需。signed i64 或 unsigned u64 都可。 | `dynamo/lib/kv-router/src/zmq_wire/deserialize.rs:101`, `:126` |
| `token_ids` | `BlockStored` 必需且不能为 null。 | `dynamo/lib/kv-router/src/zmq_wire/deserialize.rs:104` |
| `block_size` | `BlockStored` 必需且不能为 null。 | `dynamo/lib/kv-router/src/zmq_wire/deserialize.rs:106` |
| `parent_block_hash` | `BlockStored` 可选。 | `dynamo/lib/kv-router/src/zmq_wire/types.rs:64` |
| `medium` | 可选；缺失或未知默认 `Device`。只识别大写/标准字符串。 | `dynamo/lib/kv-router/src/protocols.rs:342` |
| `lora_name` | 可选；参与本地 token hash。 | `dynamo/lib/kv-router/src/zmq_wire/convert.rs:87` |

Dynamo conversion 的关键点是：`block_hashes` 被作为外部 sequence block hash 保存，而 `token_ids` 会被 Dynamo 重新按 `kv_block_size` 计算 local `tokens_hash`：

- `convert_event()` 将 raw event 转为 `KvCacheEventData::Stored`。
- `create_stored_blocks()` 要求每个 `num_block_tokens == kv_block_size`。
- `create_stored_block_from_parts()` 调 `compute_block_hash_for_seq()` 计算内部 token hash。

也就是说，Dynamo 当前不仅需要知道“哪个 sequence hash 存在”，还需要能建立 `external sequence hash -> local token hash` 的索引关系。

## 2. Mooncake 当前发布什么

Mooncake 的 publisher 位于 master 内，触发点是 Mooncake object lifecycle，而不是推理引擎 KV block lifecycle：

- `PutEnd()` 完成对象写入后发布 stored。
- `Remove()` 删除对象前发布 removed。
- disk / memory eviction 删除 replica 时发布 removed。

Mooncake 当前事件 map 主要字段如下：

| 字段 | Mooncake 当前值 | 代码依据 |
|---|---|---|
| `event_type` | `stored` / `removed` | `Mooncake/mooncake-store/src/kv_event/kv_event_publisher.cpp:264` |
| `type` | 默认开启 legacy 时为 `BlockStored` / `BlockRemoved` | `Mooncake/mooncake-store/src/kv_event/kv_event_publisher.cpp:266` |
| `model_name` | 固定 null | `Mooncake/mooncake-store/src/kv_event/kv_event_publisher.cpp:272` |
| `block_size` | 固定 null | `Mooncake/mooncake-store/src/kv_event/kv_event_publisher.cpp:274` |
| `additional_salt` | 固定 null | `Mooncake/mooncake-store/src/kv_event/kv_event_publisher.cpp:276` |
| `lora_name` | 固定 null | `Mooncake/mooncake-store/src/kv_event/kv_event_publisher.cpp:278` |
| `tenant_id` | 对象操作参数，空则 `default` | `Mooncake/mooncake-store/src/kv_event/kv_event_publisher.cpp:252` |
| `backend_id` | `kv_events_backend_id` | `Mooncake/mooncake-store/src/kv_event/kv_event_publisher.cpp:282` |
| `medium` | 小写 `cpu` / `disk` | `Mooncake/mooncake-store/src/master_service.cpp:7519` |
| `dp_rank` | event 内 null；batch trailer 固定 0 | `Mooncake/mooncake-store/src/kv_event/kv_event_publisher.cpp:286`, `:333` |
| `object_key` | Mooncake 原始对象 key | `Mooncake/mooncake-store/src/kv_event/kv_event_publisher.cpp:289` |
| `seq_hashes` | 从 object key 解析出的单个 u64，或空数组 | `Mooncake/mooncake-store/src/kv_event/kv_event_publisher.cpp:294` |
| `block_hashes` | legacy alias，同 `seq_hashes`，或空数组 | `Mooncake/mooncake-store/src/kv_event/kv_event_publisher.cpp:302` |
| `token_ids` | stored 中固定 null | `Mooncake/mooncake-store/src/kv_event/kv_event_publisher.cpp:318` |
| `parent_hash` / `parent_block_hash` | stored 中固定 null | `Mooncake/mooncake-store/src/kv_event/kv_event_publisher.cpp:316`, `:321` |

`seq_hashes` 的解析规则也很窄：只接受完整 decimal u64，或 `0x`/`0X` 前缀 hex u64。普通 64 字符 SHA256 hex、带 `_k/_v` 后缀的 key、带 tag prefix 的 key 都解析失败。

## 3. 字段级差异与影响

| 维度 | Dynamo 当前要求 | Mooncake 当前提供 | 是否影响对齐 | 影响说明 |
|---|---|---|---|---|
| ZMQ frames | 3 帧：topic、seq、payload | 3 帧，空 topic，big-endian seq，payload | 不影响 | 传输外壳基本对齐。Mooncake seq 从 1 开始，Dynamo 只用于日志，不是问题。 |
| payload shape | `[timestamp, [events], dp_rank]` | `[timestamp_ms, [events], 0]` | 基本不影响 | timestamp 单位不同，Dynamo 当前不用于内部事件。 |
| event discriminator | map 必须有 `type` | 默认有 `type`，但可关闭；另有 `event_type` | 有条件影响 | 关闭 legacy 后 Dynamo 完全不识别。Dynamo 当前不读 `event_type`。 |
| stored `token_ids` | 必需，非 null | null | 严重影响 | 当前 Dynamo 会反序列化失败，stored batch 无法进入 indexer。 |
| stored `block_size` | 必需，非 null | null | 严重影响 | 当前 Dynamo 会反序列化失败；即使能解析，转换也要求等于 `kv_block_size`。 |
| `block_hashes` | 必需；作为 external sequence hash | 只有 object key 可解析时才有；否则空数组 | 严重影响 | SGLang 写 Mooncake 的 key 不是 Mooncake 当前解析器支持的格式，实际可能经常为空。 |
| `seq_hashes` | 当前 Dynamo ZMQ 不读 | Mooncake 主要 RFC 字段 | 影响 | 这是两边 spec 名称不一致。Dynamo 当前 relay 只认 legacy `block_hashes`。 |
| `object_key` | 当前 Dynamo ZMQ 不读 | 默认提供 | 影响 | 对现有 Dynamo 无效。需要新 adapter 才能利用。 |
| `medium` | 识别 `GPU`、`CPU_PINNED`、`DISK`、`EXTERNAL` 等大写值 | 小写 `cpu` / `disk` | 严重影响 | 当前会默认成 `Device`，把 Mooncake host/disk 事件误认为 GPU/device。 |
| `dp_rank` | batch 中用于 worker+DP 分片 | batch 固定 0，event 内 null | DP 部署影响 | 多 DP 时 Mooncake master 无法表达真实 DP owner。 |
| `model_name` / tenant / backend | Dynamo 当前 ZMQ publisher 从 component/worker 配置拿身份，不消费 event 内这些字段 | Mooncake 提供 tenant/backend，但 model null | 影响集成形态 | 当前 Dynamo 不会用 `backend_id` 区分 storage daemon。需要 registration 或 adapter。 |
| `parent_block_hash` | stored 可选，但连续 batch 会用 parent 连续性做 batching | null | 轻到中等 | 单 block stored 可为 null；但没有 parent 会降低连续 prefix 表达能力。 |
| clear event | 支持 `AllBlocksCleared` | Mooncake 不发 | 次要 | master restart / 全量失效场景没有清空语义，需要靠 remove 或重建。 |

## 4. 直接接入当前 Dynamo 会发生什么

### 4.1 Stored event 不能通过当前 ZMQ parser

Mooncake stored map 默认包含：

```json
{
  "type": "BlockStored",
  "block_hashes": [...],
  "token_ids": null,
  "block_size": null
}
```

Dynamo map parser 在看到 `token_ids` 时会按 `KvTokenIds` 解码，null 不是合法 token list；看到 `block_size` 时也会按 `usize` 解码，null 不合法。因此不是“字段缺了可以 fallback”，而是当前 batch 解码就会失败。

即使后续让 parser 接受 null，`convert_event()` 仍需要 `token_ids` 来计算 `tokens_hash`。没有 token 信息，Dynamo 当前标准 ZMQ path 无法构造可查询的 stored block。

### 4.2 Removed event 只部分可用

Mooncake removed event 不需要 `token_ids` / `block_size`，所以它更接近 Dynamo 的 `BlockRemoved`。但仍有两个问题：

- 如果 object key 解析失败，`block_hashes=[]`，删除事件没有实际效果。
- `medium="cpu"/"disk"` 不被 Dynamo `StorageTier::from_kv_medium()` 识别，会默认成 `Device`，导致删除错误 tier 的 block。

### 4.3 `event_type` / `seq_hashes` / `object_key` 当前都被 Dynamo 忽略

Mooncake PR 中的 RFC 风格字段对当前 Dynamo ZMQ relay 没有意义，因为 parser 只读 `type`、`block_hashes`、`token_ids`、`block_size`、`medium` 等 SGLang/vLLM 字段。`event_type`、`seq_hashes`、`object_key`、`backend_id`、`tenant_id` 都会被当作 unknown key 跳过。

## 5. SGLang 当前能提供什么

SGLang 已经有一套更贴近 Dynamo 的 KV event：

- `BlockStored` 带 `block_hashes`、`parent_block_hash`、`token_ids`、`block_size`、`medium`。
- `BlockRemoved` 带 `block_hashes`、`medium`。
- batch payload 是 `[ts, events, attn_dp_rank]`。
- medium 使用 `GPU` / `CPU_PINNED` / `DISK` / `EXTERNAL` 这类 Dynamo 可识别的大写值。

字段来源：

- `token_ids` 来自 radix tree node 的 `node.key.token_ids`。
- `block_size` 来自 `page_size` 分块后的页面长度。
- `parent_block_hash` 来自父节点最后一个 `hash_value`。
- `block_hashes` 是 `node.hash_value` 的前 64 bit 转 signed int64。
- `attn_dp_rank` 由 publisher 按 attention DP rank 填入。

这说明：**token / parent / block size 这些信息在 SGLang 引擎层是有的，在 Mooncake master 层没有。**

另一个重要点是 SGLang 写 Mooncake Store 的 key：

- SGLang radix `hash_value` 是 SHA256 hex string。
- 写 storage 时把 `hash_value` 作为逻辑 key 传给 storage controller。
- Mooncake Store backend 会把逻辑 key 扩展成实际 object key：
  - MHA 常见为 `{hash}_{rank}_k` 和 `{hash}_{rank}_v`。
  - MLA 常见为 `{hash}_{pp_rank}_k` 或 `{hash}_k`。
  - 如果配置了 `extra_backend_tag`，还会加前缀。
  - split-head / hybrid sidecar pool 会产生更多 component key。

因此，Mooncake master 实际看到的 object key 很可能不是纯 hash，而是带前后缀的 component object key。当前 `ParseSeqHashFromObjectKey()` 无法从这些 key 中提取 Dynamo/SGLang event 使用的 64-bit block hash。

## 6. Mooncake 当前拿到的信息够不够

分两种目标看。

### 6.1 如果目标是兼容 Dynamo 当前 ZMQ relay：不够

Mooncake master 当前能拿到：

- object key
- tenant id
- replica type / metadata，可映射出粗粒度 medium
- backend id 配置
- event timestamp / 本地 event sequence

Mooncake master 当前拿不到：

- `token_ids`
- `block_size`
- `parent_block_hash`
- `dp_rank`
- `model_name`
- `lora_name`
- `additional_salt` / cache salt / hash namespace
- 多模态 block metadata
- SGLang/Dynamo 当前使用的 canonical external sequence hash，如果 object key 不是纯 decimal/0x u64
- 逻辑 KV block 与 Mooncake component object 的一对多关系，例如 MHA 的 K/V 两个对象

所以如果要求 Mooncake master 直接发出 Dynamo 当前 `BlockStored`，这些信息必须由 SGLang 额外传给 Mooncake，或写入 Mooncake object metadata。

### 6.2 如果目标是做一个 Mooncake storage-tier adapter：接近但仍不够

如果 Dynamo 新增 Mooncake adapter，不再要求 Mooncake stored event 自带 `token_ids`，而是把 Mooncake event 视为“某 lower tier 上某 sequence hash 可用/不可用”，那么 Mooncake 需要的信息少很多。

这种方案成立的前提是：

- SGLang 仍然向 Dynamo 发布 canonical GPU/CPU KV event，用 `token_ids` 帮 Dynamo 建好 `sequence hash -> token hash` 关系。
- Mooncake event 只补充 lower-tier availability，即 `seq_hash/object_key + medium + backend/worker identity`。
- Dynamo adapter 能把 Mooncake object key 转成同一个 external sequence hash，或通过 `object_key` 查到同一个逻辑 block。

即便如此，Mooncake 仍需要补齐：

- 能稳定从 object key 还原逻辑 block hash，或直接拿到 SGLang 的 block hash metadata。
- medium 映射到 Dynamo 识别的 tier，例如 `CPU_PINNED` / `DISK` / `EXTERNAL`。
- component key 去重/合并规则，避免 MHA 的 `_k` / `_v` 被当成两个 KV block。
- backend identity 与 Dynamo worker / storage endpoint 的注册关系。
- 多 DP 场景下每个 object 属于哪个 DP rank，或注册为 DP rank 0 的全局共享外部池。

## 7. 推荐修改路线

### 路线 A：在 Dynamo 侧加 Mooncake adapter，推荐

这是更合理的路线，因为 Mooncake master 是存储系统，不是推理引擎。它天然不知道 token 序列和模型上下文。

建议修改：

1. Dynamo ZMQ/RFC adapter 支持 Mooncake map：
   - 接受 `event_type=stored/removed`。
   - 读取 `seq_hashes`，必要时读取 `object_key`。
   - 读取 `backend_id`、`tenant_id`、`medium`。
   - 不要求 Mooncake stored event 带 `token_ids` / `block_size`。

2. Dynamo medium 解析兼容小写，或 Mooncake 改成发标准大写：
   - `cpu` -> `CPU_PINNED` 或 `HostPinned`
   - `disk` -> `DISK`
   - 如果 Mooncake 是远端共享池，可考虑 `EXTERNAL`

3. Adapter 使用已知 sequence hash 标记 lower-tier availability：
   - 如果 `seq_hashes` 非空，直接用它。
   - 如果只有 `object_key`，按 SGLang Mooncake key 规则解析逻辑 hash。
   - 如果无法解析，则只能记录 object-level metadata，不能参与 token prefix routing。

4. 对 Mooncake component object 做去重/合并：
   - MHA 的 `{hash}_{rank}_k` / `{hash}_{rank}_v` 应归并为同一个 logical KV page。
   - split heads 和 hybrid sidecar keys 也需要归并或过滤，只保留 base KV block。
   - 可以优先使用 Mooncake group semantics 的 group id，或者在 key parser 中识别 SGLang suffix。

5. 通过注册/配置补充 identity：
   - 每个 Mooncake publisher 注册 `worker_id/backend_id/tenant/model/block_size/dp_rank`。
   - 如果是共享 external pool，明确它是所有 worker 可访问，还是某个 worker/DP 独占。

优点：

- 不需要把大量 token 级信息塞进 Mooncake master。
- 保留 Mooncake 作为 storage-level source of truth。
- 可以和 SGLang 已有 Dynamo KV event 配合：SGLang 负责 token/hash 语义，Mooncake 负责 lower-tier availability。

风险：

- Dynamo 当前内部 store event 结构要求 stored block 有 `tokens_hash`。adapter 需要能从已有 index 找到对应 token hash，或扩展 lower-tier index 的输入结构，允许 “known sequence hash availability” 类型的事件。
- 如果 SGLang 没有同时发布 canonical KV event，Mooncake-only event 仍然不足以支持 token-prefix 查询。

### 路线 B：让 Mooncake 模拟 SGLang/vLLM ZMQ event，不推荐作为第一选择

为了让 Mooncake master 直接符合 Dynamo 当前 ZMQ relay，Mooncake stored event 至少要改成：

```json
{
  "type": "BlockStored",
  "block_hashes": [seq_hash],
  "parent_block_hash": parent_hash_or_null,
  "token_ids": [token ids for this block],
  "block_size": 128,
  "medium": "DISK",
  "lora_name": null
}
```

这要求 SGLang 在写 Mooncake object 时额外把以下信息传给 Mooncake：

- token ids
- page/block size
- parent block hash
- SGLang/Dynamo 使用的 external sequence hash
- dp rank
- model name 或模型 namespace
- lora name
- additional salt / cache salt / extra key
- 多模态 metadata，如果要支持 MM routing
- logical block id 与实际 K/V component object 的映射

这会把推理引擎语义强行灌进存储 master，复杂度较高，也容易和 SGLang 已有 KV event 重复。

### 路线 C：只修 Mooncake 少量字段，仍然不够

例如只把 `medium` 改成大写、让 `block_hashes` 非空、保证 `type` 一直发出，可以让 `BlockRemoved` 更可用，但 `BlockStored` 仍然缺 `token_ids/block_size`，不能满足当前 Dynamo ZMQ relay。

所以“少量字段修正”只能作为 Mooncake adapter 的前置改进，不能单独完成对齐。

## 8. SGLang 需要额外改什么

按推荐路线 A，SGLang 不需要把所有 token 级信息都传给 Mooncake，但需要做几类关键对齐。

### 8.1 保证 Mooncake object key 可映射到 Dynamo external block hash

当前 SGLang 的逻辑 hash 是 SHA256 hex，而 Dynamo/SGLang ZMQ event 对外发的是该 hex 前 16 位转 signed int64 后的值。Mooncake 当前只支持 decimal/`0x` u64 解析，且实际 object key 可能带 `_k/_v` 后缀。

可选修改：

- SGLang 写 Mooncake 时把 logical key 改成 Dynamo external hash 的 decimal 或 `0x` u64 格式。
- 或 Mooncake parser 支持 SGLang key 格式：
  - 识别 64 hex hash 前缀。
  - 取前 16 hex 转 u64，与 SGLang `hash_str_to_int64()` 的二进制值一致。
  - 忽略后缀 `_rank_k` / `_rank_v` / split-head suffix。
  - 忽略可选 `extra_backend_tag_`。
- 或 SGLang/Mooncake 存一份 object metadata：`logical_block_hash_u64`。

### 8.2 传递或暴露 logical block 与 component object 的关系

Mooncake master 当前按 object key 发事件，但一个 SGLang logical KV page 可能对应多个 Mooncake object：

- MHA：K/V 两个 object。
- split heads：更多 K/V component object。
- MLA：通常一个 object。
- hybrid sidecar：额外 pool object。

如果 Dynamo adapter 看到每个 component object 都 store/remove 一次，就可能重复计数或错误删除。需要 SGLang 或 Mooncake 提供：

- group id，例如同一个 logical page 的所有 component object 共享 `sglang-hicache:{logical_key}`。
- component type 标记，只让 base KV logical page 触发一次事件。
- 或在 Mooncake publisher 内 coalesce 同 group 的 component events。

### 8.3 提供 DP / model / block_size / hash namespace 的注册信息

Mooncake master 事件本身没有这些上下文。SGLang 知道：

- `page_size` / block size
- `attn_dp_rank`
- `model_name`
- server-level cache salt / extra key 语义

这些信息可以不进入 Mooncake event，但必须进入 Dynamo registration/adapter 配置。多 DP 部署尤其需要明确 Mooncake object 属于哪个 DP rank，或声明 Mooncake 是全局共享 external pool。

### 8.4 LoRA 与 cache salt 需要额外处理

Dynamo 当前支持 `lora_name`，会把 adapter 名混入 local token hash。SGLang 当前 KV event 的 `BlockStored` 只有旧的 `lora_id` slot，事件生成处写的是 `lora_id=None`，没有 `lora_name`。SGLang `RadixKey.extra_key` 参与 radix key 匹配，但当前 `hash_page()` 本身只 hash token 和 parent hash，没有把 `extra_key` 写进 hash。

如果要让 Dynamo 的 LoRA-aware routing 完整对齐，SGLang 需要：

- 在 KV event 中提供 `lora_name`，不只是 `lora_id`。
- 明确 cache salt / `extra_key` 是否应参与 external hash 或 registration namespace。
- 如果 Mooncake object key 也要区分 LoRA/cache salt，需要把这些 namespace 信息纳入 key 或 metadata。

### 8.5 保持现有 SGLang -> Dynamo KV event

最稳妥的部署形态是：

```text
SGLang KV event -> Dynamo：提供 token_ids/block_size/parent/hash，建立 prefix index。
Mooncake master event -> Dynamo adapter：提供 lower-tier object availability。
```

不要用 Mooncake master event 替代 SGLang engine event。Mooncake 适合补充“哪些 KV block 已经落到 Mooncake 存储池”，不适合单独承担 token-prefix hash 建模。

## 9. 最小改动建议

### Mooncake 侧

1. `medium` 改成 Dynamo 可识别的标准值，或至少双写：
   - memory -> `CPU_PINNED`
   - disk/local disk/nof ssd -> `DISK` 或 `EXTERNAL`

2. 扩展 key parser：
   - 支持 64 hex SHA256 string。
   - 支持从 SGLang component key 中提取 logical hash prefix。
   - 支持把 first 16 hex 转成与 SGLang `hash_str_to_int64()` 一致的 u64。

3. 避免每个 K/V component 都发布成独立 logical KV block：
   - 利用 group id 或 key suffix coalesce。
   - 至少在 event 中加 `object_key`、`component_kind`、`group_id`，让 Dynamo adapter 能过滤。

4. 不要声称当前 legacy 字段已经能直接 forward 到 Dynamo。按当前 Dynamo 代码，stored 仍会因为 token/block_size 失败。

### Dynamo 侧

1. 如果要支持 Mooncake PR #2214，新增 adapter/parser：
   - 识别 `event_type` / `seq_hashes` / `object_key`。
   - 接受 `token_ids=null` 的 storage-level stored event。
   - 将它转换为 lower-tier availability，而不是普通 engine `BlockStored`。

2. medium parser 兼容小写，避免未知 medium 默认到 Device：
   - `cpu` -> `HostPinned`
   - `disk` -> `Disk`
   - `external` / `mooncake` -> `External`

3. 支持从已存在的 engine index 补全 `tokens_hash`：
   - 如果 sequence hash 已由 SGLang engine event 建立过 token hash 映射，则 Mooncake event 只更新 tier availability。
   - 如果没见过该 sequence hash，则记录 pending 或忽略，不能凭空支持 token-prefix query。

4. 把 `backend_id` / `tenant_id` / `dp_rank` 纳入 source registration，而不是依赖当前 ZMQ event parser 忽略它们。

### SGLang 侧

1. 确保 Mooncake object key 或 metadata 能携带 Dynamo/SGLang 一致的 external sequence hash。
2. 提供 logical KV page 到 Mooncake component object 的 group/coalesce 信息。
3. 提供 model/block_size/dp_rank/hash namespace registration。
4. 如需 LoRA-aware routing，补齐 `lora_name` 和 cache salt 语义。
5. 保留现有 SGLang KV event publisher，让它继续向 Dynamo 提供 token-level event。

## 10. 最终判断

当前 Mooncake 提供的 KV event 与 Dynamo 当前要求存在实质差异，不是简单字段命名差异。最核心的不对齐是：

1. Mooncake 是 storage object event；Dynamo 当前 ZMQ relay 消费的是 engine KV block event。
2. Mooncake stored 缺 `token_ids` 和 `block_size`；Dynamo 当前必须用它们建立内部 token hash。
3. Mooncake 的 `seq_hashes/object_key` 当前不被 Dynamo relay 消费。
4. Mooncake `medium` 小写会被 Dynamo 误判为 `Device`。
5. SGLang 写入 Mooncake 的 key 与 Mooncake 当前 `seq_hash` parser 不匹配，且一个 logical block 可能对应多个 Mooncake object。

所以，对齐方案不应是“让 Mooncake master 假装成 SGLang/vLLM engine event 源”，而应是：

- SGLang 继续发布 Dynamo 兼容的 token-level KV event；
- Mooncake 发布 storage-level availability event；
- Dynamo 增加 Mooncake-aware adapter，将两类事件合并到同一个 KV-aware routing index 中。
