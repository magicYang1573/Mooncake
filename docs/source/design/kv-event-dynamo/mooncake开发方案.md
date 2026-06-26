# Mooncake KV Event 语义化改造开发方案

## 1. 背景

Mooncake 当前的定位是分布式 KV cache 存储平台，天然知道对象的存储、删除、驱逐和生命周期变化。但 Dynamo 的 KV-aware routing 需要的不是单个存储对象事件，而是和请求前缀相关的逻辑 KV block 事件。

这里的关键差异是：

- Mooncake 现在看到的是 object，例如一个逻辑 KV block 在 Mooncake 中可能拆成 K/V 两个 object，或者在 MHA 下拆成多个 head 相关 object。
- Dynamo 需要的是逻辑 block，例如某段 token prefix 对应的 `block_hash`、`parent_block_hash`、`token_ids`、`block_size`、`medium`。
- SGLang 才知道 token 语义、prefix cache 节点、父子关系、hash 计算方式、block token 内容。

因此，如果只让 Mooncake 根据 object 存储行为直接发布 Dynamo event，信息是不够的；如果让 SGLang 和 Mooncake 都发布 event，又会出现两个事件源、两套生命周期、两套一致性语义，维护成本会比较高。

本方案建议：

1. SGLang 只负责在写入 Mooncake 时透传必要的逻辑 KV block 语义。
2. Mooncake 负责维护逻辑 block 和物理 object 的映射关系。
3. Mooncake 在逻辑 block 真正完成存储、删除或失效时，统一发布 Dynamo 兼容的 KV event。

这会让事件的所有权和存储生命周期保持一致：谁管理存储状态，谁发布存储状态事件。

## 2. 总体结论

需要定义新的语义化接口，但不建议新增一套独立的 Put API。

更推荐的做法是：

- 复用现有 `Put` / `BatchPut` / `Upsert` / `BatchUpsert` 写路径。
- 复用现有 `ReplicateConfig` 和 `group_ids` 机制。
- 在 `ReplicateConfig` 中新增结构化的 KV event metadata 字段。
- 用 SGLang 现有的 `extra_config` 只做功能开关和默认参数配置，不承载每个 block 的动态语义数据。

也就是说，接口层面应该是“扩展现有写入配置”，而不是“新建一套写入接口”。

原因如下：

- `token_ids`、`block_hash`、`parent_block_hash` 这类信息是每个逻辑 block 都不同的动态数据，不适合放进进程级或 backend 级 `extra_config`。
- `extra_config` 可以降低 SGLang 配置层改动，但它表达不了每次写入的 per-block 语义。
- `group_ids` 已经能表达多个 Mooncake object 属于同一个逻辑组，是连接 Mooncake object 和 Dynamo block 的合适基础。
- 现有 group 机制只知道“这些 object 属于同一组”，不知道“这组是否完整代表一个 Dynamo block”，所以需要增加 group manifest 和 completeness 语义。

## 3. 现有机制判断

### 3.1 Mooncake 现有 group 机制

Mooncake `ReplicateConfig` 当前已经支持 `group_ids`。

它的作用是把一批 object 归入同一个 group，让 master 维护 group 到 object member 的关系，并在 lease refresh、eviction 等流程中把这些 object 作为一组处理。

这个机制非常适合作为 Dynamo logical block 的底座，因为一个 Dynamo block 在 Mooncake 里可能对应多个物理 object。

但当前 group 机制还不够：

- 不知道这个 group 预期应该有几个 object。
- 不知道 group 内每个 object 代表 K、V、某个 head，还是其他组件。
- 不知道 group 对应的 `token_ids`。
- 不知道 group 对应的 `block_hash`、`parent_block_hash`、`block_size`。
- 不知道 group 什么时候才算完整可被 Dynamo routing 使用。
- 删除时只能看到 object 删除，不能可靠判断逻辑 block 是否从完整变成不可用。

所以 group 机制应该被保留并扩展，而不是绕开。

### 3.2 SGLang 现有 extra_config

SGLang Mooncake backend 当前已经有 `extra_config`，用于配置 Mooncake backend 行为，例如 master 地址、client 地址、group semantics 开关、额外 backend tag 等。

这个配置适合承载：

- 是否启用 Mooncake semantic KV event。
- Mooncake event 发布地址或 topic。
- 默认 `model_name`。
- 默认 `dp_rank` 或 rank 透传策略。
- 是否启用 group semantics。
- object layout 模式，例如 MHA、MLA、split-head。

但它不适合承载：

- 当前 block 的 `token_ids`。
- 当前 block 的 `block_hash`。
- 当前 block 的 `parent_block_hash`。
- 当前 block 的 expected object list。
- 当前写入 batch 中每个 logical block 的组件映射。

这些数据是 per-write 或 per-block 的，应该进入 Mooncake 的写入配置对象，而不是 backend 初始化配置。

## 4. 推荐接口方案

### 4.1 接口原则

接口应该满足五个要求：

1. 一个 Dynamo logical block 可以对应多个 Mooncake object。
2. Mooncake 可以知道这个 logical block 的所有 expected object。
3. Mooncake 可以在 expected object 全部完成后再发布 `BlockStored`。
4. Mooncake 可以在任一必要 object 删除或失效后发布 `BlockRemoved`。
5. SGLang 只需要在写入时透传语义，不需要自己再维护 event 生命周期。

### 4.2 扩展 ReplicateConfig

建议在 Mooncake 的 `ReplicateConfig` 中新增 group 级 KV event metadata。

概念接口如下：

```cpp
struct KvBlockComponentSpec {
    std::string object_key;
    std::string component_role;  // examples: "k", "v", "mla_k", "head_3_k"
    uint32_t component_index{0};
};

struct KvBlockEventMetadata {
    uint32_t schema_version{1};

    // Logical group identity.
    std::string group_id;

    // Dynamo logical block identity.
    uint64_t block_hash{0};
    std::optional<uint64_t> parent_block_hash;

    // Token semantic information required by Dynamo.
    std::vector<uint32_t> token_ids;
    uint32_t block_size{0};

    // Optional routing namespace fields.
    std::optional<uint32_t> dp_rank;
    std::string model_name;
    std::string lora_name;
    std::string additional_salt;

    // Physical layout expectation.
    uint32_t expected_object_count{0};
    std::vector<KvBlockComponentSpec> expected_components;

    // Event behavior.
    bool emit_stored_event{true};
    bool emit_removed_event{true};
};

struct ReplicateConfig {
    size_t replica_num{1};
    size_t nof_replica_num{0};
    bool with_soft_pin{false};
    bool with_hard_pin{false};
    std::vector<std::string> preferred_segments{};
    std::string preferred_segment{};
    std::vector<std::string> preferred_nof_segments{};
    bool prefer_alloc_in_same_node{false};
    ObjectDataType data_type{ObjectDataType::UNKNOWN};

    // Existing field.
    std::optional<std::vector<std::string>> group_ids{};

    // New field.
    std::optional<std::vector<KvBlockEventMetadata>> kv_event_metadata{};
};
```

`kv_event_metadata` 是 group 级数据，不是 object 级数据。一个 metadata item 对应一个 `group_id`，而这个 `group_id` 可以出现在多个 object 的 `group_ids` 中。

### 4.3 BatchPut 中的对齐规则

以 `BatchPut(keys, values, config)` 为例：

- `keys` 是 Mooncake 物理 object keys。
- `config.group_ids` 仍然和 `keys` 等长。
- `config.group_ids[i]` 表示 `keys[i]` 属于哪个 logical block group。
- `config.kv_event_metadata` 是 group 级列表，长度等于本次 batch 中涉及的 logical block 数量。
- 每个 `KvBlockEventMetadata.group_id` 必须出现在 `config.group_ids` 中。
- 如果 `KvBlockEventMetadata.expected_components` 非空，则其中的 `object_key` 必须能在当前 batch 或已有 group member 中找到。
- 如果 `expected_components` 为空，则 `expected_object_count` 必须大于 0。

这样可以避免在 K/V 或多 head object 上重复携带完整 `token_ids`。

示例：

```text
SGLang logical block:
  block_hash = 10001
  parent_block_hash = 9999
  token_ids = [101, 102, 103, ...]
  block_size = 64

Mooncake physical objects:
  10001_mha_k
  10001_mha_v

BatchPut:
  keys = [
    "10001_mha_k",
    "10001_mha_v"
  ]

  group_ids = [
    "sglang-hicache:10001",
    "sglang-hicache:10001"
  ]

  kv_event_metadata = [
    {
      group_id = "sglang-hicache:10001",
      block_hash = 10001,
      parent_block_hash = 9999,
      token_ids = [...],
      block_size = 64,
      expected_object_count = 2,
      expected_components = [
        { object_key = "10001_mha_k", component_role = "k", component_index = 0 },
        { object_key = "10001_mha_v", component_role = "v", component_index = 1 }
      ]
    }
  ]
```

Mooncake 只有在两个 object 都成功完成写入后，才发布一个 Dynamo `BlockStored` event。

### 4.4 单 Put 中的规则

对于单 object 写入：

- 如果不启用 KV event，行为不变。
- 如果启用 KV event，`group_ids` 可以只有一个元素。
- `kv_event_metadata` 可以有一个 item。
- 当 `expected_object_count == 1` 时，单 object 写入完成即可触发 `BlockStored`。

### 4.5 Upsert 规则

第一阶段建议让 group metadata 保持不可变：

- 如果同一个 `group_id` 已存在，并且新的 metadata 和旧 metadata 完全一致，允许作为重试或补写处理。
- 如果同一个 `group_id` 已存在，但新的 `block_hash`、`token_ids`、`block_size` 等语义字段发生变化，返回参数错误。
- 后续如果确实需要复用 group id，可以引入 `generation` 字段。

这样能避免 Dynamo 看到同一个 logical block group 被悄悄改写成另一个语义 block。

## 5. 是否使用 extra_config

结论：不要用 `extra_config` 作为 per-block 语义数据接口，但可以用它做功能开关和默认配置。

### 5.1 可以放入 extra_config 的内容

```json
{
  "enable_group_semantics": true,
  "enable_kv_event_metadata": true,
  "enable_mooncake_kv_event_publisher": true,
  "kv_event_model_name": "llama",
  "kv_event_default_medium": "EXTERNAL",
  "kv_event_layout": "mha",
  "kv_event_schema_version": 1
}
```

这些字段是 backend 级别或进程级别配置，适合放在 `extra_config`。

### 5.2 不应该放入 extra_config 的内容

```json
{
  "block_hash": 10001,
  "parent_block_hash": 9999,
  "token_ids": [101, 102, 103],
  "block_size": 64,
  "expected_object_count": 2
}
```

这些字段每个 block 都不同，不适合放在 `extra_config`。

如果强行用 `extra_config`，会带来几个问题：

- SGLang backend 初始化配置会被 per-request/per-block 数据污染。
- 多个 batch 并发写入时，无法表达每个 block 的不同 metadata。
- Mooncake 无法把 metadata 和具体写入事务绑定。
- 重试、失败、部分成功时很难判断 event 是否应该发布。
- 后续跨语言 binding 和 RPC 校验会变得不清晰。

### 5.3 是否可以用 JSON 作为过渡

如果第一版 pybind 或 RPC 扩展结构体成本较高，可以考虑在 `ReplicateConfig` 中增加一个过渡字段：

```cpp
std::optional<std::string> kv_event_metadata_json;
```

但它应该只是 Python binding 的临时兼容层，不建议作为长期规范。

更理想的内部模型仍然是结构化类型：

```cpp
std::optional<std::vector<KvBlockEventMetadata>> kv_event_metadata;
```

JSON 字段进入 Mooncake 后应尽早 parse 成结构化 manifest，再进入 master 的 group 状态管理。

## 6. Mooncake 内部状态设计

### 6.1 Group Manifest

Mooncake master 需要为启用 KV event 的 group 维护 manifest。

概念结构如下：

```cpp
struct KvGroupManifest {
    std::string tenant_id;
    std::string group_id;

    KvBlockEventMetadata metadata;

    std::unordered_set<std::string> expected_object_keys;
    std::unordered_set<std::string> current_object_keys;

    bool stored_event_published{false};
    bool removed_event_published{false};

    uint64_t generation{0};
};
```

如果 `expected_components` 非空，`expected_object_keys` 来自 `expected_components.object_key`。

如果 `expected_components` 为空，Mooncake 可以用 `expected_object_count` 判断完整性，但这种方式较弱。更推荐 SGLang 明确传入 expected object keys。

### 6.2 完整性判断

一个 group 可以发布 `BlockStored` 的条件是：

- group 有有效的 `KvBlockEventMetadata`。
- `expected_object_keys` 非空。
- 每个 expected object 都已经在 master 中存在。
- 每个 expected object 的写入已经完成。
- 对目标 medium 来说，每个 expected object 都已经达到可用状态。
- 当前 generation 尚未发布过对应 medium 的 stored event。

第一版可以只支持一个 medium，例如把 Mooncake store 对 Dynamo 表达为 `EXTERNAL`。

后续如果需要区分 DRAM、SSD、remote tier，可以把 event published 状态扩展为 per-medium：

```cpp
std::unordered_map<StorageMedium, bool> stored_event_published_by_medium;
std::unordered_map<StorageMedium, bool> removed_event_published_by_medium;
```

### 6.3 删除和驱逐判断

当 group 已经发布过 `BlockStored` 后，任一 expected object 被删除、驱逐或变为不可用，都应该让这个 logical block 对 Dynamo 不再可用。

因此 `BlockRemoved` 的发布条件是：

- group 之前已经发布过 `BlockStored`。
- 任一 expected object 被删除或失效。
- 当前 generation 尚未发布过对应 medium 的 removed event。

对于 object 级别删除，Mooncake 要先找到其所属 group，再判断这个 object 是否属于该 group 的 expected object set。

## 7. Event 发布格式

Mooncake 对 Dynamo 发布的 event 应该保持 Dynamo 兼容，而不是发布 Mooncake object 原生事件。

### 7.1 BlockStored

建议格式：

```json
{
  "type": "BlockStored",
  "block_hashes": [10001],
  "parent_block_hash": 9999,
  "token_ids": [101, 102, 103],
  "block_size": 64,
  "medium": "EXTERNAL",
  "lora_name": "",
  "event_id": "mooncake:tenant-a:sglang-hicache:10001:stored:1",
  "source": "mooncake",
  "tenant_id": "tenant-a",
  "group_id": "sglang-hicache:10001",
  "model_name": "llama",
  "dp_rank": 0
}
```

Dynamo 当前核心依赖字段是：

- `type`
- `block_hashes`
- `token_ids`
- `block_size`
- `parent_block_hash`
- `medium`

其他字段可以作为扩展字段保留，Dynamo 不认识时可以忽略。

### 7.2 BlockRemoved

建议格式：

```json
{
  "type": "BlockRemoved",
  "block_hashes": [10001],
  "medium": "EXTERNAL",
  "event_id": "mooncake:tenant-a:sglang-hicache:10001:removed:1",
  "source": "mooncake",
  "tenant_id": "tenant-a",
  "group_id": "sglang-hicache:10001"
}
```

删除事件不需要再携带 `token_ids`，因为 Dynamo 根据 `block_hashes` 移除即可。

## 8. SGLang 需要透传的信息

Mooncake 自己无法推导以下信息，必须由 SGLang 提供：

| 字段 | 是否必须 | 来源 | 说明 |
| --- | --- | --- | --- |
| `group_id` | 必须 | SGLang Mooncake backend | 逻辑 block 和物理 object 的连接键 |
| `block_hash` | 必须 | SGLang prefix cache node | Dynamo 用来识别逻辑 block |
| `parent_block_hash` | 建议必须 | SGLang prefix cache node parent | Dynamo 构造 prefix tree 或验证父子关系 |
| `token_ids` | 必须 | SGLang prefix cache node | Dynamo 计算和校验 token hash 的基础 |
| `block_size` | 必须 | SGLang page size/cache block size | Dynamo routing 必需 |
| `expected_components` | 强烈建议必须 | SGLang Mooncake backend object expansion | Mooncake 判断 logical block 是否完整 |
| `model_name` | 建议 | SGLang runtime config | 多模型场景隔离 |
| `dp_rank` | 视部署而定 | SGLang rank/runtime | 多 DP 场景 routing 需要 |
| `lora_name` | 视部署而定 | SGLang request/cache metadata | LoRA cache 隔离 |
| `additional_salt` | 视部署而定 | SGLang hash namespace | 和 SGLang hash 语义保持一致 |

Mooncake 可以自己获得或派生的信息：

| 字段 | Mooncake 来源 | 说明 |
| --- | --- | --- |
| `tenant_id` | Mooncake tenant/session | 不需要 SGLang 重复传，除非当前路径没有 tenant |
| `object_key` | Put/BatchPut keys | 物理对象名 |
| `medium` | Mooncake 存储层/副本类型 | 第一版可统一映射为 `EXTERNAL` |
| object 写入完成状态 | Mooncake master | 用于判断 stored event |
| object 删除/驱逐状态 | Mooncake master | 用于判断 removed event |

## 9. 开发拆分建议

建议 Mooncake 侧先做一个 PR，但内部拆成两个相对独立的提交或模块。

### 9.1 第一部分：接口和 manifest

目标：

- 在 `ReplicateConfig` 中增加 `kv_event_metadata`。
- 给 C++ RPC / serialization / pybind 增加字段支持。
- 在 master 侧保存 `KvGroupManifest`。
- 在 `PutStart` / `BatchPutStart` / `Upsert` / `BatchUpsert` 中校验 metadata。
- 把现有 group member 和新的 expected components 关联起来。
- 暂不真正对外发 event，先保证状态模型正确。

验收标准：

- 不传 `kv_event_metadata` 时现有 Mooncake 行为完全不变。
- 传入 metadata 后，master 可以查到 group manifest。
- batch 中多个 object 共用一个 group 时，只生成一个 manifest。
- metadata 和 group_ids 不匹配时返回清晰错误。
- 同一个 group 重试写入相同 metadata 可以幂等处理。
- 同一个 group 写入冲突 metadata 会被拒绝。

### 9.2 第二部分：event publisher

目标：

- 增加 Dynamo 兼容 event encoder。
- 增加 event publisher 配置。
- 在 group 从 incomplete 变为 complete 时发布 `BlockStored`。
- 在 group 从 complete 变为 incomplete 时发布 `BlockRemoved`。
- 增加去重逻辑，避免同一个 generation 重复发布。

验收标准：

- MHA K/V 两个 object 都写完后，只发布一个 `BlockStored`。
- 只写完 K 或只写完 V 时不发布。
- 删除 K 或 V 任意一个后，只发布一个 `BlockRemoved`。
- 重复 `PutEnd` 或重试不会重复发布 event。
- event 字段满足 Dynamo 文档要求。

## 10. SGLang 后续改造方向

Mooncake 侧接口完成后，SGLang 需要做的事情是：

1. 保留现有 `extra_config` 作为功能开关，例如 `enable_kv_event_metadata`。
2. 在构造 Mooncake object keys 时，同步构造 `group_id`。
3. 每个 logical page/block 生成一份 `KvBlockEventMetadata`。
4. 把该 logical block 展开的所有 Mooncake object keys 填入 `expected_components`。
5. 在调用 Mooncake `BatchPut` 时同时传入：
   - object keys
   - values
   - per-object `group_ids`
   - per-group `kv_event_metadata`
6. 关闭或避免 SGLang 自己对同一份 Mooncake external cache 重复发布 Dynamo event。

SGLang 需要改动的核心不是“自己发布 Mooncake event”，而是“把它已经知道的 prefix cache 语义交给 Mooncake”。

## 11. 测试计划

Mooncake 侧建议增加以下测试。

### 11.1 接口测试

- `ReplicateConfig` 默认构造不带 `kv_event_metadata`，现有路径不受影响。
- `kv_event_metadata` 可以通过 C++ client 构造并传到 master。
- Python binding 可以设置 `kv_event_metadata` 或临时 JSON metadata。
- `group_ids` 长度和 `keys` 长度不一致时仍然报错。
- metadata 中的 `group_id` 不在 `group_ids` 中时报错。

### 11.2 Manifest 测试

- 一个 group 两个 expected object，写入一个 object 不完整。
- 两个 expected object 都写入后完整。
- 重复写入相同 metadata 不产生冲突。
- 相同 group 写入不同 `block_hash` 或 `token_ids` 报错。
- `expected_components` 中缺失 object 时不发布 stored event。

### 11.3 Event 测试

- 两个物理 object 组成一个 logical block，只发一个 `BlockStored`。
- 删除任意一个 expected object，只发一个 `BlockRemoved`。
- 重复删除不重复发布。
- event 中 `token_ids`、`block_size`、`parent_block_hash` 与输入 metadata 一致。
- medium 映射符合配置。

### 11.4 回归测试

- 未启用 KV event metadata 时，普通 Put/BatchPut/Remove 行为不变。
- 已有 group semantics 的 lease refresh 和 eviction 行为不变。
- 不带 group 的 object 不进入 KV event 流程。

## 12. 风险和开放问题

### 12.1 token_ids 存储成本

`token_ids` 对 Dynamo `BlockStored` 是必要字段。Mooncake 是否要长期保存它，需要结合 event replay 需求决定。

第一版建议保存到 group manifest 中，保证 event 发布和必要重放可用。后续如果有独立 event log，可以考虑发布后只保留 block hash 和删除所需状态。

### 12.2 medium 映射

Dynamo 的 `StorageMedium` 包括 `GPU`、`CPU_PINNED`、`DISK`、`EXTERNAL`。

Mooncake 作为外部存储系统，第一版建议统一发布为 `EXTERNAL`。如果后续 Dynamo routing 想区分 Mooncake 的内存层和磁盘层，再扩展 per-tier medium。

### 12.3 多副本语义

Mooncake 可能有多个 replica。Dynamo 关心的是 logical block 是否可用于 routing，不一定关心每个 replica。

第一版建议定义为：只要满足 Mooncake 当前读可用条件，就发布 `BlockStored`。如果副本数量影响读可用性，应复用 Mooncake 现有 object 可用判断，不把副本细节暴露给 Dynamo。

### 12.4 group generation

如果未来同一个 `group_id` 会被复用，需要加入 `generation`。

第一版建议直接禁止同 group 不同 metadata，避免复用引入歧义。

### 12.5 多模态 metadata

Dynamo 未来可能需要 `block_mm_infos`。Mooncake 第一版可以先不支持，但接口应该允许未来扩展，例如：

```cpp
std::string extra_metadata_json;
```

或者后续新增结构化字段。

## 13. 推荐落地路径

推荐按下面顺序推进：

1. Mooncake 基于最新 `main` 新建分支。
2. 参考已有 KV event PR 中的 publisher 和 event encoder，但不要直接沿用 object-level event 语义。
3. 在 `ReplicateConfig` 中增加 group 级 `kv_event_metadata`。
4. 扩展 master group 状态，增加 manifest 和 completeness 判断。
5. 用 mock publisher 写完 Mooncake 侧单元测试。
6. 接入 Dynamo-compatible publisher。
7. Mooncake PR 合并或稳定后，再改 SGLang 透传 metadata。
8. 最后做 Mooncake + SGLang + Dynamo 的端到端验证。

## 14. 最终建议

从架构上看，“Mooncake 发布 KV event”是合理的，但前提是 Mooncake 发布的是语义化 logical block event，而不是 object event。

最小且可维护的接口方案是：

- 继续使用现有写入接口。
- 继续使用 `group_ids` 连接多个物理 object。
- 新增 `ReplicateConfig.kv_event_metadata` 传入 logical block 语义。
- 使用 `extra_config` 控制是否启用该能力和默认配置。
- Mooncake 维护 group manifest，并在 logical block 完整性变化时发布 Dynamo event。

这样 Mooncake 仍然是存储生命周期的唯一事件源，SGLang 只承担语义透传，Dynamo 收到的则是它真正需要的 prefix cache block 事件。
