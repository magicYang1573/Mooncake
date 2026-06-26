# Mooncake 与 SGLang 交互层次和接口抽象

## 结论摘要

Mooncake 与 SGLang 的主要交互不是 KV event 接口，而是 SGLang HiCache 的 L3 storage backend 接口。SGLang 负责 token、radix tree、prefix page、hash、GPU/host KV pool 等推理引擎语义；Mooncake 负责分布式对象存储、对象副本、内存/SSD 池、RDMA/zero-copy 数据传输和驱逐。

核心边界可以概括为：

```text
SGLang:   logical KV page / token prefix / radix hash / host buffer index
Mooncake: object key / object bytes / registered buffer pointer / replica / medium
```

也就是说，SGLang 把每个可复用的 KV page 映射成 Mooncake object；Mooncake 并不知道这个 object 背后对应哪些 token、哪个 parent block、哪个 radix node，除非 SGLang 额外把这些语义作为 metadata 或事件传进去。

## 主要源码位置

SGLang 侧：

- `sglang/python/sglang/srt/mem_cache/hicache_storage.py`
- `sglang/python/sglang/srt/mem_cache/storage/backend_factory.py`
- `sglang/python/sglang/srt/mem_cache/storage/mooncake_store/mooncake_store.py`
- `sglang/python/sglang/srt/managers/cache_controller.py`
- `sglang/python/sglang/srt/mem_cache/hiradix_cache.py`
- `sglang/python/sglang/srt/mem_cache/events.py`
- `sglang/python/sglang/srt/disaggregation/kv_events.py`
- `sglang/python/sglang/srt/distributed/device_communicators/mooncake_transfer_engine.py`
- `sglang/python/sglang/srt/layers/moe/token_dispatcher/mooncake.py`

Mooncake 侧：

- `Mooncake/mooncake-integration/store/store_py.cpp`
- `Mooncake/mooncake-store/include/pyclient.h`
- `Mooncake/mooncake-store/src/real_client.cpp`
- `Mooncake/mooncake-store/src/dummy_client.cpp`
- `Mooncake/mooncake-store/include/store_c.h`
- `Mooncake/mooncake-transfer-engine/include/transfer_engine.h`

已有背景文档：

- `kv_event_alignment_report.md`
- `mooncake_kv_event_rules.md`
- `dynamo_kv_event_rules.md`

## 总体交互链路

主链路是 HiCache L3 storage backend：

```text
Request tokens
  -> SGLang Radix / HiRadixCache
  -> CacheController
  -> HiCacheStorage 抽象接口
  -> MooncakeStore 适配器
  -> mooncake.store.MooncakeDistributedStore
  -> Mooncake PyClient
  -> RealClient 或 DummyClient
  -> Mooncake master / store service / TransferEngine
```

分层含义：

| 层次 | 组件 | 负责内容 | 是否理解 token/radix 语义 |
|---|---|---|---|
| 请求与 prefix 层 | `RadixCache` / `HiRadixCache` | token prefix、radix node、page hash、prefix 命中 | 是 |
| L1/L2/L3 调度层 | `CacheController` | GPU KV、host KV、storage backend 之间的搬运调度 | 部分理解 page/token 数量 |
| Storage 抽象层 | `HiCacheStorage` | `batch_exists` / `batch_get` / `batch_set` 通用接口 | 不要求理解 |
| Mooncake 适配层 | `MooncakeStore` | 把 page hash + host index 转成 object key + pointer + size | 只做命名和布局转换 |
| Mooncake 客户端层 | `MooncakeDistributedStore` / `PyClient` | object put/get/exist/remove、buffer registration | 否 |
| Mooncake 存储层 | master / store service / real client | replica、分配、驱逐、RDMA、SSD offload | 否 |

## SGLang 侧的抽象

### HiCacheStorage

`HiCacheStorage` 是 SGLang 对所有 L3 storage backend 的统一抽象。Mooncake、NIXL、HF3FS、file backend 等都挂在这个接口下。

核心接口：

```python
class HiCacheStorage:
    def register_mem_pool_host(self, mem_pool_host): ...
    def register_mem_host_pool_v2(self, host_pool, host_pool_name): ...

    def batch_exists(self, keys, extra_info=None) -> int: ...
    def batch_get_v1(self, keys, host_indices, extra_info=None) -> list[bool]: ...
    def batch_set_v1(self, keys, host_indices, extra_info=None) -> list[bool]: ...

    def batch_exists_v2(self, keys, pool_transfers=None, extra_info=None): ...
    def batch_get_v2(self, transfers, extra_info=None) -> dict: ...
    def batch_set_v2(self, transfers, extra_info=None) -> dict: ...

    def get(self, key, target_location=None, target_sizes=None): ...
    def set(self, key, value=None, target_location=None, target_sizes=None): ...
    def clear(self): ...
```

其中 Mooncake 主要走 zero-copy 的 v1/v2 path：

- v1：普通 KV page 读写。
- v2：多 pool / sidecar pool 读写，例如 draft、Mamba、SWA、DSA、DeepSeek V4 side pools。

### StorageBackendFactory

`StorageBackendFactory` 把 `"mooncake"` 注册到：

```text
sglang.srt.mem_cache.storage.mooncake_store.mooncake_store.MooncakeStore
```

所以命令行启用：

```bash
python -m sglang.launch_server \
  --enable-hierarchical-cache \
  --hicache-storage-backend mooncake \
  --model-path ...
```

本质上就是创建一个 `MooncakeStore(storage_config, mem_pool_host)`。

### CacheController

`CacheController` 是 L1/L2/L3 数据搬运的调度者：

- GPU -> host：把 device KV page backup 到 host KV pool。
- host -> storage：把 host KV page 写入 L3 backend。
- storage -> host：根据 prefix hash 查询 L3 命中并预取。
- host -> GPU：把 host KV page load back 到 device KV pool。

当 storage backend 是 Mooncake 时，`CacheController` 会选择 zero-copy page get/set：

```python
self.page_get_func = self._page_get_zero_copy
self.page_set_func = self._page_set_zero_copy
```

对应调用：

```python
self.storage_backend.batch_get_v1(hash_values, host_indices, extra_info)
self.storage_backend.batch_set_v1(hash_values, host_indices, extra_info)
```

这里传给 Mooncake backend 的不是 tensor 内容，而是：

- `hash_values`: SGLang 计算出的逻辑 KV page hash。
- `host_indices`: host KV pool 中的 page index。

`MooncakeStore` 再根据 host pool metadata 把它转成 pointer 和 size。

## MooncakeStore 适配层

`MooncakeStore` 同时继承：

```python
class MooncakeStore(HiCacheStorage, MooncakeBaseStore):
    ...
```

它的职责不是实现缓存算法，而是做三类适配：

1. 加载 Mooncake 配置并初始化 `MooncakeDistributedStore`。
2. 注册 SGLang host KV pool 的底层 tensor buffer。
3. 把 SGLang 的逻辑 page key 转成 Mooncake object key，再用 zero-copy API 做 batch get/set。

### 初始化接口

配置来源优先级：

1. `--hicache-storage-backend-extra-config`
2. `SGLANG_HICACHE_MOONCAKE_CONFIG_PATH`
3. Mooncake 相关环境变量

典型配置项：

| 配置 | 含义 |
|---|---|
| `master_server_address` | Mooncake master 地址 |
| `metadata_server` | Mooncake metadata service 地址，或 `P2PHANDSHAKE` |
| `global_segment_size` | 本进程贡献给 Mooncake 全局池的内存大小 |
| `protocol` | `rdma` / `tcp` / `efa` 等 |
| `device_name` | RDMA device |
| `standalone_storage` | 是否使用 dummy client 模式 |
| `client_server_address` | dummy client 连接本地 real client 的地址 |
| `enable_ssd_offload` | 是否启用 Mooncake SSD offload |
| `ssd_offload_path` | SSD spill 目录 |
| `extra_backend_tag` | 给 object key 加前缀，隔离不同 backend/tenant |
| `enable_group_semantics` | 是否给同一 logical page 的多个 physical object 设置 group id |

full client 模式调用：

```python
self.store.setup(
    client_hostname,
    metadata_server,
    per_tp_global_segment_size,
    DEFAULT_LOCAL_BUFFER_SIZE,
    protocol,
    device_name,
    master_server_address,
    transfer_engine,
    enable_ssd_offload=True/False,
    ssd_offload_path=...,
)
```

dummy client 模式调用：

```python
self.store.setup_dummy(
    required_bytes,
    DEFAULT_LOCAL_BUFFER_SIZE,
    client_server_address,
)
```

### Buffer registration

Mooncake zero-copy 的前提是 SGLang 把 host KV pool 的 tensor buffer 注册给 Mooncake：

```python
ptr = tensor.data_ptr()
size = tensor.numel() * tensor.element_size()
self.store.register_buffer(ptr, size)
```

普通 KV pool 通过：

```python
register_mem_pool_host(mem_pool_host)
```

sidecar / draft / hybrid pool 通过：

```python
register_mem_host_pool_v2(host_pool, host_pool_name)
```

注册之后，后续读写只传指针地址和 size，不传 Python tensor 内容。

## Mooncake 对外 API

SGLang 使用的是 Mooncake Python binding：

```python
from mooncake.store import MooncakeDistributedStore
```

关键对象：

```python
store = MooncakeDistributedStore()
```

核心 API：

```python
store.setup(...)
store.setup_dummy(...)

store.register_buffer(ptr, size)
store.unregister_buffer(ptr)

store.batch_is_exist(keys)

store.batch_put_from(keys, buffer_ptrs, sizes, config=None)
store.batch_get_into(keys, buffer_ptrs, sizes)

store.batch_put_from_multi_buffers(keys, all_buffer_ptrs, all_sizes, config=None)
store.batch_get_into_multi_buffers(keys, all_buffer_ptrs, all_sizes)

store.remove_all()
```

这些接口在 C++ binding 中映射到底层 `PyClient`：

```cpp
class PyClient {
 public:
  virtual int setup_real(...) = 0;
  virtual int setup_dummy(...) = 0;

  virtual int register_buffer(void* buffer, size_t size) = 0;
  virtual int unregister_buffer(void* buffer) = 0;

  virtual std::vector<int> batch_put_from(
      const std::vector<std::string>& keys,
      const std::vector<void*>& buffers,
      const std::vector<size_t>& sizes,
      const ReplicateConfig& config) = 0;

  virtual std::vector<int64_t> batch_get_into(
      const std::vector<std::string>& keys,
      const std::vector<void*>& buffers,
      const std::vector<size_t>& sizes) = 0;
};
```

返回值语义：

- `batch_put_from`: 每个 object 成功返回 `0`，失败返回负值。
- `batch_get_into`: 每个 object 成功返回读取字节数，失败返回负值。
- `batch_is_exist`: 每个 key 存在返回 `1`，不存在或失败返回其他值。

SGLang 会把 physical object 的结果重新聚合成 logical page 的布尔结果。

## Logical page 到 Mooncake object 的命名

这是 Mooncake/SGLang 抽象边界中最关键、也最容易影响 KV event 对齐的部分。

SGLang 内部的 `hash_values` 是 logical KV page hash。写入 Mooncake 时，`MooncakeStore` 会按模型结构和并行配置扩展成一个或多个 physical object key。

### MHA

普通 MHA KV page 通常拆成 K 和 V 两个 object：

```text
{hash}_{tp_rank}_k
{hash}_{tp_rank}_v
```

如果启用 pipeline parallel：

```text
{hash}_{tp_rank}_{pp_rank}_k
{hash}_{tp_rank}_{pp_rank}_v
```

### MLA / rank-replicated KV

MLA 类模型通常是单 object：

```text
{hash}_{pp_rank}_k
```

没有 PP 时 suffix 可能为空或更短。

### Split heads

如果配置了 `tp_lcm_size` 并启用 split-head，单个 logical page 会展开到多个 target rank：

```text
{hash}_{rank0}_k
{hash}_{rank0}_v
{hash}_{rank1}_k
{hash}_{rank1}_v
...
```

### Draft / sidecar pool

v2 path 使用 `PoolTransfer` 描述不同 pool。示例：

```python
PoolTransfer(
    name=PoolName.DRAFT,
    host_indices=host_indices,
    keys=hash_values,
)
```

`MooncakeStore` 会根据 pool 类型扩展 suffix，例如：

- Draft MHA: `{hash}_{rank}_draft_k`, `{hash}_{rank}_draft_v`
- Draft MLA: `{hash}_{pp_rank}_draft_k`
- Mamba: `{hash}_{rank}_temporal`, `{hash}_{rank}_conv_{i}`
- SWA: `{hash}_{rank}_swa_k`, `{hash}_{rank}_swa_v`
- DSA / DeepSeek V4 side pools: `{hash}_{pp_rank}_{pool_name}`

### extra_backend_tag

如果设置了 `extra_backend_tag`：

```text
{extra_backend_tag}_{object_key}
```

这可以用于多 tenant / 多 backend 隔离，但也意味着 Mooncake master 看到的 object key 不是裸 hash。

### group semantics

如果开启 `enable_group_semantics`，SGLang 会给同一个 logical page 派生出的多个 physical object 设置同一个 group id：

```text
sglang-hicache:{logical_key}
```

其意图是让 Mooncake 在 metadata routing、lease refresh、eviction 时知道这些对象属于同一个逻辑 page，例如 MHA 的 K/V object 应该作为一组看待。

## 数据流

### 写入 L3

```text
1. SGLang radix node 形成可 backup 的 KV page。
2. CacheController.write() 把 GPU KV page copy 到 host KV pool。
3. HiRadixCache.write_backup_storage() 把 host_indices、token_ids、hash_value 放入 backup queue。
4. backup thread 调 CacheController._page_backup()。
5. MooncakeStore.batch_set_v1(hash_values, host_indices)。
6. MooncakeStore 根据 host_indices 得到 buffer_ptrs / sizes。
7. MooncakeStore 把 logical hash 扩展成 physical object keys。
8. MooncakeDistributedStore.batch_is_exist(keys) 先查存在性。
9. 对不存在的 object 调 batch_put_from(keys, ptrs, sizes, config)。
10. Mooncake master 分配 replica，TransferEngine/RealClient 完成数据写入。
```

### 读取 L3

```text
1. 新请求到来，SGLang 根据 token prefix 计算一串 page hash。
2. CacheController._storage_hit_query() 调 storage_backend.batch_exists(batch_hashes)。
3. MooncakeStore 把 logical hash 扩展成 physical object keys 并查 batch_is_exist。
4. 只有一个 logical page 的所有 required physical objects 都存在，才算该 page 命中。
5. prefetch thread 为命中的 pages 分配 host_indices。
6. MooncakeStore.batch_get_v1(hash_values, host_indices)。
7. MooncakeDistributedStore.batch_get_into(keys, ptrs, sizes) 直接把 object 读入 host KV pool。
8. SGLang 再把 host KV page load 回 GPU。
```

## 部署模式

### Full client 模式

SGLang 进程本身作为 Mooncake client：

```text
SGLang process
  - MooncakeDistributedStore
  - TransferEngine
  - registered host KV buffers
  - optional contributed global segment

Mooncake master
Mooncake metadata service, optional
Mooncake store service, optional
```

优点：

- 部署简单。
- SGLang 可以直接参与 Mooncake 全局内存池。
- 数据路径更直接。

风险：

- RDMA、内存注册、store client 生命周期和 SGLang 进程耦合。
- SGLang 进程退出时，其贡献的内存和对象也会消失。

### Dummy client 模式

SGLang 只作为轻量 dummy client，连接本机 Mooncake real client/store service：

```text
SGLang process
  - DummyClient
  - connects to local mooncake_client

local mooncake_client / real client
  - owns global segment
  - owns RDMA resources
  - persists beyond SGLang process

Mooncake master
```

优点：

- RDMA 和大内存管理从 SGLang 进程剥离。
- SGLang 重启时 cache 有机会保留。
- 生产稳定性更好。

代价：

- 多一个本地服务。
- 需要保证 dummy client 可见的 host buffers 被 real client 正确映射。

## 与 Mooncake TransferEngine 的区别

SGLang 还有一条 Mooncake 交互线是直接 transfer engine，不是 HiCache object storage。

对应封装：

```python
MooncakeTransferEngine
```

主要接口：

```python
register(ptr, length)
deregister(ptr)
batch_register(ptrs, lengths)
batch_deregister(ptrs)
transfer_sync(session_id, src_buffer, peer_buffer_address, length)
batch_transfer_sync(session_id, src_buffers, peer_buffer_addresses, lengths)
```

这条线用于点对点 tensor/KV 搬运场景，例如 disaggregation 或多模态 runtime 中的直接传输。它的抽象是：

```text
session id + local address + remote address + length
```

而 HiCache MooncakeStore 的抽象是：

```text
object key + registered buffer pointer + size
```

二者共用 Mooncake 的 transfer 能力，但语义层不一样。

## 与 Mooncake EP 的区别

SGLang MoE 里还可以使用 Mooncake EP dispatcher：

```python
MooncakeEPDispatcher
```

它封装的是：

```python
mooncake.mooncake_ep_buffer.Buffer.dispatch(...)
mooncake.mooncake_ep_buffer.Buffer.combine(...)
```

这条线服务于 expert parallel 的 token dispatch/combine，不参与 HiCache L3 object storage，也不参与 KV event。

## 与 KV event 的关系

这一点需要单独强调：SGLang 的 KV event 和 Mooncake master 的 KV/object event 在抽象层次上不同。

### SGLang KV event

SGLang 在 radix/cache 层能产生引擎语义完整的事件：

```python
BlockStored(
    block_hashes=[...],
    parent_block_hash=...,
    token_ids=[...],
    block_size=...,
    lora_id=None,
    medium="GPU" / "CPU_PINNED" / "DISK" / "EXTERNAL",
)

BlockRemoved(
    block_hashes=[...],
    medium=...,
)
```

字段来源：

- `token_ids`: radix node 的 token 内容。
- `block_size`: SGLang page size / 当前 page token 数。
- `parent_block_hash`: parent radix node 的最后一个 page hash。
- `block_hashes`: SGLang 计算出的 page hash。
- `medium`: GPU/CPU/storage 层级。

所以 SGLang event 是 token/radix/block 语义。

### Mooncake master event

Mooncake master 自然能看到的是 object lifecycle：

```text
object key stored
object key removed
replica in memory/disk
tenant/backend
timestamp
```

Mooncake master 默认不知道：

- token ids
- block size
- parent block hash
- LoRA / multimodal context
- SGLang radix tree
- 一个 logical page 与多个 physical object 的完整对应关系
- DP rank / model semantic owner

因此 Mooncake master event 更像 storage object event，而不是 SGLang/Dynamo 当前需要的 canonical KV block event。

## 对 Dynamo KV-aware routing 的影响

如果下游是 Dynamo KV-aware routing，需要区分两种思路。

### 方案 A：SGLang 继续发布 canonical KV event，Mooncake 只补 lower-tier availability

推荐。

SGLang 负责：

- 发布带 `token_ids` / `block_size` / `parent_block_hash` 的 canonical event。
- 让 Dynamo 建立 token prefix 到 block hash 的索引。

Mooncake 负责：

- 发布 object 存在/删除事件。
- 表达某个 sequence hash 或 object key 在 CPU/DISK/EXTERNAL tier 可用。

Dynamo 侧需要一个 Mooncake adapter：

- 识别 Mooncake `event_type=stored/removed`。
- 读取 `object_key` / `seq_hashes` / `backend_id` / `tenant_id` / `medium`。
- 能把 Mooncake physical object key 归并回 SGLang logical page hash。
- 能处理 MHA K/V、split heads、sidecar pools、group id。
- 不强制要求 Mooncake stored event 携带 `token_ids`。

### 方案 B：让 Mooncake 直接模拟 SGLang KV event

不推荐作为第一选择。

如果 Mooncake master 要直接发 Dynamo 当前 ZMQ relay 可消费的 `BlockStored`，它需要拿到：

- `token_ids`
- `block_size`
- `parent_block_hash`
- SGLang canonical block hash
- model / DP / LoRA / salt 等维度

这些信息天然属于 SGLang 引擎层，不属于 Mooncake storage master 层。把它们灌进 Mooncake 会让存储系统强耦合推理引擎语义。

## 抽象边界总结

### SGLang 对 Mooncake 的期待

SGLang 期待 Mooncake 是一个高性能、分布式、可 zero-copy 的 object storage backend：

```text
exists(keys) -> object availability
put_from(keys, registered_ptrs, sizes) -> store objects
get_into(keys, registered_ptrs, sizes) -> load objects
remove / clear -> clean objects
```

SGLang 自己负责：

- 何时写入 L3。
- 何时预取。
- 哪些 prefix/page/hash 是可复用的。
- 一个 logical page 应拆成哪些 physical objects。
- 读写成功后如何更新 radix node 状态。

### Mooncake 对 SGLang 的期待

Mooncake 期待调用方提供：

- object key
- buffer pointer
- buffer size
- 已注册的可访问内存
- 可选 `ReplicateConfig`
- 可选 `group_ids`

Mooncake 自己负责：

- 分配 replica。
- 维护 metadata。
- 按 medium / segment / policy 驱逐。
- RDMA/TCP/EFA 数据传输。
- SSD offload。
- real/dummy client 生命周期。

## 一句话总结

SGLang 与 Mooncake 的主交互是“KV page 到分布式 object store”的适配：SGLang 保留 token/radix/cache 语义，Mooncake 提供 object-level zero-copy 存储和传输。KV event 如果要对接 Dynamo，最好让 SGLang 继续作为 token 语义的 source of truth，让 Mooncake event 作为 storage tier availability 的补充，而不是要求 Mooncake master 直接伪装成 SGLang 引擎事件源。
