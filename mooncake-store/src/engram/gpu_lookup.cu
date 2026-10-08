#include "engram/gpu_lookup.h"

#include <cstdio>

namespace mooncake::engram {
namespace {
__device__ unsigned long long load_acquire(const unsigned long long* ptr) {
    unsigned long long value;
    asm volatile("ld.acquire.sys.global.u64 %0, [%1];"
                 : "=l"(value)
                 : "l"(ptr)
                 : "memory");
    return value;
}
template <typename Id>
__global__ void submit(const Id* ids, size_t tokens, size_t heads,
                       size_t token_stride, size_t head_stride,
                       const int64_t* offsets, int64_t* staged,
                       GpuLookupMailbox* box, unsigned long long* sequence) {
    for (size_t i = blockIdx.x * blockDim.x + threadIdx.x; i < tokens * heads;
         i += gridDim.x * blockDim.x) {
        const size_t head = i / tokens, token = i % tokens;
        const int64_t id = ids[token * token_stride + head * head_stride];
        // Preserve padding; negative local IDs other than -1 must fail closed.
        staged[i] =
            id == -1 ? -1 : (id < offsets[head] ? -2 : id - offsets[head]);
    }
    __threadfence_system();
    __syncthreads();
    if (threadIdx.x == 0 &&
        atomicAdd_system(&box->blocks, 1u) == gridDim.x - 1) {
        box->blocks = 0;
        box->tokens = tokens;
        box->error = 0;
        __threadfence_system();
        const auto ticket = atomicAdd_system(sequence, 1ull) + 1;
        asm volatile(
            "st.release.sys.global.u64 [%0], %1;" ::"l"(&box->submitted),
            "l"(ticket)
            : "memory");
    }
}

__global__ void wait_result(GpuLookupMailbox* box) {
    // One sleeping thread, rather than holding up the host launch thread.
    const auto ticket = load_acquire(&box->submitted);
    unsigned long long start;
    asm volatile("mov.u64 %0, %%globaltimer;" : "=l"(start));
    while (load_acquire(&box->completed) != ticket) {
        __nanosleep(100);
        unsigned long long now;
        asm volatile("mov.u64 %0, %%globaltimer;" : "=l"(now));
        if (now - start > 60000000000ull) {
            printf("Mooncake Engram lookup timed out (60s)\n");
            asm volatile("trap;");
        }
    }
    __threadfence_system();
    if (box->error) {
        printf("Mooncake Engram lookup failed: %d\n", box->error);
        asm volatile("trap;");
    }
}
}  // namespace

cudaError_t launch_engram_submit(const void* ids, bool int64, size_t tokens,
                                 size_t heads, size_t token_stride,
                                 size_t head_stride, const int64_t* offsets,
                                 int64_t* staged_ids, GpuLookupMailbox* mailbox,
                                 unsigned long long* sequence,
                                 cudaStream_t stream) {
    const int blocks = static_cast<int>(
        min(size_t(32), max(size_t(1), (tokens * heads + 255) / 256)));
    if (int64)
        submit<<<blocks, 256, 0, stream>>>(
            static_cast<const int64_t*>(ids), tokens, heads, token_stride,
            head_stride, offsets, staged_ids, mailbox, sequence);
    else
        submit<<<blocks, 256, 0, stream>>>(
            static_cast<const int32_t*>(ids), tokens, heads, token_stride,
            head_stride, offsets, staged_ids, mailbox, sequence);
    return cudaGetLastError();
}

cudaError_t launch_engram_wait(GpuLookupMailbox* mailbox, cudaStream_t stream) {
    wait_result<<<1, 1, 0, stream>>>(mailbox);
    return cudaGetLastError();
}
}  // namespace mooncake::engram
