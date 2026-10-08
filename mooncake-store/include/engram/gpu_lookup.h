#pragma once

#include <cuda_runtime_api.h>
#include <cstdint>
#include <memory>

namespace mooncake::engram {

// Separate cache lines for the GPU producer and CPU completion writer.
struct GpuLookupMailbox {
    alignas(64) unsigned long long submitted = 0;
    unsigned int blocks = 0;
    size_t tokens = 0;
    alignas(64) unsigned long long completed = 0;
    int error = 0;
};

cudaError_t launch_engram_submit(const void* ids, bool int64, size_t tokens,
                                 size_t heads, size_t token_stride,
                                 size_t head_stride, const int64_t* offsets,
                                 int64_t* staged_ids, GpuLookupMailbox* mailbox,
                                 unsigned long long* sequence,
                                 cudaStream_t stream);
cudaError_t launch_engram_wait(GpuLookupMailbox* mailbox, cudaStream_t stream);

class GpuLookup {
   public:
    struct State;
    explicit GpuLookup(std::shared_ptr<State> state);
    void wait(uintptr_t stream);
    void check() const;

   private:
    friend class GpuLookupContext;
    std::shared_ptr<State> state_;
};

}  // namespace mooncake::engram
