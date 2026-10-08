#include "engram/gpu_lookup.h"
#include "engram/engram_store.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace mooncake::engram {
namespace {
void cuda_check(cudaError_t rc) {
    if (rc != cudaSuccess) throw std::runtime_error(cudaGetErrorString(rc));
}

template <typename T>
struct Mapped {
    T* host = nullptr;
    T* device = nullptr;
    explicit Mapped(size_t count = 1) {
        cuda_check(cudaHostAlloc(reinterpret_cast<void**>(&host),
                                 count * sizeof(T), cudaHostAllocMapped));
        auto rc = cudaHostGetDevicePointer(reinterpret_cast<void**>(&device),
                                           host, 0);
        if (rc != cudaSuccess) {
            cudaFreeHost(host);
            cuda_check(rc);
        }
        std::fill_n(host, count, T{});
    }
    ~Mapped() { cudaFreeHost(host); }
    Mapped(const Mapped&) = delete;
    Mapped& operator=(const Mapped&) = delete;
};
}  // namespace

struct GpuLookup::State {
    EngramStore* store;
    int device;
    std::vector<int> tables;
    std::vector<int64_t> offsets;
    void* output;
    size_t bytes;
    cudaEvent_t submitted_event = nullptr;
    cudaStream_t submit_stream = nullptr;
    Mapped<GpuLookupMailbox> mailbox;
    Mapped<int64_t> staged, gpu_offsets;
    State(EngramStore* store_, int device_, const std::vector<int>& tables_,
          const std::vector<int64_t>& offsets_, void* output_, size_t bytes_)
        : store(store_),
          device(device_),
          tables(tables_),
          offsets(offsets_),
          output(output_),
          bytes(bytes_),
          staged(std::max<size_t>(
              1, bytes / store->get_row_bytes(tables.front()))),
          gpu_offsets(tables.size()) {
        std::copy(offsets.begin(), offsets.end(), gpu_offsets.host);
        cuda_check(
            cudaEventCreateWithFlags(&submitted_event, cudaEventDisableTiming));
    }
    ~State() { cudaEventDestroy(submitted_event); }
};

namespace {
// One native submission thread per GPU. A GPU-assigned sequence preserves layer
// order across Store handles, eager execution, and CUDA Graph replay.
class Proxy {
   public:
    Mapped<unsigned long long> sequence;
    explicit Proxy(int device) : device_(device) {
        int ordering = 0, options = 0;
        cuda_check(cudaDeviceGetAttribute(
            &ordering, cudaDevAttrGPUDirectRDMAWritesOrdering, device));
        flush_ = ordering < cudaFlushGPUDirectRDMAWritesToOwner;
        if (flush_) {
            cuda_check(cudaDeviceGetAttribute(
                &options, cudaDevAttrGPUDirectRDMAFlushWritesOptions, device));
            if (!(options & cudaFlushGPUDirectRDMAWritesOptionHost))
                throw std::runtime_error(
                    "GPU does not support host GPUDirect visibility flush");
        }
        thread_ = std::thread([this] { run(); });
    }
    ~Proxy() {
        stop_.store(true);
        thread_.join();
    }
    void add(const std::shared_ptr<GpuLookup::State>& state) {
        std::lock_guard<std::mutex> lock(mutex_);
        states_.push_back(state);
    }

   private:
    void run() {
        const auto device_rc = cudaSetDevice(device_);
        unsigned long long next = 1;
        while (!stop_.load(std::memory_order_relaxed)) {
            std::shared_ptr<GpuLookup::State> ready;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                for (auto it = states_.begin(); it != states_.end();) {
                    auto state = it->lock();
                    if (!state) {
                        it = states_.erase(it);
                        continue;
                    }
                    if (__atomic_load_n(&state->mailbox.host->submitted,
                                        __ATOMIC_ACQUIRE) == next) {
                        ready = std::move(state);
                        break;
                    }
                    ++it;
                }
            }
            if (!ready) {
                std::this_thread::yield();
                continue;
            }
            int rc = -1;
            try {
                cuda_check(device_rc);
                rc = ready->store->lookup(ready->tables, ready->staged.host,
                                          ready->mailbox.host->tokens,
                                          ready->output, ready->bytes);
                if (rc == 0 && flush_)
                    cuda_check(cudaDeviceFlushGPUDirectRDMAWrites(
                        cudaFlushGPUDirectRDMAWritesTargetCurrentDevice,
                        cudaFlushGPUDirectRDMAWritesToOwner));
            } catch (const std::exception& e) {
                fprintf(stderr, "Mooncake Engram proxy: %s\n", e.what());
                rc = -1;
            }
            ready->mailbox.host->error = rc;
            __atomic_store_n(&ready->mailbox.host->completed, next++,
                             __ATOMIC_RELEASE);
        }
    }
    int device_;
    bool flush_;
    std::atomic<bool> stop_{false};
    std::mutex mutex_;
    std::vector<std::weak_ptr<GpuLookup::State>> states_;
    std::thread thread_;
};

std::shared_ptr<Proxy> proxy_for(int device) {
    static std::mutex mutex;
    static std::map<int, std::weak_ptr<Proxy>> proxies;
    std::lock_guard<std::mutex> lock(mutex);
    auto proxy = proxies[device].lock();
    if (!proxy) proxies[device] = proxy = std::make_shared<Proxy>(device);
    return proxy;
}
}  // namespace

class GpuLookupContext {
   public:
    explicit GpuLookupContext(int device)
        : device_(device), proxy_(proxy_for(device)) {}
    ~GpuLookupContext() {
        // Caller must finish and destroy graphs before releasing the Store.
        // Drain submit-only requests too, before the underlying client closes.
        int previous = -1;
        cudaGetDevice(&previous);
        cudaSetDevice(device_);
        cudaDeviceSynchronize();
        for (auto& request : requests_) {
            auto* box = request->state_->mailbox.host;
            while (__atomic_load_n(&box->submitted, __ATOMIC_ACQUIRE) !=
                   __atomic_load_n(&box->completed, __ATOMIC_ACQUIRE))
                std::this_thread::yield();
        }
        if (previous >= 0) cudaSetDevice(previous);
    }
    std::shared_ptr<GpuLookup> lookup(EngramStore* store,
                                      const std::vector<int>& tables,
                                      const void* ids, bool int64,
                                      size_t tokens, size_t ts, size_t hs,
                                      const std::vector<int64_t>& offsets,
                                      void* output, size_t bytes,
                                      cudaStream_t stream) {
        std::lock_guard<std::mutex> lock(mutex_);
        int device;
        cuda_check(cudaGetDevice(&device));
        if (device != device_)
            throw std::invalid_argument("Engram Store CUDA device changed");
        std::shared_ptr<GpuLookup> request;
        for (auto& candidate : requests_) {
            const auto& s = *candidate->state_;
            if (s.tables == tables && s.offsets == offsets &&
                s.output == output && s.bytes == bytes) {
                request = candidate;
                break;
            }
        }
        if (!request) {
            cudaStreamCaptureStatus capture;
            cuda_check(cudaStreamIsCapturing(stream, &capture));
            if (capture != cudaStreamCaptureStatusNone)
                throw std::runtime_error(
                    "Warm up Engram lookup with these output buffers before "
                    "CUDA Graph capture");
            auto state = std::make_shared<GpuLookup::State>(
                store, device, tables, offsets, output, bytes);
            request = std::make_shared<GpuLookup>(state);
            requests_.push_back(request);
            proxy_->add(state);
        }
        const auto& s = *request->state_;
        cuda_check(launch_engram_submit(ids, int64, tokens, tables.size(), ts,
                                        hs, s.gpu_offsets.device,
                                        s.staged.device, s.mailbox.device,
                                        proxy_->sequence.device, stream));
        request->state_->submit_stream = stream;
        cudaStreamCaptureStatus capture;
        cuda_check(cudaStreamIsCapturing(stream, &capture));
        cuda_check(cudaEventRecordWithFlags(
            s.submitted_event, stream,
            capture == cudaStreamCaptureStatusNone ? cudaEventRecordDefault
                                                   : cudaEventRecordExternal));
        return request;
    }

   private:
    int device_;
    std::mutex mutex_;
    std::shared_ptr<Proxy> proxy_;
    std::vector<std::shared_ptr<GpuLookup>> requests_;
};

GpuLookup::GpuLookup(std::shared_ptr<State> state) : state_(std::move(state)) {}
void GpuLookup::wait(uintptr_t stream) {
    int device;
    cuda_check(cudaGetDevice(&device));
    if (device != state_->device)
        throw std::invalid_argument("Engram wait CUDA device changed");
    auto consumer = reinterpret_cast<cudaStream_t>(stream);
    if (consumer != state_->submit_stream) {
        cudaStreamCaptureStatus capture;
        cuda_check(cudaStreamIsCapturing(consumer, &capture));
        cuda_check(cudaStreamWaitEvent(consumer, state_->submitted_event,
                                       capture == cudaStreamCaptureStatusNone
                                           ? cudaEventWaitDefault
                                           : cudaEventWaitExternal));
    }
    cuda_check(launch_engram_wait(state_->mailbox.device, consumer));
}
void GpuLookup::check() const {
    const auto* box = state_->mailbox.host;
    if (__atomic_load_n(&box->submitted, __ATOMIC_ACQUIRE) !=
        __atomic_load_n(&box->completed, __ATOMIC_ACQUIRE))
        throw std::runtime_error("Engram lookup is still in flight");
    if (box->error)
        throw std::runtime_error("Engram lookup failed, rc=" +
                                 std::to_string(box->error));
}

std::shared_ptr<GpuLookup> EngramStore::lookup_cuda(
    const std::vector<int>& tables, const void* ids, bool int64, size_t tokens,
    size_t ts, size_t hs, const std::vector<int64_t>& offsets, void* output,
    size_t bytes, uintptr_t stream) {
    if (tables.empty() || offsets.size() != tables.size() || !store_)
        throw std::invalid_argument(
            "CUDA lookup requires Store-backed tables and one offset per head");
    const auto width = get_row_bytes(tables.front());
    if (tokens > INT_MAX ||
        tables.size() > SIZE_MAX / std::max<size_t>(1, tokens) / width ||
        bytes < tables.size() * tokens * width ||
        bytes / width > SIZE_MAX / sizeof(int64_t))
        throw std::invalid_argument("CUDA lookup output size mismatch");
    for (size_t h = 0; h < tables.size(); ++h)
        if (get_num_heads(tables[h]) != 1 ||
            get_row_bytes(tables[h]) != width || offsets[h] < 0)
            throw std::invalid_argument(
                "CUDA lookup requires single-head tables of equal width and "
                "nonnegative offsets");
    int device;
    cuda_check(cudaGetDevice(&device));
    std::call_once(gpu_lookup_init_, [&] {
        cudaStreamCaptureStatus capture;
        cuda_check(cudaStreamIsCapturing(reinterpret_cast<cudaStream_t>(stream),
                                         &capture));
        if (capture != cudaStreamCaptureStatusNone)
            throw std::runtime_error(
                "Warm up Engram lookup before CUDA Graph capture");
        gpu_lookup_ = std::make_shared<GpuLookupContext>(device);
    });
    return gpu_lookup_->lookup(this, tables, ids, int64, tokens, ts, hs,
                               offsets, output, bytes,
                               reinterpret_cast<cudaStream_t>(stream));
}
}  // namespace mooncake::engram
