#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <cstring>
#include <limits>

#include "engram/engram_store.h"
#include "engram/engram_store_config.h"
#include "pyclient.h"
#ifdef MOONCAKE_ENGRAM_CUDA
#include "engram/gpu_lookup.h"
#endif

namespace py = pybind11;
using namespace mooncake;
using namespace mooncake::engram;

namespace {

constexpr char kPyClientCapsuleName[] = "mooncake.PyClient.shared_ptr";
constexpr char kPyClientCapsuleMethod[] = "_get_pyclient_capsule";

std::shared_ptr<PyClient> unwrap_pyclient_capsule(py::object capsule) {
    if (capsule.is_none()) {
        return nullptr;
    }

    if (!PyCapsule_CheckExact(capsule.ptr())) {
        throw std::runtime_error(
            "store wrapper returned a non-capsule PyClient handle");
    }

    py::capsule py_client_capsule(capsule);
    const char* capsule_name = py_client_capsule.name();
    if (capsule_name == nullptr ||
        std::strcmp(capsule_name, kPyClientCapsuleName) != 0) {
        throw std::runtime_error(
            "store wrapper returned an unexpected PyClient capsule type");
    }

    auto* ptr = static_cast<std::shared_ptr<PyClient>*>(
        py_client_capsule.get_pointer());
    if (ptr == nullptr) {
        throw std::runtime_error(
            "store wrapper returned an empty PyClient capsule");
    }
    return *ptr;
}

std::shared_ptr<PyClient> unwrap_store(py::object store_obj) {
    if (store_obj.is_none()) {
        return nullptr;
    }

    try {
        return store_obj.cast<std::shared_ptr<PyClient>>();
    } catch (const py::cast_error&) {
    }

    if (!py::hasattr(store_obj, kPyClientCapsuleMethod)) {
        throw std::runtime_error(
            "EngramStore store parameter must be a PyClient or store wrapper "
            "that "
            "implements _get_pyclient_capsule()");
    }

    try {
        py::object capsule = store_obj.attr(kPyClientCapsuleMethod)();
        return unwrap_pyclient_capsule(capsule);
    } catch (const py::error_already_set& e) {
        throw std::runtime_error(
            "Failed to unwrap store wrapper for EngramStore: " +
            std::string(e.what()));
    }
}

py::array require_embedding_buffer(py::handle buf, int64_t rows,
                                   int row_bytes) {
    if (!py::isinstance<py::array>(buf)) {
        throw std::runtime_error("embedding_buffers must be NumPy arrays");
    }
    auto arr = py::reinterpret_borrow<py::array>(buf);
    if (!arr.dtype().is(py::dtype::of<uint8_t>()) ||
        !(arr.flags() & py::array::c_style)) {
        throw std::runtime_error("rows must be contiguous uint8 arrays");
    }
    if (arr.ndim() != 2 || arr.shape(0) != rows || arr.shape(1) != row_bytes) {
        throw std::runtime_error(
            "embedding buffer must match per-head table shape");
    }
    return arr;
}

}  // namespace

namespace mooncake {
namespace engram {

void bind_engram_store(py::module& m) {
#ifdef MOONCAKE_ENGRAM_CUDA
    py::class_<GpuLookup, std::shared_ptr<GpuLookup>>(m, "EngramLookup")
        .def("wait", &GpuLookup::wait, py::arg("stream"),
             "Enqueue completion and visibility on the consumer CUDA stream.")
        .def("check", &GpuLookup::check,
             "Check asynchronous errors after synchronizing the CUDA stream.");
#endif
    py::class_<EngramStoreConfig>(m, "EngramStoreConfig")
        .def(py::init<>())
        .def_readwrite("table_vocab_sizes",
                       &EngramStoreConfig::table_vocab_sizes)
        .def_readwrite("row_bytes", &EngramStoreConfig::row_bytes);

    py::class_<EngramStore>(m, "EngramStore")
        .def("get_layer_ids", &EngramStore::get_layer_ids)
        .def("get_table_vocab_sizes", &EngramStore::get_table_vocab_sizes)
        .def("get_store_keys", &EngramStore::get_store_keys)
        .def("get_num_heads", &EngramStore::get_num_heads)
        .def("get_row_bytes", &EngramStore::get_row_bytes)
        .def(
            "remove_from_store",
            [](EngramStore& self, int layer_id, bool force) {
                int ret = self.remove_from_store(layer_id, force);
                if (ret < 0) {
                    throw std::runtime_error("remove_from_store failed, rc=" +
                                             std::to_string(ret));
                }
                return ret;
            },
            py::arg("layer_id"), py::arg("force") = false,
            "Remove all Mooncake Store tables for the selected layer. "
            "Returns the number of removed head tables; missing keys are "
            "ignored.")
        .def(py::init([](const std::map<int, EngramStoreConfig>& layers,
                         py::object store_obj, const std::string& local_dir) {
                 std::shared_ptr<PyClient> store = unwrap_store(store_obj);
                 py::gil_scoped_release release;
                 return new EngramStore(layers, store, local_dir);
             }),
             py::arg("layers"), py::arg("store_client") = py::none(),
             py::arg("local_dir") = "")
#ifdef MOONCAKE_ENGRAM_CUDA
        .def(
            "lookup",
            [](EngramStore& self, const std::vector<int>& tables,
               py::object ids, py::object output, uintptr_t stream,
               std::vector<int64_t> offsets) {
                if (!ids.attr("is_cuda").cast<bool>() ||
                    !output.attr("is_cuda").cast<bool>() ||
                    !ids.attr("device").equal(output.attr("device")))
                    throw std::invalid_argument(
                        "lookup tensors must be on the same CUDA device");
                int device;
                if (cudaGetDevice(&device) != cudaSuccess ||
                    device != ids.attr("device").attr("index").cast<int>())
                    throw std::invalid_argument(
                        "lookup requires the current CUDA device");
                auto shape = ids.attr("shape").cast<std::vector<size_t>>();
                const auto dtype =
                    py::str(ids.attr("dtype")).cast<std::string>();
                if (shape.size() != 2 || shape[1] != tables.size() ||
                    tables.empty() ||
                    (dtype != "torch.int32" && dtype != "torch.int64"))
                    throw std::invalid_argument(
                        "CUDA IDs must be int32/int64 [tokens,heads]");
                const auto strides =
                    ids.attr("stride")().cast<std::vector<size_t>>();
                const auto width = self.get_row_bytes(tables.front());
                if (py::str(output.attr("dtype")).cast<std::string>() !=
                        "torch.uint8" ||
                    !output.attr("is_contiguous")().cast<bool>() ||
                    output.attr("shape").cast<std::vector<size_t>>() !=
                        std::vector<size_t>{tables.size(), shape[0],
                                            static_cast<size_t>(width)})
                    throw std::invalid_argument(
                        "output must be contiguous uint8 "
                        "[heads,tokens,row_bytes]");
                if (offsets.empty()) offsets.resize(tables.size(), 0);
                auto storage = output.attr("untyped_storage")();
                const auto base = storage.attr("data_ptr")().cast<uintptr_t>();
                const auto capacity = storage.attr("nbytes")().cast<size_t>();
                const auto address =
                    shape[0]
                        ? output.attr("data_ptr")().cast<uintptr_t>()
                        : base + output.attr("storage_offset")().cast<size_t>();
                if (address < base || address - base > capacity)
                    throw std::invalid_argument("Invalid output storage range");
                return self.lookup_cuda(
                    tables,
                    reinterpret_cast<void*>(
                        ids.attr("data_ptr")().cast<uintptr_t>()),
                    dtype == "torch.int64", shape[0], strides[0], strides[1],
                    offsets, reinterpret_cast<void*>(address),
                    capacity - (address - base), stream);
            },
            py::arg("table_ids"), py::arg("row_ids"), py::arg("output"),
            py::kw_only(), py::arg("stream"),
            py::arg("offsets") = std::vector<int64_t>{}, py::keep_alive<0, 1>(),
            "Enqueue GPU IDs on stream and return a lookup handle. The caller "
            "must keep tensors alive, register output, warm up before graph "
            "capture, and wait on the handle before reading/reusing output.")
#endif
        .def(
            "lookup",
            [](EngramStore& self, const std::vector<int>& table_ids,
               py::array ids, py::object output) {
                if (table_ids.empty() || ids.ndim() != 2 ||
                    ids.shape(0) !=
                        static_cast<py::ssize_t>(table_ids.size()) ||
                    !ids.dtype().is(py::dtype::of<int64_t>()) ||
                    !(ids.flags() & py::array::c_style))
                    throw std::invalid_argument(
                        "lookup requires contiguous int64 IDs "
                        "[heads,tokens]");
                const int width = self.get_row_bytes(table_ids.front());
                void* address;
                size_t bytes;
                std::vector<py::ssize_t> shape;
                if (py::isinstance<py::array>(output)) {
                    auto array = output.cast<py::array>();
                    if (!array.writeable() ||
                        !array.dtype().is(py::dtype::of<uint8_t>()) ||
                        !(array.flags() & py::array::c_style))
                        throw std::invalid_argument(
                            "output must be contiguous writable uint8");
                    auto info = array.request();
                    address = info.ptr;
                    bytes = array.nbytes();
                    shape = info.shape;
                } else {
                    // Tensor metadata only: no torch dependency or hidden copy.
                    if (py::str(output.attr("dtype")).cast<std::string>() !=
                            "torch.uint8" ||
                        !output.attr("is_contiguous")().cast<bool>())
                        throw std::invalid_argument(
                            "output must be a contiguous uint8 tensor");
                    address = reinterpret_cast<void*>(
                        output.attr("data_ptr")().cast<uintptr_t>());
                    bytes = output.attr("numel")().cast<size_t>();
                    shape =
                        output.attr("shape").cast<std::vector<py::ssize_t>>();
                }
                if (shape !=
                    std::vector<py::ssize_t>{ids.shape(0), ids.shape(1), width})
                    throw std::invalid_argument(
                        "output must have shape [heads,tokens,row_bytes]");
                int ret;
                {
                    py::gil_scoped_release release;
                    ret = self.lookup(table_ids,
                                      static_cast<const int64_t*>(ids.data()),
                                      ids.shape(1), address, bytes);
                }
                if (ret != 0)
                    throw std::runtime_error("EngramStore lookup failed");
            },
            py::arg("table_ids"), py::arg("row_ids"), py::arg("output"),
            "Read into one pre-registered contiguous uint8 tensor/array "
            "[heads,tokens,row_bytes]. "
            "IDs are contiguous int64 [heads,tokens]; -1 leaves that row "
            "untouched. "
            "Waits for all transfers; GPU callers must ensure GPUDirect "
            "visibility before consumption.")
        .def(
            "lookup_into",
            [](EngramStore& self, int layer_id,
               py::array_t<int64_t, py::array::c_style> ids, py::array output) {
                const int width = self.get_row_bytes(layer_id);
                if (ids.ndim() != 3 ||
                    ids.shape(2) != self.get_num_heads(layer_id) ||
                    ids.shape(0) > std::numeric_limits<int>::max() ||
                    ids.shape(1) > std::numeric_limits<int>::max() ||
                    output.ndim() != 4 || output.shape(0) != ids.shape(0) ||
                    output.shape(1) != ids.shape(1) ||
                    output.shape(2) != ids.shape(2) ||
                    output.shape(3) != width || !output.writeable() ||
                    !(output.flags() & py::array::c_style) ||
                    !output.dtype().is(py::dtype::of<uint8_t>())) {
                    throw std::runtime_error(
                        "lookup_into requires contiguous IDs [B,L,H] and "
                        "matching writable uint8 output [B,L,H,row_bytes]");
                }
                if (ids.size() == 0) return;
                auto ids_buf = ids.request();
                auto out_buf = output.request();
                const int B = static_cast<int>(ids_buf.shape[0]);
                const int L = static_cast<int>(ids_buf.shape[1]);
                int ret;
                {
                    py::gil_scoped_release release;
                    ret = self.lookup_into(
                        layer_id, static_cast<const int64_t*>(ids_buf.ptr), B,
                        L, out_buf.ptr, out_buf.size * out_buf.itemsize);
                }
                if (ret != 0)
                    throw std::runtime_error("EngramStore lookup_into failed");
            },
            py::arg("layer_id"), py::arg("row_ids").noconvert(),
            py::arg("output").noconvert(),
            "Read into caller-owned uint8 memory. The caller must keep the "
            "output registered with this Store for Store-backed reads. "
            "Local tables do not require output registration. "
            "This method does not allocate, register, or unregister output.")
        .def(
            "lookup_many_into",
            [](EngramStore& self, py::sequence layer_ids, py::sequence row_ids,
               py::sequence outputs) {
                const size_t count = py::len(layer_ids);
                if (py::len(row_ids) != count || py::len(outputs) != count) {
                    throw std::runtime_error(
                        "layer_ids, row_ids, and outputs must have equal "
                        "lengths");
                }

                std::vector<py::array> id_arrays;
                std::vector<py::array> output_arrays;
                std::vector<EngramStore::LookupRequest> requests;
                id_arrays.reserve(static_cast<size_t>(count));
                output_arrays.reserve(static_cast<size_t>(count));
                requests.reserve(static_cast<size_t>(count));
                for (size_t i = 0; i < count; ++i) {
                    const int layer_id = py::cast<int>(layer_ids[i]);
                    py::handle ids_object = row_ids[i];
                    py::handle output_object = outputs[i];
                    if (!py::isinstance<py::array>(ids_object) ||
                        !py::isinstance<py::array>(output_object)) {
                        throw std::runtime_error(
                            "row_ids and outputs must contain NumPy arrays");
                    }
                    auto ids = py::reinterpret_borrow<py::array>(ids_object);
                    auto output =
                        py::reinterpret_borrow<py::array>(output_object);
                    const int width = self.get_row_bytes(layer_id);
                    if (ids.ndim() != 3 ||
                        !ids.dtype().is(py::dtype::of<int64_t>()) ||
                        !(ids.flags() & py::array::c_style) ||
                        ids.shape(2) != self.get_num_heads(layer_id) ||
                        ids.shape(0) > std::numeric_limits<int>::max() ||
                        ids.shape(1) > std::numeric_limits<int>::max() ||
                        output.ndim() != 4 || output.shape(0) != ids.shape(0) ||
                        output.shape(1) != ids.shape(1) ||
                        output.shape(2) != ids.shape(2) ||
                        output.shape(3) != width || !output.writeable() ||
                        !(output.flags() & py::array::c_style) ||
                        !output.dtype().is(py::dtype::of<uint8_t>())) {
                        throw std::runtime_error(
                            "lookup_many_into requires contiguous IDs "
                            "[B,L,H] and matching writable uint8 outputs "
                            "[B,L,H,row_bytes]");
                    }

                    id_arrays.push_back(ids);
                    output_arrays.push_back(output);
                    if (ids.size() == 0) continue;
                    auto ids_buffer = ids.request();
                    auto output_buffer = output.request();
                    requests.push_back(EngramStore::LookupRequest{
                        .layer_id = layer_id,
                        .row_ids = static_cast<const int64_t*>(ids_buffer.ptr),
                        .batch_size = static_cast<int>(ids_buffer.shape[0]),
                        .sequence_length =
                            static_cast<int>(ids_buffer.shape[1]),
                        .output = output_buffer.ptr,
                        .output_size = static_cast<size_t>(output_buffer.size) *
                                       output_buffer.itemsize,
                    });
                }

                int ret;
                {
                    py::gil_scoped_release release;
                    ret = self.lookup_many_into(requests);
                }
                if (ret != 0)
                    throw std::runtime_error(
                        "EngramStore lookup_many_into failed");
            },
            py::arg("layer_ids"), py::arg("row_ids"), py::arg("outputs"),
            "Read several layers into registered outputs through one Store "
            "ranged-read submission.")
        .def(
            "lookup_many_into_registered",
            [](EngramStore& self, py::sequence layer_ids, py::sequence row_ids,
               py::sequence output_addresses, py::sequence output_sizes) {
                const size_t count = py::len(layer_ids);
                if (py::len(row_ids) != count ||
                    py::len(output_addresses) != count ||
                    py::len(output_sizes) != count) {
                    throw std::runtime_error(
                        "layer_ids, row_ids, output_addresses, and "
                        "output_sizes must have equal lengths");
                }

                std::vector<py::array> id_arrays;
                std::vector<EngramStore::LookupRequest> requests;
                id_arrays.reserve(count);
                requests.reserve(count);
                for (size_t i = 0; i < count; ++i) {
                    const int layer_id = py::cast<int>(layer_ids[i]);
                    py::handle ids_object = row_ids[i];
                    if (!py::isinstance<py::array>(ids_object)) {
                        throw std::runtime_error(
                            "row_ids must contain NumPy arrays");
                    }
                    auto ids = py::reinterpret_borrow<py::array>(ids_object);
                    if (ids.ndim() != 3 ||
                        !ids.dtype().is(py::dtype::of<int64_t>()) ||
                        !(ids.flags() & py::array::c_style) ||
                        ids.shape(2) != self.get_num_heads(layer_id) ||
                        ids.shape(0) > std::numeric_limits<int>::max() ||
                        ids.shape(1) > std::numeric_limits<int>::max()) {
                        throw std::runtime_error(
                            "lookup_many_into_registered requires contiguous "
                            "int64 IDs [B,L,H]");
                    }

                    id_arrays.push_back(ids);
                    if (ids.size() == 0) continue;
                    const auto output_address =
                        py::cast<uintptr_t>(output_addresses[i]);
                    const auto output_size = py::cast<size_t>(output_sizes[i]);
                    if (output_address == 0) {
                        throw std::runtime_error(
                            "registered output address must be nonzero");
                    }
                    auto ids_buffer = ids.request();
                    requests.push_back(EngramStore::LookupRequest{
                        .layer_id = layer_id,
                        .row_ids = static_cast<const int64_t*>(ids_buffer.ptr),
                        .batch_size = static_cast<int>(ids_buffer.shape[0]),
                        .sequence_length =
                            static_cast<int>(ids_buffer.shape[1]),
                        .output = reinterpret_cast<void*>(output_address),
                        .output_size = output_size,
                    });
                }

                int ret;
                {
                    py::gil_scoped_release release;
                    ret = self.lookup_many_into_registered(requests);
                }
                if (ret != 0)
                    throw std::runtime_error(
                        "EngramStore lookup_many_into_registered failed");
            },
            py::arg("layer_ids"), py::arg("row_ids"),
            py::arg("output_addresses"), py::arg("output_sizes"),
            "Read several layers directly into Store-registered memory. The "
            "caller owns pointer validation and lifetime; output contents are "
            "undefined on failure.")
        .def(
            "populate",
            [](EngramStore& self, int layer_id, py::list embedding_buffers,
               const ReplicateConfig& config) {
                const std::vector<int64_t> vocab_sizes =
                    self.get_table_vocab_sizes(layer_id);
                const int row_bytes = self.get_row_bytes(layer_id);
                if (static_cast<size_t>(py::len(embedding_buffers)) !=
                    vocab_sizes.size()) {
                    throw std::runtime_error(
                        "embedding_buffers size must match num_heads");
                }

                std::vector<py::array> arrays;
                std::vector<void*> bufs;
                std::vector<size_t> sizes;
                arrays.reserve(vocab_sizes.size());
                bufs.reserve(vocab_sizes.size());
                sizes.reserve(vocab_sizes.size());
                for (size_t i = 0; i < vocab_sizes.size(); ++i) {
                    auto arr = require_embedding_buffer(
                        embedding_buffers[i], vocab_sizes[i], row_bytes);
                    auto req = arr.request();
                    arrays.push_back(arr);
                    bufs.push_back(req.ptr);
                    sizes.push_back(arr.nbytes());
                }
                py::gil_scoped_release release;
                int ret = self.populate(layer_id, bufs, sizes, config);
                if (ret != 0) {
                    throw std::runtime_error("populate failed");
                }
            },
            py::arg("layer_id"), py::arg("embedding_buffers"),
            py::arg("config") = ReplicateConfig{},
            "Create a layer from per-head uint8 arrays. Copies into owned "
            "local "
            "tables or uploads to Store; inputs may be released after return. "
            "Local layers are published atomically and must not already "
            "exist.");
}

}  // namespace engram
}  // namespace mooncake
