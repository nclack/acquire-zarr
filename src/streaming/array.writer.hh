#pragma once

#include "zarr.dimension.hh"
#include "thread.pool.hh"
#include "s3.connection.hh"
#include "blosc.compression.params.hh"
#include "file.sink.hh"
#include "definitions.hh"

#include <condition_variable>
#include <filesystem>

namespace fs = std::filesystem;

namespace zarr {
struct ArrayWriterConfig
{
    std::shared_ptr<ArrayDimensions> dimensions;
    ZarrDataType dtype;
    int level_of_detail;
    std::optional<std::string> bucket_name;
    std::string store_path;
    std::optional<BloscCompressionParams> compression_params;
};

/**
 * @brief Downsample the array writer configuration to a lower resolution.
 * @param[in] config The original array writer configuration.
 * @param[out] downsampled_config The downsampled array writer configuration.
 * @return True if @p downsampled_config can be downsampled further.
 * This is determined by the chunk size in @p config. This function will return
 * false if and only if downsampling brings one or more dimensions lower than
 * the chunk size along that dimension.
 */
[[nodiscard]]
bool
downsample(const ArrayWriterConfig& config,
           ArrayWriterConfig& downsampled_config);

class ArrayWriter
{
  public:
    ArrayWriter(const ArrayWriterConfig& config,
                std::shared_ptr<ThreadPool> thread_pool);

    ArrayWriter(const ArrayWriterConfig& config,
                std::shared_ptr<ThreadPool> thread_pool,
                std::shared_ptr<S3ConnectionPool> s3_connection_pool);

    virtual ~ArrayWriter() = default;

    /**
     * @brief Write a frame to the array.
     * @param data The frame data.
     * @return The number of bytes written.
     */
    [[nodiscard]] size_t write_frame(std::span<const std::byte> data);

  protected:
    ArrayWriterConfig config_;

    /// Buffering
    std::vector<ByteVector> data_buffers_;

    /// Filesystem
    std::vector<std::string> data_paths_;
    std::unique_ptr<Sink> metadata_sink_;

    /// Multithreading
    std::shared_ptr<ThreadPool> thread_pool_;
    std::mutex buffers_mutex_;

    /// Bookkeeping
    uint64_t bytes_to_flush_;
    uint32_t frames_written_;
    uint32_t append_chunk_index_;
    bool is_finalizing_;

    std::shared_ptr<S3ConnectionPool> s3_connection_pool_;

    /**
     * @brief Compute the number of bytes to allocate for a single chunk.
     * @note Allocate the usual chunk size, plus the maximum Blosc overhead if
     * we're compressing.
     * @return The number of bytes to allocate per chunk.
     */
    size_t bytes_to_allocate_per_chunk_() const;

    bool is_s3_array_() const;
    virtual std::string data_root_() const = 0;
    virtual std::string metadata_path_() const = 0;
    virtual const DimensionPartsFun parts_along_dimension_() const = 0;

    void make_data_paths_();
    [[nodiscard]] bool make_metadata_sink_();
    virtual void make_buffers_() = 0;

    virtual BytePtr get_chunk_data_(uint32_t index) = 0;

    bool should_flush_() const;
    virtual bool should_rollover_() const = 0;

    size_t write_frame_to_chunks_(std::span<const std::byte> data);

    [[nodiscard]] virtual bool compress_and_flush_data_() = 0;
    void rollover_();

    [[nodiscard]] virtual bool write_array_metadata_() = 0;

    virtual void close_sinks_() = 0;

    friend bool finalize_array(std::unique_ptr<ArrayWriter>&& writer);
};

bool
finalize_array(std::unique_ptr<ArrayWriter>&& writer);
} // namespace zarr
