#pragma once

#include <cstdint>

namespace zarr {
struct Lz4CompressionParams
{
    uint8_t level{ 1 };
};
} // namespace zarr
