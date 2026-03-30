#pragma once

#include <cstdint>

namespace zarr {
struct ZstdCompressionParams
{
    uint8_t level{ 3 };
};
} // namespace zarr
