#include "shim_convert.h"

#include <stdlib.h>
#include <string.h>

enum dtype
shim_convert_dtype(ZarrDataType dt)
{
    switch (dt) {
        case ZarrDataType_uint8:
            return dtype_u8;
        case ZarrDataType_uint16:
            return dtype_u16;
        case ZarrDataType_uint32:
            return dtype_u32;
        case ZarrDataType_uint64:
            return dtype_u64;
        case ZarrDataType_int8:
            return dtype_i8;
        case ZarrDataType_int16:
            return dtype_i16;
        case ZarrDataType_int32:
            return dtype_i32;
        case ZarrDataType_int64:
            return dtype_i64;
        case ZarrDataType_float32:
            return dtype_f32;
        case ZarrDataType_float64:
            return dtype_f64;
        default:
            return dtype_u8;
    }
}

enum compression_codec
shim_convert_codec(const ZarrCompressionSettings* settings)
{
    if (!settings || settings->compressor == ZarrCompressor_None) {
        return CODEC_NONE;
    }
    switch (settings->codec) {
        case ZarrCompressionCodec_BloscLZ4:
        case ZarrCompressionCodec_Lz4:
            return CODEC_LZ4;
        case ZarrCompressionCodec_BloscZstd:
        case ZarrCompressionCodec_Zstd:
            return CODEC_ZSTD;
        default:
            return CODEC_NONE;
    }
}

enum dimension_axis_type
shim_convert_axis_type(ZarrDimensionType type)
{
    switch (type) {
        case ZarrDimensionType_Space:
            return dimension_axis_space;
        case ZarrDimensionType_Channel:
            return dimension_axis_channel;
        case ZarrDimensionType_Time:
            return dimension_axis_time;
        case ZarrDimensionType_Other:
        default:
            return dimension_axis_other;
    }
}

enum lod_reduce_method
shim_convert_reduce_method(ZarrDownsamplingMethod method)
{
    switch (method) {
        case ZarrDownsamplingMethod_Mean:
            return lod_reduce_mean;
        case ZarrDownsamplingMethod_Min:
            return lod_reduce_min;
        case ZarrDownsamplingMethod_Max:
            return lod_reduce_max;
        case ZarrDownsamplingMethod_Decimate:
        default:
            return lod_reduce_mean;
    }
}

struct dimension*
shim_convert_dimensions(const ZarrDimensionProperties* props,
                        size_t count,
                        const size_t* storage_dimension_order,
                        bool multiscale)
{
    struct dimension* dims = calloc(count, sizeof(struct dimension));
    if (!dims) {
        return NULL;
    }

    for (size_t i = 0; i < count; ++i) {
        dims[i].size = props[i].array_size_px;
        dims[i].chunk_size = props[i].chunk_size_px;
        dims[i].chunks_per_shard = props[i].shard_size_chunks;
        dims[i].name = props[i].name;
        dims[i].downsample =
          multiscale && props[i].type == ZarrDimensionType_Space;
        dims[i].storage_position = (uint8_t)i;
        dims[i].ngff.type = shim_convert_axis_type(props[i].type);
        dims[i].ngff.unit = props[i].unit;
        dims[i].ngff.scale = props[i].scale;
    }

    if (storage_dimension_order) {
        for (size_t i = 0; i < count; ++i) {
            dims[storage_dimension_order[i]].storage_position = (uint8_t)i;
        }
    }

    return dims;
}
