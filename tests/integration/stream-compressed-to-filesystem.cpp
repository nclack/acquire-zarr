#include "compressed-test-helper.hh"

namespace {
void
verify_blosc_metadata(const nlohmann::json& meta)
{
    using namespace compressed_test;

    const auto& shape = meta["shape"];
    EXPECT_EQ(size_t, shape.size(), 5);
    EXPECT_EQ(int, shape[0].get<int>(), array_timepoints);
    EXPECT_EQ(int, shape[1].get<int>(), array_channels);
    EXPECT_EQ(int, shape[2].get<int>(), array_planes);
    EXPECT_EQ(int, shape[3].get<int>(), array_height);
    EXPECT_EQ(int, shape[4].get<int>(), array_width);

    const auto& chunks = meta["chunk_grid"]["configuration"]["chunk_shape"];
    EXPECT_EQ(size_t, chunks.size(), 5);
    EXPECT_EQ(int, chunks[0].get<int>(), chunk_timepoints * shard_timepoints);
    EXPECT_EQ(int, chunks[1].get<int>(), chunk_channels * shard_channels);
    EXPECT_EQ(int, chunks[2].get<int>(), chunk_planes * shard_planes);
    EXPECT_EQ(int, chunks[3].get<int>(), chunk_height * shard_height);
    EXPECT_EQ(int, chunks[4].get<int>(), chunk_width * shard_width);

    const auto dtype = meta["data_type"].get<std::string>();
    EXPECT(dtype == "uint16",
           "Expected dtype to be 'uint16', but got '",
           dtype,
           "'");

    const auto& codecs = meta["codecs"];
    EXPECT_EQ(size_t, codecs.size(), 1);
    const auto& sharding_codec = codecs[0]["configuration"];

    const auto& shards = sharding_codec["chunk_shape"];
    EXPECT_EQ(size_t, shards.size(), 5);
    EXPECT_EQ(int, shards[0].get<int>(), chunk_timepoints);
    EXPECT_EQ(int, shards[1].get<int>(), chunk_channels);
    EXPECT_EQ(int, shards[2].get<int>(), chunk_planes);
    EXPECT_EQ(int, shards[3].get<int>(), chunk_height);
    EXPECT_EQ(int, shards[4].get<int>(), chunk_width);

    const auto& internal_codecs = sharding_codec["codecs"];
    EXPECT(internal_codecs.size() == 2,
           "Expected 2 internal codecs, got ",
           internal_codecs.size());

    EXPECT(internal_codecs[0]["name"].get<std::string>() == "bytes",
           "Expected first codec to be 'bytes', got ",
           internal_codecs[0]["name"].get<std::string>());
    EXPECT(internal_codecs[1]["name"].get<std::string>() == "blosc",
           "Expected second codec to be 'blosc', got ",
           internal_codecs[1]["name"].get<std::string>());

    const auto& blosc_config = internal_codecs[1]["configuration"];
    EXPECT_EQ(int, blosc_config["blocksize"].get<int>(), 0);
    EXPECT_EQ(int, blosc_config["clevel"].get<int>(), 2);
    EXPECT(blosc_config["cname"].get<std::string>() == "lz4",
           "Expected codec name to be 'lz4', got ",
           blosc_config["cname"].get<std::string>());
    EXPECT(blosc_config["shuffle"].get<std::string>() == "bitshuffle",
           "Expected shuffle to be 'bitshuffle', got ",
           blosc_config["shuffle"].get<std::string>());
    EXPECT_EQ(int, blosc_config["typesize"].get<int>(), 2);

    const auto& dimension_names = meta["dimension_names"];
    EXPECT_EQ(size_t, dimension_names.size(), 5);
    EXPECT(dimension_names[0].get<std::string>() == "t",
           "Expected 't', got ",
           dimension_names[0].get<std::string>());
    EXPECT(dimension_names[1].get<std::string>() == "c",
           "Expected 'c', got ",
           dimension_names[1].get<std::string>());
    EXPECT(dimension_names[2].get<std::string>() == "z",
           "Expected 'z', got ",
           dimension_names[2].get<std::string>());
    EXPECT(dimension_names[3].get<std::string>() == "y",
           "Expected 'y', got ",
           dimension_names[3].get<std::string>());
    EXPECT(dimension_names[4].get<std::string>() == "x",
           "Expected 'x', got ",
           dimension_names[4].get<std::string>());
}
} // namespace

int
main()
{
    ZarrCompressionSettings compression_settings = {
        .compressor = ZarrCompressor_Blosc1,
        .codec = ZarrCompressionCodec_BloscLZ4,
        .level = 2,
        .shuffle = 2,
    };

    return compressed_test::run_test(compression_settings,
                                     verify_blosc_metadata);
}
