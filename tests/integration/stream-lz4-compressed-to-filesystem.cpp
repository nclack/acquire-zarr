#include "compressed-test-helper.hh"

namespace {
void
verify_lz4_metadata(const nlohmann::json& meta)
{
    const auto& codecs = meta["codecs"];
    EXPECT_EQ(size_t, codecs.size(), 1);
    const auto& sharding_codec = codecs[0]["configuration"];

    const auto& internal_codecs = sharding_codec["codecs"];
    EXPECT(internal_codecs.size() == 2,
           "Expected 2 internal codecs, got ",
           internal_codecs.size());

    EXPECT(internal_codecs[0]["name"].get<std::string>() == "bytes",
           "Expected first codec to be 'bytes', got ",
           internal_codecs[0]["name"].get<std::string>());
    EXPECT(internal_codecs[1]["name"].get<std::string>() == "lz4",
           "Expected second codec to be 'lz4', got ",
           internal_codecs[1]["name"].get<std::string>());

    const auto& lz4_config = internal_codecs[1]["configuration"];
    EXPECT_EQ(int, lz4_config["level"].get<int>(), 1);
}
} // namespace

int
main()
{
    ZarrCompressionSettings compression_settings = {
        .compressor = ZarrCompressor_Lz4,
        .codec = ZarrCompressionCodec_Lz4,
        .level = 1,
        .shuffle = 0,
    };

    return compressed_test::run_test(compression_settings,
                                     verify_lz4_metadata);
}
