#include "compressed-test-helper.hh"

namespace {
void
verify_zstd_metadata(const nlohmann::json& meta)
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
    EXPECT(internal_codecs[1]["name"].get<std::string>() == "zstd",
           "Expected second codec to be 'zstd', got ",
           internal_codecs[1]["name"].get<std::string>());

    const auto& zstd_config = internal_codecs[1]["configuration"];
    EXPECT_EQ(int, zstd_config["level"].get<int>(), 3);
    EXPECT(zstd_config["checksum"].get<bool>() == false,
           "Expected checksum to be false");
}
} // namespace

int
main()
{
    ZarrCompressionSettings compression_settings = {
        .compressor = ZarrCompressor_Zstd,
        .codec = ZarrCompressionCodec_Zstd,
        .level = 3,
        .shuffle = 0,
    };

    return compressed_test::run_test(compression_settings,
                                     verify_zstd_metadata);
}
