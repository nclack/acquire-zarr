// Header-only S3 test helpers using AWS CLI via popen().
// Replaces miniocpp for test validation against MinIO/S3.
#pragma once

#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

namespace s3 {

inline std::string
endpoint()
{
    const char* v = std::getenv("ZARR_S3_ENDPOINT");
    return v ? v : "";
}

inline std::string
bucket()
{
    const char* v = std::getenv("ZARR_S3_BUCKET_NAME");
    return v ? v : "";
}

// Run a command, return exit code.
inline int
run(const std::string& cmd)
{
    int rc = std::system(cmd.c_str());
#ifdef _WIN32
    return rc;
#else
    return WIFEXITED(rc) ? WEXITSTATUS(rc) : 1;
#endif
}

// Run a command and capture stdout into a string.
inline std::string
capture(const std::string& cmd)
{
    std::string out;
    FILE* f = popen(cmd.c_str(), "r");
    if (!f)
        return out;
    char buf[4096];
    while (size_t n = fread(buf, 1, sizeof(buf), f))
        out.append(buf, n);
    pclose(f);
    return out;
}

// Run a command and capture stdout into a byte vector.
inline std::vector<uint8_t>
capture_bytes(const std::string& cmd)
{
    std::vector<uint8_t> out;
    FILE* f = popen(cmd.c_str(), "r");
    if (!f)
        return out;
    uint8_t buf[4096];
    while (size_t n = fread(buf, 1, sizeof(buf), f))
        out.insert(out.end(), buf, buf + n);
    pclose(f);
    return out;
}

inline std::string
aws_prefix()
{
    return "aws --endpoint-url " + endpoint();
}

inline bool
object_exists(const std::string& key)
{
    std::string cmd = aws_prefix() + " s3api head-object --bucket " + bucket() +
                      " --key " + key + " > /dev/null 2>&1";
    return run(cmd) == 0;
}

inline size_t
get_object_size(const std::string& key)
{
    std::string cmd = aws_prefix() + " s3api head-object --bucket " + bucket() +
                      " --key " + key +
                      " --query ContentLength --output text 2>/dev/null";
    std::string out = capture(cmd);
    if (out.empty())
        return 0;
    return std::stoull(out);
}

inline std::string
get_object_contents(const std::string& key)
{
    std::string cmd =
      aws_prefix() + " s3 cp s3://" + bucket() + "/" + key + " - 2>/dev/null";
    return capture(cmd);
}

inline std::vector<uint8_t>
get_object_bytes(const std::string& key)
{
    std::string cmd =
      aws_prefix() + " s3 cp s3://" + bucket() + "/" + key + " - 2>/dev/null";
    return capture_bytes(cmd);
}

inline bool
remove_prefix(const std::string& prefix)
{
    std::string cmd = aws_prefix() + " s3 rm --recursive s3://" + bucket() +
                      "/" + prefix + " > /dev/null 2>&1";
    return run(cmd) == 0;
}

inline bool
remove_items(const std::vector<std::string>& keys)
{
    for (const auto& key : keys) {
        std::string cmd = aws_prefix() + " s3 rm s3://" + bucket() + "/" + key +
                          " > /dev/null 2>&1";
        if (run(cmd) != 0)
            return false;
    }
    return true;
}

} // namespace s3
