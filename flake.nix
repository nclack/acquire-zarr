{
  description = "Development environment for acquire-zarr";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    claude-code.url = "github:sadjow/claude-code-nix";
    claude-code.inputs.nixpkgs.follows = "nixpkgs";
    claude-code.inputs.flake-utils.follows = "flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils, claude-code }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        devShells.default = pkgs.mkShell.override { stdenv = pkgs.clangStdenv; } {
          name = "acquire-zarr";

          buildInputs = with pkgs; [
            tmux
            # Build tools
            cmake
            ninja
            pkg-config

            # Development tools
            awscli2
            lldb
            clang-tools
            claude-code.packages.${system}.default
            cmake-language-server
            cmake-format
            just
            gh
            man-pages
            man-pages-posix
            uv

            # Libraries
            lz4
            zstd
            c-blosc
            nlohmann_json
            crc32c
            openssl
            curlpp
            inih
            pugixml
            zlib
            llvmPackages.openmp
            # s3 writer
            aws-c-common
            aws-c-cal
            aws-c-io
            aws-c-http
            aws-c-auth
            aws-c-s3
            aws-c-compression
            aws-c-sdkutils
            aws-checksums
            s2n-tls

            # Python support
            python311
            python311Packages.pybind11
          ];

          CMAKE_PREFIX_PATH = with pkgs; "${c-blosc}:${nlohmann_json}:${crc32c}:${openssl}:${curlpp}:${inih}:${pugixml}:${zlib}";
          blosc_DIR = "${pkgs.c-blosc}/lib/cmake/blosc";

        };
      }
    );
}
