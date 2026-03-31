# syntax=docker/dockerfile:1

# CPU-only shim build & test — no CUDA, no vcpkg.
FROM ubuntu:24.04 AS deps

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        ninja-build \
        libzstd-dev \
        liblz4-dev \
        libomp-dev \
        libssl-dev \
        nlohmann-json3-dev \
        git \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Build AWS C libraries from source (not packaged in Ubuntu 24.04).
RUN git clone --depth 1 --branch v0.12.6 https://github.com/awslabs/aws-c-common.git      /tmp/b && cmake -S /tmp/b -B /tmp/b/build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt/aws -DCMAKE_PREFIX_PATH=/opt/aws -DBUILD_TESTING=OFF && cmake --build /tmp/b/build --target install && rm -rf /tmp/b \
 && git clone --depth 1 --branch v0.9.13  https://github.com/awslabs/aws-c-cal.git          /tmp/b && cmake -S /tmp/b -B /tmp/b/build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt/aws -DCMAKE_PREFIX_PATH=/opt/aws -DBUILD_TESTING=OFF && cmake --build /tmp/b/build --target install && rm -rf /tmp/b \
 && git clone --depth 1 --branch v0.2.10  https://github.com/awslabs/aws-checksums.git      /tmp/b && cmake -S /tmp/b -B /tmp/b/build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt/aws -DCMAKE_PREFIX_PATH=/opt/aws -DBUILD_TESTING=OFF && cmake --build /tmp/b/build --target install && rm -rf /tmp/b \
 && git clone --depth 1 --branch v1.7.0   https://github.com/aws/s2n-tls.git               /tmp/b && cmake -S /tmp/b -B /tmp/b/build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt/aws -DCMAKE_PREFIX_PATH=/opt/aws -DBUILD_TESTING=OFF && cmake --build /tmp/b/build --target install && rm -rf /tmp/b \
 && git clone --depth 1 --branch v0.26.3  https://github.com/awslabs/aws-c-io.git           /tmp/b && cmake -S /tmp/b -B /tmp/b/build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt/aws -DCMAKE_PREFIX_PATH=/opt/aws -DBUILD_TESTING=OFF && cmake --build /tmp/b/build --target install && rm -rf /tmp/b \
 && git clone --depth 1 --branch v0.3.2   https://github.com/awslabs/aws-c-compression.git  /tmp/b && cmake -S /tmp/b -B /tmp/b/build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt/aws -DCMAKE_PREFIX_PATH=/opt/aws -DBUILD_TESTING=OFF && cmake --build /tmp/b/build --target install && rm -rf /tmp/b \
 && git clone --depth 1 --branch v0.10.12 https://github.com/awslabs/aws-c-http.git         /tmp/b && cmake -S /tmp/b -B /tmp/b/build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt/aws -DCMAKE_PREFIX_PATH=/opt/aws -DBUILD_TESTING=OFF && cmake --build /tmp/b/build --target install && rm -rf /tmp/b \
 && git clone --depth 1 --branch v0.2.4   https://github.com/awslabs/aws-c-sdkutils.git     /tmp/b && cmake -S /tmp/b -B /tmp/b/build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt/aws -DCMAKE_PREFIX_PATH=/opt/aws -DBUILD_TESTING=OFF && cmake --build /tmp/b/build --target install && rm -rf /tmp/b \
 && git clone --depth 1 --branch v0.10.1  https://github.com/awslabs/aws-c-auth.git         /tmp/b && cmake -S /tmp/b -B /tmp/b/build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt/aws -DCMAKE_PREFIX_PATH=/opt/aws -DBUILD_TESTING=OFF && cmake --build /tmp/b/build --target install && rm -rf /tmp/b \
 && git clone --depth 1 --branch v0.11.5  https://github.com/awslabs/aws-c-s3.git           /tmp/b && cmake -S /tmp/b -B /tmp/b/build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt/aws -DCMAKE_PREFIX_PATH=/opt/aws -DBUILD_TESTING=OFF && cmake --build /tmp/b/build --target install && rm -rf /tmp/b

ENV CMAKE_PREFIX_PATH="/opt/aws"

FROM deps AS build
WORKDIR /src
COPY . .

ARG CMAKE_BUILD_TYPE=Release
RUN cmake -S shim -B shim/build -G Ninja \
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
        -DCHUCKY_ENABLE_GPU=OFF \
    && cmake --build shim/build

FROM build AS test
WORKDIR /src/shim/build
RUN ctest --output-on-failure
