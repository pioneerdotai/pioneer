FROM ubuntu:24.04

ADD --checksum=sha256:8b2a587ffd672c4687e7581dad4b2f6c1bb2ad6b480cd9771ba2ff48e0b8c75d https://apt.llvm.org/llvm-snapshot.gpg.key /etc/apt/keyrings/llvm.asc

RUN apt-get -o Acquire::Retries=3 -o Acquire::http::Timeout=30 -o APT::Update::Error-Mode=any update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends ca-certificates \
    && chmod 0644 /etc/apt/keyrings/llvm.asc \
    && echo 'deb [signed-by=/etc/apt/keyrings/llvm.asc] https://apt.llvm.org/noble/ llvm-toolchain-noble-21 main' > /etc/apt/sources.list.d/llvm.list \
    && apt-get -o Acquire::Retries=3 -o Acquire::https::Timeout=30 -o APT::Update::Error-Mode=any update \
    && DEBIAN_FRONTEND=noninteractive apt-get -o Acquire::Retries=3 -o Acquire::http::Timeout=30 install -y --no-install-recommends \
        build-essential clang-21 cmake curl file git libclang-21-dev pkg-config procps python3 time unzip xz-utils zstd \
        libasound2-dev libegl1-mesa-dev libfontconfig-dev libfreetype6-dev libgbm-dev libgl1-mesa-dev \
        libpipewire-0.3-dev libssl-dev libudev-dev libwayland-dev libwebkit2gtk-4.1-dev \
        libx11-dev libxcb-shape0-dev libxcb-xfixes0-dev libxkbcommon-dev libxkbcommon-x11-dev \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/usr/lib/llvm-21/bin:${PATH}" \
    LIBCLANG_PATH="/usr/lib/llvm-21/lib"

# Exercise the default C++ driver used by webrtc-sys and the native linker.
RUN printf '#if __clang_major__ != 21\n#error Clang 21 required\n#endif\nint main() {}\n' \
    | clang++ -x c++ - -o /tmp/clang-check && /tmp/clang-check && rm /tmp/clang-check

# Match the hosted runner's UID for its mounted workspace and run tests unprivileged.
RUN useradd --uid 1001 --create-home --shell /bin/bash runner
USER runner
