FROM ubuntu:24.04

RUN apt-get -o Acquire::Retries=3 -o Acquire::http::Timeout=30 -o APT::Update::Error-Mode=any update \
    && DEBIAN_FRONTEND=noninteractive apt-get -o Acquire::Retries=3 -o Acquire::http::Timeout=30 install -y --no-install-recommends \
        build-essential ca-certificates clang cmake curl file git libclang-dev pkg-config procps python3 time unzip xz-utils zstd \
        libasound2-dev libegl1-mesa-dev libfontconfig-dev libfreetype6-dev libgbm-dev libgl1-mesa-dev \
        libpipewire-0.3-dev libssl-dev libudev-dev libwayland-dev libwebkit2gtk-4.1-dev \
        libx11-dev libxcb-shape0-dev libxcb-xfixes0-dev libxkbcommon-dev libxkbcommon-x11-dev \
    && rm -rf /var/lib/apt/lists/*

# Match the hosted runner's UID for its mounted workspace and run tests unprivileged.
RUN useradd --uid 1001 --create-home --shell /bin/bash runner
USER runner
