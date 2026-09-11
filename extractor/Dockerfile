FROM archlinux:latest

ARG TARGETARCH
ARG ZIG_VERSION=0.16.0

RUN pacman -Syu --noconfirm && \
    pacman -S --noconfirm librdkafka gcc curl xz

RUN case "${TARGETARCH}" in \
      amd64) zig_arch=x86_64 ;; \
      arm64) zig_arch=aarch64 ;; \
      *) echo "unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac && \
    curl --fail --location --silent --show-error \
      "https://ziglang.org/download/${ZIG_VERSION}/zig-${zig_arch}-linux-${ZIG_VERSION}.tar.xz" \
      --output /tmp/zig.tar.xz && \
    tar -C /opt -xf /tmp/zig.tar.xz && \
    ln -s "/opt/zig-${zig_arch}-linux-${ZIG_VERSION}/zig" /usr/local/bin/zig && \
    rm /tmp/zig.tar.xz

WORKDIR /app

COPY . .

RUN zig build -Doptimize=ReleaseFast

CMD ["./zig-out/bin/extractor"]
