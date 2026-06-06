FROM archlinux:latest

RUN pacman -Syu --noconfirm && \
    pacman -S --noconfirm zig librdkafka gcc

WORKDIR /app

COPY . .

RUN zig build -Doptimize=ReleaseFast

CMD ["./zig-out/bin/extractor"]
