# TODO: Consider using https://github.com/GoogleContainerTools/distroless
FROM docker.io/library/ubuntu:24.04
ARG target_arch

# Tini & deps
RUN apt update && \
    apt -y install tini openssl && \
    apt-get clean && rm -rf /var/lib/apt/lists /var/cache/apt/archives

# Engine
COPY target/ci-release/risingwave /opt/engine/bin/risingwave
COPY target/ci-release/kamu-engine-risingwave /opt/engine/bin/kamu-engine-risingwave
COPY odf/config.toml /opt/engine/config.toml

ENV RUST_BACKTRACE=1
ENV RUST_LOG=info
EXPOSE 2884/tcp
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/engine/bin/kamu-engine-risingwave"]
