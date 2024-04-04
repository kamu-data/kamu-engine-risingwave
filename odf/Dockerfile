# TODO: Consider using https://github.com/GoogleContainerTools/distroless
FROM docker.io/library/ubuntu:24.04
ARG TARGETPLATFORM

# Tini
RUN apt update && \
    apt -y install tini && \
    apt-get clean && rm -rf /var/lib/apt/lists /var/cache/apt/archives

# Engine
COPY odf/.tmp/$TARGETPLATFORM/risingwave /opt/engine/bin/risingwave
COPY odf/.tmp/$TARGETPLATFORM/kamu-engine-risingwave /opt/engine/bin/kamu-engine-risingwave
COPY odf/config.toml /opt/engine/config.toml

ENV RUST_BACKTRACE=1
ENV RUST_LOG=info
EXPOSE 2884/tcp
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/engine/bin/kamu-engine-risingwave"]
