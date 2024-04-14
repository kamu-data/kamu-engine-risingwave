TARGET_ARCH = x86_64-unknown-linux-gnu
PLATFORM = linux/amd64
RW_VERSION = $(shell cargo metadata --format-version 1 | jq -r '.packages[] | select(.name == "risingwave_cmd_all").version')
ADAPTER_VERSION = $(shell cargo metadata --format-version 1 | jq -r '.packages[] | select(.name == "kamu-engine-risingwave").version')
IMAGE_TAG = $(ADAPTER_VERSION)-risingwave_${RW_VERSION}
IMAGE = ghcr.io/kamu-data/engine-risingwave:$(IMAGE_TAG)


###############################################################################
# Test
###############################################################################

.PHONY: build
build:
	cd .. && ./risedev build
	cd .. && cargo test -p kamu-engine-risingwave --no-run


.PHONY: test
test:
	cd .. && \
		RUST_BACKTRACE=1 \
		RUST_LOG_SPAN_EVENTS=new,close \
		RUST_LOG=debug \
		RW_ODF_SOURCE_DEBUG=1 \
		RW_ODF_SINK_DEBUG=1 \
		cargo test \
		-p kamu-engine-risingwave -- \
		--test-threads=1 \
		--nocapture


###############################################################################
# Release
###############################################################################

.PHONY: build-release
build-release:
	# Build engine
	cd .. && \
		RUSTFLAGS="--cfg tokio_unstable" \
		cross build \
		-p risingwave_cmd_all -p risedev \
		--release \
		--target $(TARGET_ARCH)

	# Build adapter
	cd .. && \
		RUSTFLAGS="--cfg tokio_unstable" \
		cross build \
		-p kamu-engine-risingwave \
		--release \
		--target $(TARGET_ARCH)


# Build image for local testing purposes
.PHONY: image
image:
	rm -rf .tmp && mkdir -p .tmp/$(PLATFORM)
	cp ../target/$(TARGET_ARCH)/release/kamu-engine-risingwave .tmp/$(PLATFORM)/kamu-engine-risingwave
	cp ../target/$(TARGET_ARCH)/release/risingwave .tmp/$(PLATFORM)/risingwave

	cd .. && docker build \
	    --build-arg target_arch=$(TARGET_ARCH) \
		-t $(IMAGE) \
		-f odf/Dockerfile \
		--load \
		.

# Build and push multi-arch image
.PHONY: image-multi-arch
image-multi-arch:
	rm -rf .tmp && mkdir -p .tmp/linux/amd64 && mkdir .tmp/linux/arm64
	cp ../target/x86_64-unknown-linux-gnu/release/kamu-engine-risingwave .tmp/linux/amd64/kamu-engine-risingwave
	cp ../target/x86_64-unknown-linux-gnu/release/risingwave .tmp/linux/amd64/risingwave
	cp ../target/aarch64-unknown-linux-gnu/release/kamu-engine-risingwave .tmp/linux/arm64/kamu-engine-risingwave
	cp ../target/aarch64-unknown-linux-gnu/release/risingwave .tmp/linux/arm64/risingwave

	cd .. && docker buildx build \
		--push \
	    --platform linux/amd64,linux/arm64 \
		-t $(IMAGE) \
		-f odf/Dockerfile \
		.

.PHONY: clean
clean:
	rm -rf .tmp
