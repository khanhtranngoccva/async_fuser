TARGET=$(shell rustc -vV | sed -n 's|host: ||p')
UNSTABLE_FLAGS=--cfg tokio_unstable
TSAN_FLAGS=-Zsanitizer=thread $(UNSTABLE_FLAGS)
ASAN_FLAGS=-Zsanitizer=address,leak $(UNSTABLE_FLAGS)
RSAN_FLAGS=-Zsanitizer=realtime $(UNSTABLE_FLAGS)

test:
	cargo test --release

tsan:
	RUSTFLAGS="$(TSAN_FLAGS)" RUSTDOCFLAGS="$(TSAN_FLAGS)" cargo +nightly test -Z build-std --target $(TARGET) -- $(TEST_FILTER)

asan:
	RUSTFLAGS="$(ASAN_FLAGS)" RUSTDOCFLAGS="$(ASAN_FLAGS)" cargo +nightly test -Z build-std --target $(TARGET) -- $(TEST_FILTER)

rsan:
	RUSTFLAGS="$(RSAN_FLAGS)" RUSTDOCFLAGS="$(RSAN_FLAGS)" cargo +nightly test -Z build-std --target $(TARGET) -- $(TEST_FILTER)

sanitize:
	$(MAKE) tsan
	$(MAKE) asan
	$(MAKE) rsan

clean:
	cargo clean