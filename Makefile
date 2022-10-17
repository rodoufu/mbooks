build-image:
	docker build -f Dockerfile -t mbooks:latest .

clean:
	cargo clean
	docker container rm mbooks || true
	docker image rm mbooks:latest || true

run:
	docker run --rm --name "mbooks" mbooks:latest

test:
	cargo +$(cat rust-toolchain) test
