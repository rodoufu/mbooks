build-image:
	docker build -f Dockerfile -t mbooks:latest .

clean:
	cargo clean
	docker container rm mbooks || true
	docker image rm mbooks:latest || true
	docker-compose down --rmi all -v --remove-orphans

run:
	docker run --rm --name "mbooks" mbooks:latest

test:
	cargo +$(cat rust-toolchain) test

run-compose:
	docker-compose up
