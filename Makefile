SHELL := /usr/bin/env bash

build-docker-materialize:
	docker build -f infra/DockerfileMaterialize -t eo-flow-materialize-eager . --build-arg CLOUD=gcp

run-docker-materialize:
	docker run -it --rm \
		--env "DATA=$data" \
		--env GOOGLE_APPLICATION_CREDENTIALS=/tmp/keys/gcp.json \
		--env "CLOUD=gcp" \
		--mount "type=bind,src=$(GOOGLE_APPLICATION_CREDENTIALS),dst=/tmp/keys/gcp.json,readonly" \
		--name materialize-eager \
		eo-flow-materialize-eager
