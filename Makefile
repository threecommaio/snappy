OUT := snappy
VERSION ?= $(shell git describe --tags || echo 0.1)
PKG=github.com/threecommaio/snappy
BUILDTIME:=$(shell date +"%Y.%m.%d.%H%M%S")
COMMIT_HASH:=$(shell git log --pretty=format:'%h' -n 1)

FLAGS:=-X ${PKG}/pkg.Version=${VERSION}
FLAGS:=${FLAGS} -X ${PKG}/pkg.BuildTime=${BUILDTIME}
FLAGS:=${FLAGS} -X ${PKG}/pkg.CommitHash=${COMMIT_HASH}

all: build

build:
	@go build -i -o ${OUT} -ldflags="${FLAGS}"

release:
	@mkdir -p bin
	@gox -osarch="linux/amd64 darwin/amd64 linux/386" -ldflags="${FLAGS}" -output="bin/{{.Dir}}_{{.OS}}_{{.Arch}}"

install: build
	@go install

docker:
	@docker build -t threecomma/snappy:${VERSION} .

clean:
	-@rm ${OUT} ${OUT}-v*

.PHONY: build install clean