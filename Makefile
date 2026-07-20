.PHONY: build debug install test upload lint proto install_grpc_tools clean

PROTOC_VERSION= 21.8
PROTOC_GEN_GO_VERSION= "v1.28"
PROTOC_GEN_GO_GRPC_VERSION= "v1.2"

# go env
#GOPROXY     := "https://goproxy.cn,direct"
GOPROXY     := "https://proxy.golang.org,direct"
GOOS        := $(if $(GOOS),$(GOOS),$(shell go env GOOS))
GOARCH      := $(if $(GOARCH),$(GOARCH),$(shell go env GOARCH))
CGO_LDFLAGS := "-static"
CGO_CFLAGS  := "-D_LARGEFILE64_SOURCE"
CC          := musl-gcc

GOENV := GO111MODULE=on
GOENV += GOPROXY=$(GOPROXY)
GOENV += CC=$(CC)
GOENV += CGO_ENABLED=1 CGO_LDFLAGS=$(CGO_LDFLAGS) CGO_CFLAGS=$(CGO_CFLAGS)
GOENV += GOOS=$(GOOS) GOARCH=$(GOARCH)
GOLANGCILINT_VERSION ?= v1.50.0
GOBIN := $(shell go env GOPATH)/bin
GOBIN_GOLANGCILINT := $(shell which $(GOBIN)/golangci-lint)
# go
GO := go

# output
OUTPUT := bin/dingo

# build flags
LDFLAGS := -s -w
LDFLAGS += -extldflags "-static -fpic"
LDFLAGS += -X github.com/dingodb/dingocli/cli/cli.Version=3.1
LDFLAGS += -X github.com/dingodb/dingocli/cli/cli.CommitId=$(shell git rev-parse --short HEAD)
LDFLAGS += -X github.com/dingodb/dingocli/cli/cli.BuildTime=$(shell date +%Y-%m-%dT%H:%M:%S)
LDFLAGS += -X github.com/dingodb/dingocli/cli/cli.Branch=$(shell git rev-parse --abbrev-ref HEAD)

BUILD_FLAGS := -a
BUILD_FLAGS += -trimpath
BUILD_FLAGS += -ldflags '$(LDFLAGS)'
BUILD_FLAGS += $(EXTRA_FLAGS)

# debug flags
GCFLAGS := "all=-N -l"

DEBUG_FLAGS := -gcflags=$(GCFLAGS)

# go test
GO_TEST ?= $(GO) test

# test flags
CASE ?= "."

TEST_FLAGS := -v
TEST_FLAGS += -p 3
TEST_FLAGS += -cover
TEST_FLAGS += -count=1
TEST_FLAGS += $(DEBUG_FLAGS)
TEST_FLAGS += -run $(CASE)

# packages
PACKAGES := $(PWD)/cmd/dingo/main.go

# tools
GOPATH := $(shell go env GOPATH)
PROTOC_GEN_GO := $(GOPATH)/bin/protoc-gen-go
PROTOC_GEN_GO_GRPC := $(GOPATH)/bin/protoc-gen-go-grpc


# tar
VERSION := "unknown"

build: proto
	$(GOENV) $(GO) build -o $(OUTPUT) $(BUILD_FLAGS) $(PACKAGES)

debug:
	$(GOENV) $(GO) build -o $(OUTPUT) $(DEBUG_FLAGS) $(PACKAGES)

install:
	cp bin/dingo ~/.dingo/bin

test:
	$(GO_TEST) $(TEST_FLAGS) ./...

upload:
	@NOSCMD=$(NOSCMD) bash build/package/upload.sh $(VERSION)

lint:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCILINT_VERSION)
	$(GOBIN_GOLANGCILINT) run -v

proto: install_grpc_tools
	@bash mk-proto.sh

install_grpc_tools: $(PROTOC_GEN_GO) $(PROTOC_GEN_GO_GRPC)
$(PROTOC_GEN_GO):
	go install google.golang.org/protobuf/cmd/protoc-gen-go@${PROTOC_GEN_GO_VERSION}
$(PROTOC_GEN_GO_GRPC):
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@${PROTOC_GEN_GO_GRPC_VERSION}

clean:
	rm -rf bin
	rm -rf proto/*
